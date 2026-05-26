"""
feature_engine/features.py
---------------------------
Cálculo de features cuantitativas sobre DataFrames OHLCV limpios.

Entrada esperada
────────────────
df_ohlcv   → salida de aggregator.compute_ohlcv()
             columnas: symbol, window_start, window_end, window_label,
                       open, high, low, close, volume, trade_count,
                       buy_volume, sell_volume, price_qty_sum

df_tickers → salida de aggregator.compute_spread_timeseries()
             columnas: symbol, window_label, window_start,
                       spread_mean, spread_min, spread_max, spread_std,
                       mid_price_mean, tick_count
             Adicionalmente incluye: obi_mean (order book imbalance)

Cada función es pura: recibe DataFrame(s), devuelve DataFrame.
Sin efectos secundarios — componibles y testeables de forma aislada.

Features implementadas
──────────────────────
P0 (core):
  1. vwap                → precio promedio ponderado por volumen
  2. log_return          → retorno logarítmico entre ventanas consecutivas
  3. rolling_volatility  → volatilidad histórica (stddev de log_returns)
  4. momentum / pct      → cambio de precio en N ventanas
  5. buy_sell_ratio      → presión compradora vs vendedora
  6. spread_mean_pct     → spread como % del mid_price (normalizado)

P1 (microestructura):
  7. obi                 → Order Book Imbalance (bid_qty vs ask_qty)
  8. return_autocorr     → autocorrelación del log_return (reversión a media)
  9. realized_volatility → volatilidad realizada anualizada (método Parkinson)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# 1. VWAP  —  Volume-Weighted Average Price
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  VWAP = Σ(price_i × qty_i) / Σ(qty_i)
#
# El VWAP es el precio de referencia institucional: los grandes participantes
# usan el VWAP del día como benchmark para evaluar la calidad de ejecución.
# Una orden ejecutada por debajo del VWAP (para compras) se considera buena
# ejecución.
#
# price_qty_sum y volume ya vienen pre-calculados del aggregator,
# así que el cálculo es una división directa — sin re-leer los trades raw.
#
# Invariante: VWAP siempre está entre min(price) y max(price) de la ventana.
# ───────────────────────────────────────────────────────────────────────────

def add_vwap(df: DataFrame) -> DataFrame:
    """
    Agrega columna `vwap` al DataFrame OHLCV.
    Requiere columnas: price_qty_sum, volume.
    """
    return df.withColumn(
        "vwap",
        F.round(
            F.col("price_qty_sum") / F.col("volume"),
            8,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. log_return  —  Retorno logarítmico entre ventanas consecutivas
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  log_return_t = ln(close_t / close_{t-1})
#
# El retorno logarítmico es preferido al aritmético en finanzas porque:
#   - Es simétrico: +10% y -10% suman 0 en log (el aritmético no)
#   - Es aditivo en el tiempo: retorno semanal = suma de retornos diarios
#   - Permite aplicar estadística gaussiana directamente
#   - Es la base para calcular volatilidad (stddev de log_returns)
#
# La primera ventana de cada símbolo tendrá log_return = null — correcto,
# no hay ventana anterior con qué comparar.
# ───────────────────────────────────────────────────────────────────────────

def add_log_return(df: DataFrame) -> DataFrame:
    """
    Agrega columna `log_return` usando close price entre ventanas consecutivas.
    Requiere columnas: symbol, window_start, close.
    """
    w = Window.partitionBy("symbol").orderBy("window_start")
    return df.withColumn(
        "log_return",
        F.round(F.log(F.col("close") / F.lag("close", 1).over(w)), 10)
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3. rolling_volatility  —  Volatilidad histórica rolling
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  vol_t = stddev(log_return_{t-N+1}, ..., log_return_t)
#
# La volatilidad histórica mide la dispersión de los retornos en una
# ventana deslizante de N períodos. Es el estimador más simple y más
# usado de volatilidad realizada.
#
# N = 10 períodos por defecto:
#   OHLCV 1m → ventana de 10 minutos
#   OHLCV 5m → ventana de 50 minutos
#   OHLCV 1h → ventana de 10 horas
#
# Nota: para annualizarla, multiplicar por sqrt(períodos por año).
# ───────────────────────────────────────────────────────────────────────────

def add_rolling_volatility(df: DataFrame, periods: int = 10) -> DataFrame:
    """
    Agrega columna `rolling_volatility` como stddev de log_return en ventana
    deslizante de `periods` períodos.

    Args:
        df:      DataFrame con columnas symbol, window_start, log_return
        periods: Número de períodos en la ventana deslizante (default: 10)
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-periods + 1, 0)
    )
    return df.withColumn(
        "rolling_volatility",
        F.round(F.stddev("log_return").over(w), 10)
    )


# ═══════════════════════════════════════════════════════════════════════════
# 4. momentum  —  Cambio de precio relativo en ventana N
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  momentum_t     = close_t - close_{t-N}
#           momentum_pct_t = (close_t - close_{t-N}) / close_{t-N}
#
# El momentum captura la tendencia de corto plazo. Un momentum positivo
# indica que el precio está más alto que N ventanas atrás — señal de
# tendencia alcista. Esto es la base del "momentum investing".
#
# N = 5 períodos: para OHLCV 1m, compara con 5 minutos antes.
# ───────────────────────────────────────────────────────────────────────────

def add_momentum(df: DataFrame, periods: int = 5) -> DataFrame:
    """
    Agrega columnas `momentum` y `momentum_pct`.

    Args:
        df:      DataFrame con columnas symbol, window_start, close
        periods: Lookback en número de ventanas (default: 5)
    """
    w = Window.partitionBy("symbol").orderBy("window_start")
    close_n_ago = F.lag("close", periods).over(w)
    return (
        df
        .withColumn("momentum",     F.round(F.col("close") - close_n_ago, 8))
        .withColumn("momentum_pct", F.round((F.col("close") - close_n_ago) / close_n_ago, 8))
    )


# ═══════════════════════════════════════════════════════════════════════════
# 5. spread_mean_pct  —  Spread normalizado como % del mid_price
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  spread_pct = spread_mean / mid_price_mean × 100
#
# El spread absoluto no es comparable entre activos: el spread de BTC
# puede ser $5 y el de BNB $0.01, pero ambos pueden representar
# el mismo costo relativo de transacción.
#
# Normalizando por mid_price se obtiene un costo de liquidez comparable.
# ───────────────────────────────────────────────────────────────────────────

def add_spread_pct(df_spread: DataFrame) -> DataFrame:
    """
    Agrega columna `spread_mean_pct`: spread como porcentaje del mid_price.
    Permite comparar liquidez entre activos de distinto valor absoluto.

    Requiere columnas: spread_mean, mid_price_mean.
    """
    return df_spread.withColumn(
        "spread_mean_pct",
        F.round(F.col("spread_mean") / F.col("mid_price_mean") * 100, 6)
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6. buy_sell_ratio  —  Presión compradora vs vendedora
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  BSR = buy_volume / sell_volume
#
# Un BSR > 1 indica dominancia compradora (presión alcista).
# Un BSR < 1 indica dominancia vendedora (presión bajista).
# BSR = 1 indica equilibrio perfecto entre compradores y vendedores.
#
# Nota sobre is_buyer_maker:
#   is_buyer_maker=True  → el comprador colocó la orden límite (maker)
#                          → el agressor fue el vendedor → trade = sell
#   is_buyer_maker=False → el comprador agredió el libro → trade = buy
# ───────────────────────────────────────────────────────────────────────────

def add_buy_sell_ratio(df: DataFrame) -> DataFrame:
    """
    Agrega columna `buy_sell_ratio`.
    Requiere columnas: buy_volume, sell_volume.
    """
    return df.withColumn(
        "buy_sell_ratio",
        F.round(F.col("buy_volume") / F.col("sell_volume"), 6)
    )


# ═══════════════════════════════════════════════════════════════════════════
# 7. Order Book Imbalance (OBI)  —  Presión del libro de órdenes
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  OBI = (bid_qty_mean - ask_qty_mean) / (bid_qty_mean + ask_qty_mean)
#
# El OBI mide el desbalance entre la cantidad disponible en el lado
# comprador (bids) vs el lado vendedor (asks) del order book.
#
# Interpretación:
#   OBI > 0  → más liquidez en bids → presión compradora → señal alcista
#   OBI < 0  → más liquidez en asks → presión vendedora  → señal bajista
#   OBI = 0  → libro de órdenes perfectamente balanceado
#
# Rango: [-1, +1]. Un OBI cercano a ±1 indica un libro muy desbalanceado,
# lo que generalmente precede a un movimiento brusco de precio.
#
# Diferencia con buy_sell_ratio:
#   BSR mide transacciones ejecutadas (el pasado).
#   OBI mide intenciones pendientes en el libro (el futuro inmediato).
# Por eso el OBI tiene mayor capacidad predictiva a muy corto plazo.
#
# Esta feature se calcula desde df_spread (book tickers agregados),
# no desde df_ohlcv, porque requiere best_bid_qty y best_ask_qty.
# ───────────────────────────────────────────────────────────────────────────

def add_obi(df_spread: DataFrame) -> DataFrame:
    """
    Agrega columna `obi` (Order Book Imbalance) al DataFrame de spread.

    Requiere columnas: bid_qty_mean, ask_qty_mean.
    Estas columnas las agrega aggregator.compute_spread_timeseries().

    Returns:
        DataFrame con columna adicional `obi` en rango [-1, +1].
    """
    bid = F.col("bid_qty_mean")
    ask = F.col("ask_qty_mean")
    total = bid + ask

    return df_spread.withColumn(
        "obi",
        F.round(
            F.when(total > 0, (bid - ask) / total).otherwise(F.lit(None)),
            6,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 8. Autocorrelación del log_return  —  Detección de reversión a la media
# ═══════════════════════════════════════════════════════════════════════════
#
# La autocorrelación de lag-1 mide qué tan correlacionado está el retorno
# de la ventana actual con el retorno de la ventana anterior.
#
# Interpretación:
#   autocorr > 0  → momentum: retornos positivos tienden a seguir positivos
#   autocorr < 0  → reversión a la media: retornos positivos tienden a
#                   revertir (señal contraria)
#   autocorr ≈ 0  → retornos no predecibles (hipótesis de mercado eficiente)
#
# En mercados de cripto a alta frecuencia (1m) se observa típicamente
# autocorrelación negativa — consistente con reversión a la media causada
# por market makers que rebalancean continuamente sus posiciones.
#
# Implementación: correlación de Pearson entre log_return y su lag-1,
# calculada sobre una ventana deslizante de N períodos.
# La ventana debe ser suficientemente grande (≥ 20) para ser estadísticamente
# significativa, pero no tan grande que pierda relevancia temporal.
# ───────────────────────────────────────────────────────────────────────────

def add_return_autocorr(df: DataFrame, periods: int = 20) -> DataFrame:
    """
    Agrega columna `return_autocorr`: autocorrelación de lag-1 del log_return
    calculada sobre una ventana deslizante de `periods` períodos.

    Args:
        df:      DataFrame con columnas symbol, window_start, log_return
        periods: Ventana deslizante para calcular la correlación (default: 20)

    Returns:
        DataFrame con columna `return_autocorr` en rango [-1, +1].

    Nota: requiere al menos `periods` filas por símbolo para producir
    valores no nulos. Las primeras `periods` filas serán null.
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-periods + 1, 0)
    )

    # Crear lag-1 del log_return para la correlación
    lag_return = F.lag("log_return", 1).over(
        Window.partitionBy("symbol").orderBy("window_start")
    )

    return (
        df
        .withColumn("_lag_return", lag_return)
        .withColumn(
            "return_autocorr",
            F.round(F.corr("log_return", "_lag_return").over(w), 6)
        )
        .drop("_lag_return")
    )


# ═══════════════════════════════════════════════════════════════════════════
# 9. Volatilidad realizada  —  Estimador de Parkinson (high-low range)
# ═══════════════════════════════════════════════════════════════════════════
#
# El estimador de Parkinson usa el rango high-low de la vela en lugar
# de solo el retorno close-to-close, capturando mejor la volatilidad
# intradía — especialmente relevante en mercados 24/7 como cripto.
#
# Fórmula de Parkinson (1980):
#   vol_parkinson = sqrt( (1 / (4 × ln(2))) × mean(ln(high/low)²) )
#
# Ventajas sobre la volatilidad histórica rolling:
#   - Usa toda la información de la vela (no solo el cierre)
#   - Es 5x más eficiente estadísticamente que el estimador close-to-close
#   - Captura gaps intradía que la volatilidad rolling ignora
#   - Siempre ≥ 0 (no puede ser negativa como stddev de retornos nulos)
#
# Constante: 1/(4×ln2) ≈ 0.3607
# ───────────────────────────────────────────────────────────────────────────

_PARKINSON_CONST = 1.0 / (4.0 * 0.6931471805599453)  # 1/(4×ln2) ≈ 0.3607


def add_realized_volatility(df: DataFrame, periods: int = 10) -> DataFrame:
    """
    Agrega columna `realized_volatility` usando el estimador de Parkinson
    (high-low range) sobre una ventana deslizante de `periods` períodos.

    Es más eficiente estadísticamente que rolling_volatility (close-to-close)
    porque usa toda la información de la vela, no solo el precio de cierre.

    Args:
        df:      DataFrame con columnas symbol, window_start, high, low
        periods: Ventana deslizante (default: 10)

    Returns:
        DataFrame con columna `realized_volatility`.
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-periods + 1, 0)
    )

    # ln(high/low)² — el cuadrado del log-range de la vela
    log_hl_sq = F.pow(F.log(F.col("high") / F.col("low")), 2)

    return df.withColumn(
        "realized_volatility",
        F.round(
            F.sqrt(F.lit(_PARKINSON_CONST) * F.avg(log_hl_sq).over(w)),
            10,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline completo
# ═══════════════════════════════════════════════════════════════════════════

def compute_all_features(df_ohlcv: DataFrame) -> DataFrame:
    """
    Aplica todas las features OHLCV-based sobre el DataFrame.

    Orden de aplicación (el orden importa — algunas dependen de otras):
        1. vwap                → requiere price_qty_sum, volume
        2. log_return          → requiere close, window_start
        3. rolling_volatility  → requiere log_return (paso 2)
        4. realized_volatility → requiere high, low (independiente de 2-3)
        5. momentum            → requiere close, window_start
        6. buy_sell_ratio      → requiere buy_volume, sell_volume
        7. return_autocorr     → requiere log_return (paso 2)

    Returns:
        DataFrame con todas las columnas OHLCV originales más:
        vwap, log_return, rolling_volatility, realized_volatility,
        momentum, momentum_pct, buy_sell_ratio, return_autocorr
    """
    return (
        df_ohlcv
        .transform(add_vwap)
        .transform(add_log_return)
        .transform(add_rolling_volatility)
        .transform(add_realized_volatility)
        .transform(add_momentum)
        .transform(add_buy_sell_ratio)
        .transform(add_return_autocorr)
    )


def final_feature_set(df_features: DataFrame, df_spread: DataFrame) -> DataFrame:
    """
    Une las features OHLCV-based con las features de microestructura
    (spread + OBI) en un único DataFrame por símbolo y ventana.

    El OBI se calcula aquí desde df_spread si las columnas bid_qty_mean
    y ask_qty_mean están disponibles (requiere aggregator actualizado).

    Join key: (symbol, window_label, window_start)
    """
    if df_spread is None:
        return df_features

    df_spread_enriched = add_spread_pct(df_spread)

    # Agregar OBI si las columnas necesarias están disponibles
    if "bid_qty_mean" in df_spread_enriched.columns and "ask_qty_mean" in df_spread_enriched.columns:
        df_spread_enriched = add_obi(df_spread_enriched)
        spread_cols = [
            "symbol", "window_label", "window_start",
            "spread_mean", "spread_mean_pct",
            "spread_min", "spread_max", "spread_std",
            "mid_price_mean", "tick_count", "obi",
        ]
    else:
        spread_cols = [
            "symbol", "window_label", "window_start",
            "spread_mean", "spread_mean_pct",
            "spread_min", "spread_max", "spread_std",
            "mid_price_mean", "tick_count",
        ]

    # Solo seleccionar columnas que existen
    available = [c for c in spread_cols if c in df_spread_enriched.columns]

    return df_features.join(
        F.broadcast(df_spread_enriched.select(available)),
        on=["symbol", "window_label", "window_start"],
        how="left",
    )