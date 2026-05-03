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

Cada función es pura: recibe DataFrame(s), devuelve DataFrame.
Sin efectos secundarios — componibles y testeables de forma aislada.
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
# Interpreta el precio promedio ponderado por volumen transaccionado.
# Es más representativo que el precio simple (close) porque da mayor
# peso a los trades grandes.
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
            8,   # 8 decimales — precisión estándar en cripto
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. log_return  —  Retorno logarítmico entre ventanas consecutivas
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  log_return_t = ln(close_t / close_{t-1})
#                        = ln(close_t) - ln(close_{t-1})
#
# El retorno logarítmico es preferido al retorno aritmético en finanzas porque:
#   - Es simétrico: +10% y -10% suman 0 (el aritmético no)
#   - Es aditivo en el tiempo: retorno semanal = suma de retornos diarios
#   - Aproxima bien los retornos pequeños
#   - Es la base para calcular volatilidad (stddev de log_returns)
#
# Implementación: lag(close, 1) sobre Window particionada por símbolo
# y ordenada por window_start. La primera ventana de cada símbolo tendrá
# log_return = null (no hay ventana anterior) — comportamiento correcto.
# ───────────────────────────────────────────────────────────────────────────

def add_log_return(df: DataFrame) -> DataFrame:
    """
    Agrega columna `log_return` usando close price entre ventanas consecutivas.
    Requiere columnas: symbol, window_start, close.
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
    )

    prev_close = F.lag("close", 1).over(w)

    return df.withColumn(
        "log_return",
        F.round(
            F.log(F.col("close") / prev_close),
            10,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3. rolling_volatility  —  Volatilidad histórica rolling
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  vol_t = stddev(log_return_{t-N+1}, ..., log_return_t)
#
# La volatilidad histórica mide la dispersión de los retornos en una
# ventana deslizante de N períodos. Es la desviación estándar de los
# log_returns — el estimador más simple y más usado de volatilidad realizada.
#
# N = 10 períodos por defecto (parametrizable):
#   - Para OHLCV de 1m → ventana de 10 minutos
#   - Para OHLCV de 5m → ventana de 50 minutos
#
# rowsBetween(-N+1, 0): incluye las N filas anteriores incluyendo la actual.
# Requiere que log_return ya esté calculado (llamar add_log_return primero).
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
# Fórmula:  momentum_t = close_t - close_{t-N}
#           momentum_%_t = (close_t - close_{t-N}) / close_{t-N}
#
# El momentum mide si el precio está subiendo o bajando con respecto
# a N períodos atrás. Un momentum positivo indica tendencia alcista.
#
# Se calcula en dos versiones:
#   - Absoluta (momentum):    diferencia de precio en la misma unidad (USD)
#   - Relativa (momentum_pct): diferencia como fracción del precio base,
#                               comparable entre activos de distinto valor
#
# N = 5 períodos por defecto:
#   Para OHLCV 1m → compara el cierre actual con el de 5 minutos antes.
# ───────────────────────────────────────────────────────────────────────────

def add_momentum(df: DataFrame, periods: int = 5) -> DataFrame:
    """
    Agrega columnas `momentum` y `momentum_pct`.

    Args:
        df:      DataFrame con columnas symbol, window_start, close
        periods: Lookback en número de ventanas (default: 5)
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
    )

    close_n_ago = F.lag("close", periods).over(w)

    return (
        df
        .withColumn(
            "momentum",
            F.round(F.col("close") - close_n_ago, 8)
        )
        .withColumn(
            "momentum_pct",
            F.round((F.col("close") - close_n_ago) / close_n_ago, 8)
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 5. spread_mean  —  Spread bid-ask promedio por ventana
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  spread_t = best_ask_t - best_bid_t
#           spread_mean = avg(spread_t) para t en ventana
#
# El spread bid-ask es el costo implícito de ejecutar una operación de
# compra-venta inmediata. Un spread bajo indica alta liquidez (BTC).
# Un spread alto indica menor liquidez o mayor incertidumbre (BNB).
#
# Viene directamente del aggregator.compute_spread_timeseries(),
# aquí lo normalizamos opcionalmente como porcentaje del mid_price
# para comparación cross-asset.
# ───────────────────────────────────────────────────────────────────────────

def add_spread_pct(df_spread: DataFrame) -> DataFrame:
    """
    Agrega columna `spread_mean_pct`: spread como porcentaje del mid_price.
    Permite comparar liquidez entre activos de distinto valor absoluto.

    Requiere columnas: spread_mean, mid_price_mean.
    """
    return df_spread.withColumn(
        "spread_mean_pct",
        F.round(
            F.col("spread_mean") / F.col("mid_price_mean") * 100,
            6,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 6. buy_sell_ratio  —  Presión compradora vs vendedora
# ═══════════════════════════════════════════════════════════════════════════
#
# Fórmula:  buy_sell_ratio = buy_volume / sell_volume
#
# Un ratio > 1 indica dominancia compradora (presión alcista).
# Un ratio < 1 indica dominancia vendedora (presión bajista).
# Ratio = 1 indica equilibrio.
#
# Nota: buy_volume y sell_volume se calculan desde is_buyer_maker
#   is_buyer_maker=True  → orden de compra es la pasiva (maker)
#                          → el agressor es el vendedor → sell_volume
#   is_buyer_maker=False → el agressor es el comprador → buy_volume
# ───────────────────────────────────────────────────────────────────────────

def add_buy_sell_ratio(df: DataFrame) -> DataFrame:
    """
    Agrega columna `buy_sell_ratio`.
    Requiere columnas: buy_volume, sell_volume.
    """
    return df.withColumn(
        "buy_sell_ratio",
        F.round(
            F.col("buy_volume") / F.col("sell_volume"),
            6,
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline completo: combina OHLCV + features en un único DataFrame
# ═══════════════════════════════════════════════════════════════════════════

def compute_all_features(df_ohlcv: DataFrame) -> DataFrame:
    """
    Aplica todas las features sobre un DataFrame OHLCV.

    Orden de aplicación:
        1. vwap             → requiere price_qty_sum, volume
        2. log_return       → requiere close, window_start
        3. rolling_volatility → requiere log_return (paso 2)
        4. momentum         → requiere close, window_start
        5. buy_sell_ratio   → requiere buy_volume, sell_volume

    Returns:
        DataFrame con todas las columnas OHLCV originales más:
        vwap, log_return, rolling_volatility,
        momentum, momentum_pct, buy_sell_ratio
    """
    return (
        df_ohlcv
        .transform(add_vwap)
        .transform(add_log_return)
        .transform(add_rolling_volatility)
        .transform(add_momentum)
        .transform(add_buy_sell_ratio)
    )


def final_feature_set(df_features: DataFrame, df_spread: DataFrame) -> DataFrame:
    """
    Une las features de trades (OHLCV-based) con las features de spread
    (ticker-based) en un único DataFrame por símbolo y ventana.

    Join key: (symbol, window_start, window_label)
    """
    df_spread_with_pct = add_spread_pct(df_spread)

    spread_cols = [
        "symbol", "window_label", "window_start",
        "spread_mean", "spread_mean_pct",
        "spread_min", "spread_max", "spread_std",
        "mid_price_mean", "tick_count",
    ]

    return df_features.join(
        F.broadcast(df_spread_with_pct.select(spread_cols)),
        on=["symbol", "window_label", "window_start"],
        how="left",
    )