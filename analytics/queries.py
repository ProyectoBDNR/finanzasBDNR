"""
analytics/queries.py
---------------------
Queries analíticas sobre el dataset de features de CryptoFlow.

Cada función responde una pregunta de negocio concreta.
Todas reciben DataFrames (salida de feature_engine) y retornan DataFrames.

Tecnología elegida por query:
  ┌──────────────────────────────────────────────────┬──────────────────┐
  │ Query                                            │ Mejor con        │
  ├──────────────────────────────────────────────────┼──────────────────┤
  │ Q1 Régimen de volatilidad por activo             │ Spark (Window)   │
  │ Q2 Spread vs liquidez — correlación              │ Spark (agg)      │
  │ Q3 Divergencia de momentum cross-asset           │ Spark (pivot)    │
  │ Q4 Detección de picos de volumen anómalos        │ Spark (zscore)   │
  │ Q5 Latencia end-to-end por símbolo               │ Cassandra + Spark│
  │ Q6 Presión compradora acumulada (buy/sell ratio) │ Spark (Window)   │
  │ Q7 VWAP vs close — tracking error               │ Spark (Window)   │
  └──────────────────────────────────────────────────┴──────────────────┘
"""

from __future__ import annotations


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# Q1 · Régimen de volatilidad por activo
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿En qué régimen de volatilidad opera cada activo
#           y cuándo cambia de régimen?
#
# Por qué no es trivial:
#   No solo calcula volatilidad promedio — la clasifica en tres regímenes
#   (baja / media / alta) y detecta transiciones entre ellos.
#   Una transición LOW→HIGH es señal de evento de mercado relevante.
#
# Método:
#   1. Percentiles de rolling_volatility por símbolo (p33, p66)
#   2. Clasifica cada ventana en LOW / MED / HIGH
#   3. Detecta cambio de régimen vs ventana anterior (lag)
#   4. Reporta duración media en cada régimen
# ───────────────────────────────────────────────────────────────────────────

def q1_volatility_regime(df: DataFrame) -> DataFrame:
    """
    Clasifica ventanas en regímenes de volatilidad y detecta transiciones.

    Returns:
        DataFrame con columnas:
          symbol, window_start, rolling_volatility,
          vol_regime, prev_regime, regime_change, regime_duration_windows
    """
    # Percentiles por símbolo para definir umbrales relativos
    # (más robusto que umbrales absolutos que varían por activo)
    pct_df = df.groupBy("symbol").agg(
        F.percentile_approx("rolling_volatility", 0.33).alias("p33"),
        F.percentile_approx("rolling_volatility", 0.66).alias("p66"),
    )

    df_with_pct = df.join(F.broadcast(pct_df), on="symbol", how="left")

    # Clasificación por régimen
    df_regime = df_with_pct.withColumn(
        "vol_regime",
        F.when(F.col("rolling_volatility").isNull(), F.lit("UNKNOWN"))
         .when(F.col("rolling_volatility") <= F.col("p33"),  F.lit("LOW"))
         .when(F.col("rolling_volatility") <= F.col("p66"),  F.lit("MED"))
         .otherwise(F.lit("HIGH"))
    )

    # Detección de cambio de régimen
    w = Window.partitionBy("symbol").orderBy("window_start")
    df_transitions = df_regime.withColumn(
        "prev_regime",
        F.lag("vol_regime", 1).over(w)
    ).withColumn(
        "regime_change",
        (F.col("vol_regime") != F.col("prev_regime")).cast("boolean")
    )

    # Duración acumulada en el régimen actual (ventanas consecutivas sin cambio)
    # Técnica: group ID incremental que sube en cada cambio de régimen
    df_with_group = df_transitions.withColumn(
        "regime_group",
        F.sum(F.col("regime_change").cast("int")).over(
            w.rowsBetween(Window.unboundedPreceding, 0)
        )
    )

    w_group = Window.partitionBy("symbol", "regime_group").orderBy("window_start")
    result = df_with_group.withColumn(
        "regime_duration_windows",
        F.row_number().over(w_group)
    ).select(
        "symbol", "window_start", "rolling_volatility",
        "vol_regime", "prev_regime", "regime_change",
        "regime_duration_windows",
    )

    return result.orderBy("symbol", "window_start")


# ═══════════════════════════════════════════════════════════════════════════
# Q2 · Spread vs volumen — ¿cuándo el spread aumenta con el volumen?
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿La liquidez (spread) se degrada cuando el volumen aumenta?
#           ¿O los activos más líquidos mantienen spread bajo incluso
#           bajo alta actividad?
#
# Por qué no es trivial:
#   Detecta si hay una relación positiva entre volumen y spread
#   (degradación de liquidez bajo estrés) o si son independientes.
#   Relevante para validar la calidad de los datos y la microestructura.
#
# Método:
#   1. Une features de trades con spread timeseries
#   2. Divide volumen en cuartiles por símbolo
#   3. Calcula spread_mean promedio por cuartil de volumen
#   4. Reporta si hay monotonía (spread sube con volumen)
# ───────────────────────────────────────────────────────────────────────────

def q2_spread_liquidity_profile(df: DataFrame) -> DataFrame:
    """
    Perfil de liquidez: spread promedio por cuartil de volumen, por símbolo.

    Interpreta:
      Si spread_q4 > spread_q1 → el mercado se estrecha en alta actividad.
      Si spread_q4 ≈ spread_q1 → liquidez profunda (típico en BTC).

    Returns:
        DataFrame con columnas:
          symbol, volume_quartile, avg_spread, avg_volume,
          window_count, spread_vol_ratio
    """
    # Requiere spread_mean en el mismo DataFrame (resultado de final_feature_set)
    df_filtered = df.filter(
        F.col("spread_mean").isNotNull() &
        F.col("volume").isNotNull()
    )

    # Cuartil de volumen por símbolo
    w = Window.partitionBy("symbol").orderBy("volume")
    df_with_quartile = df_filtered.withColumn(
        "volume_quartile",
        F.ntile(4).over(w)
    )

    result = df_with_quartile.groupBy("symbol", "volume_quartile").agg(
        F.round(F.avg("spread_mean"),      6).alias("avg_spread"),
        F.round(F.avg("volume"),           4).alias("avg_volume"),
        F.count("*").alias("window_count"),
        # Spread como fracción del volumen — cuanto más bajo, mejor la liquidez
        F.round(F.avg("spread_mean") / F.avg("volume"), 8).alias("spread_vol_ratio"),
    ).orderBy("symbol", "volume_quartile")

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Q3 · Divergencia de momentum cross-asset
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿En qué ventanas temporales BTC, ETH y BNB divergen
#           en su dirección de momentum? ¿Hay activos que lideran
#           al mercado y otros que siguen?
#
# Por qué no es trivial:
#   La divergencia de momentum entre activos correlacionados es una señal
#   analítica de desacoplamiento. Si BTC sube y ETH baja en la misma
#   ventana, hay información sobre flujo de capital entre activos.
#
# Método:
#   1. Pivota momentum_pct por símbolo (una columna por activo)
#   2. Calcula dirección de cada activo (+1 / -1)
#   3. Suma de direcciones: 3=todos alcistas, -3=todos bajistas,
#      ±1 o ±2 = divergencia
#   4. Filtra las ventanas con divergencia máxima
# ───────────────────────────────────────────────────────────────────────────

def q3_momentum_divergence(df: DataFrame) -> DataFrame:
    """
    Detecta ventanas donde los activos divergen en dirección de momentum.

    Returns:
        DataFrame con columnas:
          window_start,
          momentum_BTCUSDT, momentum_ETHUSDT, momentum_BNBUSDT,
          dir_BTC, dir_ETH, dir_BNB,
          consensus_score,   (-3 a +3)
          divergence         (True si no hay consenso total)
    """
    # Dirección de momentum: +1 alcista, -1 bajista, 0 neutro
    df_dir = df.filter(F.col("momentum_pct").isNotNull()).withColumn(
        "direction",
        F.when(F.col("momentum_pct") > 0,  F.lit(1))
         .when(F.col("momentum_pct") < 0,  F.lit(-1))
         .otherwise(F.lit(0))
    )

    # Pivot: una fila por window_start, columnas por símbolo
    pivoted = df_dir.groupBy("window_start").pivot(
        "symbol", ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    ).agg(
        F.first("momentum_pct").alias("momentum"),
    ).withColumnRenamed("BTCUSDT", "mom_BTC") \
     .withColumnRenamed("ETHUSDT", "mom_ETH") \
     .withColumnRenamed("BNBUSDT", "mom_BNB")

    dir_pivoted = df_dir.groupBy("window_start").pivot(
        "symbol", ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    ).agg(
        F.first("direction"),
    ).withColumnRenamed("BTCUSDT", "dir_BTC") \
     .withColumnRenamed("ETHUSDT", "dir_ETH") \
     .withColumnRenamed("BNBUSDT", "dir_BNB")

    joined = pivoted.join(dir_pivoted, on="window_start", how="inner")

    result = joined.withColumn(
        "consensus_score",
        F.col("dir_BTC") + F.col("dir_ETH") + F.col("dir_BNB")
    ).withColumn(
        "divergence",
        (F.abs(F.col("consensus_score")) < 3).cast("boolean")
    ).orderBy("window_start")

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Q4 · Detección de picos de volumen anómalos (Z-score)
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿En qué ventanas el volumen es estadísticamente anómalo?
#           ¿Coinciden esos picos con movimientos de precio?
#
# Por qué no es trivial:
#   Usa Z-score rolling para detectar anomalías de volumen sin umbrales
#   fijos. Un umbral fijo (ej. volumen > X) no funciona cross-asset porque
#   BTC y BNB tienen escalas de volumen completamente distintas.
#
# Método:
#   1. Z-score del volumen = (vol - mean(vol)) / stddev(vol) rolling 20 ventanas
#   2. Marca como anómalo si |z| > 2 (equivale a ~95% CI)
#   3. Calcula el retorno en esa ventana para correlacionar anomalía-precio
# ───────────────────────────────────────────────────────────────────────────

def q4_volume_anomalies(df: DataFrame, z_threshold: float = 2.0) -> DataFrame:
    """
    Detecta ventanas con volumen estadísticamente anómalo via Z-score rolling.

    Args:
        df:            DataFrame de features con columnas volume, log_return
        z_threshold:   Umbral de Z-score para clasificar como anomalía (default: 2.0)

    Returns:
        DataFrame con columnas:
          symbol, window_start, volume, volume_zscore,
          is_anomaly, log_return, price_move_during_anomaly
    """
    ROLLING_N = 20   # ventana para calcular media y stddev de volumen

    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-ROLLING_N + 1, 0)
    )

    df_zscore = df.withColumn(
        "vol_rolling_mean", F.avg("volume").over(w)
    ).withColumn(
        "vol_rolling_std",  F.stddev("volume").over(w)
    ).withColumn(
        "volume_zscore",
        F.round(
            (F.col("volume") - F.col("vol_rolling_mean")) / F.col("vol_rolling_std"),
            4
        )
    ).withColumn(
        "is_anomaly",
        F.abs(F.col("volume_zscore")) > z_threshold
    ).withColumn(
        # Magnitud del movimiento de precio en esa ventana (valor absoluto)
        "price_move_abs",
        F.abs(F.col("log_return"))
    )

    return df_zscore.select(
        "symbol", "window_start",
        "volume", "volume_zscore", "is_anomaly",
        "log_return", "price_move_abs",
    ).orderBy("symbol", "window_start")


# ═══════════════════════════════════════════════════════════════════════════
# Q5 · Latencia end-to-end del pipeline
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿Cuánto tiempo tarda un evento desde que ocurre en Binance
#           hasta que queda persistido en Cassandra? ¿Hay degradación
#           con el tiempo o bajo alta carga?
#
# Por qué no es trivial:
#   Mide tres latencias distintas del pipeline usando los tres timestamps
#   que el sistema mantiene por trazabilidad:
#     - binance_latency:   event_time → trade_time  (latencia de Binance)
#     - network_latency:   ingestion_ts → event_time (tiempo en red/consumer)
#     - total_latency:     ingestion_ts → trade_time (end-to-end)
#
#   Detecta si hay degradación en percentiles altos (p95/p99) que indican
#   eventos extremos, no solo el promedio.
# ───────────────────────────────────────────────────────────────────────────

def q5_pipeline_latency(df_raw_trades: DataFrame) -> DataFrame:
    """
    Analiza la latencia end-to-end del pipeline por símbolo.

    Entrada: raw_trades (con trade_time, event_time, ingestion_ts).
    No usa el DataFrame de features — trabaja sobre los datos crudos
    para medir el pipeline desde su origen.

    Returns:
        DataFrame con columnas:
          symbol,
          binance_latency_ms_p50/p95/p99   (event_time - trade_time)
          network_latency_ms_p50/p95/p99   (ingestion_ts - event_time)
          total_latency_ms_p50/p95/p99     (ingestion_ts - trade_time)
          sample_count
    """
    df_latency = df_raw_trades.withColumn(
        # Latencia interna de Binance: tiempo entre ejecución y emisión del evento
        "binance_latency_ms",
        F.col("event_time") - F.col("trade_time")
    ).withColumn(
        # Latencia de red+consumer: desde que Binance emite hasta que ingestamos
        "network_latency_ms",
        (F.unix_timestamp(F.col("ingestion_ts")) * 1000).cast("long")
        - F.col("event_time")
    ).withColumn(
        # Latencia total: desde ejecución real hasta persistencia
        "total_latency_ms",
        (F.unix_timestamp(F.col("ingestion_ts")) * 1000).cast("long")
        - F.col("trade_time")
    )

    return df_latency.groupBy("symbol").agg(
        # Binance latency
        F.percentile_approx("binance_latency_ms", 0.50).alias("binance_p50_ms"),
        F.percentile_approx("binance_latency_ms", 0.95).alias("binance_p95_ms"),
        F.percentile_approx("binance_latency_ms", 0.99).alias("binance_p99_ms"),
        # Network latency
        F.percentile_approx("network_latency_ms", 0.50).alias("network_p50_ms"),
        F.percentile_approx("network_latency_ms", 0.95).alias("network_p95_ms"),
        F.percentile_approx("network_latency_ms", 0.99).alias("network_p99_ms"),
        # Total latency
        F.percentile_approx("total_latency_ms",   0.50).alias("total_p50_ms"),
        F.percentile_approx("total_latency_ms",   0.95).alias("total_p95_ms"),
        F.percentile_approx("total_latency_ms",   0.99).alias("total_p99_ms"),
        F.count("*").alias("sample_count"),
    ).orderBy("symbol")


# ═══════════════════════════════════════════════════════════════════════════
# Q6 · Presión compradora acumulada — ¿el mercado tiene sesgo direccional?
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿Hay períodos sostenidos donde los compradores dominan?
#           ¿Esa dominancia precede movimientos alcistas de precio?
#
# Por qué no es trivial:
#   Buy/sell ratio por ventana individual tiene mucho ruido.
#   La presión acumulada (suma rolling del ratio) suaviza el ruido
#   y revela tendencias de flujo de órdenes.
#   Correlaciona flujo acumulado con retorno acumulado para validar
#   la señal antes de usarla en análisis futuros.
# ───────────────────────────────────────────────────────────────────────────

def q6_cumulative_buy_pressure(df: DataFrame, rolling_n: int = 10) -> DataFrame:
    """
    Calcula presión compradora acumulada y su correlación con retorno.

    Returns:
        DataFrame con columnas:
          symbol, window_start,
          buy_sell_ratio, cumulative_pressure,
          cumulative_return, pressure_signal
          (pressure_signal: +1 si presión > 1, -1 si < 1)
    """
    w = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-rolling_n + 1, 0)
    )

    w_cumulative = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    return (
        df
        .filter(F.col("buy_sell_ratio").isNotNull())
        .withColumn(
            "cumulative_pressure",
            # Suma rolling del log(buy_sell_ratio): positivo = dominio comprador
            F.round(
                F.sum(F.log(F.col("buy_sell_ratio"))).over(w),
                6
            )
        )
        .withColumn(
            "cumulative_return",
            # Retorno acumulado en la misma ventana rolling
            F.round(F.sum("log_return").over(w), 8)
        )
        .withColumn(
            "pressure_signal",
            F.when(F.col("cumulative_pressure") > 0,  F.lit(1))
             .when(F.col("cumulative_pressure") < 0,  F.lit(-1))
             .otherwise(F.lit(0))
        )
        .select(
            "symbol", "window_start",
            "buy_sell_ratio", "cumulative_pressure",
            "cumulative_return", "pressure_signal",
        )
        .orderBy("symbol", "window_start")
    )


# ═══════════════════════════════════════════════════════════════════════════
# Q7 · VWAP tracking error — ¿qué tan bien sigue el close al VWAP?
# ═══════════════════════════════════════════════════════════════════════════
#
# Pregunta: ¿Cuánto se desvía el precio de cierre del VWAP?
#           Un tracking error alto indica distribución asimétrica de trades
#           dentro de la ventana (pocos trades grandes al final).
#
# Por qué no es trivial:
#   El VWAP es el benchmark institucional estándar para evaluar ejecuciones.
#   La diferencia close-VWAP (tracking error) tiene información sobre
#   la microestructura: si close > VWAP sistemáticamente, los trades
#   grandes ocurren al inicio de la ventana, no al final.
# ───────────────────────────────────────────────────────────────────────────

def q7_vwap_tracking_error(df: DataFrame) -> DataFrame:
    """
    Calcula y estadifica el tracking error entre close y VWAP.

    Returns:
        DataFrame con dos partes:
          1. Por ventana: symbol, window_start, close, vwap, tracking_error_pct
          2. Resumen por símbolo: mean, std, skew del tracking error
    """
    df_error = df.filter(
        F.col("vwap").isNotNull() & F.col("close").isNotNull()
    ).withColumn(
        # Error porcentual: (close - VWAP) / VWAP × 100
        # Positivo: close más alto que precio ponderado → presión al cierre
        # Negativo: close más bajo → trades grandes al inicio de ventana
        "tracking_error_pct",
        F.round(
            (F.col("close") - F.col("vwap")) / F.col("vwap") * 100,
            6
        )
    ).withColumn(
        "tracking_abs_pct",
        F.abs(F.col("tracking_error_pct"))
    )

    # Por ventana
    detail = df_error.select(
        "symbol", "window_start", "close", "vwap",
        "tracking_error_pct", "tracking_abs_pct",
    ).orderBy("symbol", "window_start")

    # Resumen por símbolo
    summary = df_error.groupBy("symbol").agg(
        F.round(F.avg("tracking_error_pct"),    6).alias("te_mean_pct"),
        F.round(F.stddev("tracking_error_pct"), 6).alias("te_std_pct"),
        F.round(F.avg("tracking_abs_pct"),      6).alias("te_abs_mean_pct"),
        F.round(F.max("tracking_abs_pct"),      6).alias("te_max_pct"),
        # Skewness manual: avg((x - mean)^3) / std^3
        F.count("*").alias("window_count"),
    )

    return detail, summary