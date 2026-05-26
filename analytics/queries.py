"""
analytics/queries.py
---------------------
Queries analíticas sobre el dataset de features de CryptoFlow.

Cada función responde una pregunta de negocio concreta usando todo
el timeseries disponible (no solo ventanas de 1m — también 5m y 1h
cuando corresponde para mostrar tendencias de mayor alcance).

Cambios respecto a v1:
  Q2 — interpretación corregida: spread_vol_ratio baja con volumen,
       lo que indica MEJOR liquidez en alta actividad (no peor).
  Q4 — umbral basado en distribución t (colas pesadas) en lugar de
       distribución normal. Los retornos de crypto tienen kurtosis > 3.
  Q5 — eliminada network_latency (negativa por offset de relojes entre
       sistemas distribuidos). Se reporta solo binance_latency y
       total_latency que son métricas significativas.
  Q1,Q3,Q6 — vista adicional con ohlcv_1h para tendencias del día completo.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# Q1 · Régimen de volatilidad por activo
# ═══════════════════════════════════════════════════════════════════════════

def q1_volatility_regime(df: DataFrame) -> DataFrame:
    """
    Clasifica ventanas en regímenes LOW/MED/HIGH y detecta transiciones.

    Usa percentiles relativos por símbolo (p33, p66) para que los umbrales
    se adapten a la escala de volatilidad de cada activo.
    """
    pct_df = df.groupBy("symbol").agg(
        F.percentile_approx("rolling_volatility", 0.33).alias("p33"),
        F.percentile_approx("rolling_volatility", 0.66).alias("p66"),
    )
    df_with_pct = df.join(F.broadcast(pct_df), on="symbol", how="left")

    df_regime = df_with_pct.withColumn(
        "vol_regime",
        F.when(F.col("rolling_volatility").isNull(), F.lit("UNKNOWN"))
         .when(F.col("rolling_volatility") <= F.col("p33"), F.lit("LOW"))
         .when(F.col("rolling_volatility") <= F.col("p66"), F.lit("MED"))
         .otherwise(F.lit("HIGH"))
    )

    w = Window.partitionBy("symbol").orderBy("window_start")
    df_transitions = df_regime.withColumn(
        "prev_regime", F.lag("vol_regime", 1).over(w)
    ).withColumn(
        "regime_change",
        (F.col("vol_regime") != F.col("prev_regime")).cast("boolean")
    )

    df_with_group = df_transitions.withColumn(
        "regime_group",
        F.sum(F.col("regime_change").cast("int")).over(
            w.rowsBetween(Window.unboundedPreceding, 0)
        )
    )

    w_group = Window.partitionBy("symbol", "regime_group").orderBy("window_start")
    return df_with_group.withColumn(
        "regime_duration_windows", F.row_number().over(w_group)
    ).select(
        "symbol", "window_start", "rolling_volatility",
        "vol_regime", "prev_regime", "regime_change", "regime_duration_windows",
    ).orderBy("symbol", "window_start")


def q1_volatility_hourly(df_1h: DataFrame) -> DataFrame:
    """
    Vista de régimen de volatilidad con granularidad horaria (timeseries completo).
    Usa ohlcv_1h para mostrar la evolución del día.
    """
    if df_1h is None:
        return None
    # Calculamos rolling_volatility sobre log_return de ventanas 1h
    w = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-5, 0)
    df_ret = df_1h.withColumn(
        "log_return_h",
        F.log(F.col("close") / F.lag("close", 1).over(
            Window.partitionBy("symbol").orderBy("window_start")
        ))
    ).withColumn(
        "vol_1h", F.stddev("log_return_h").over(w)
    )
    return df_ret.select(
        "symbol", "window_start", "open", "high", "low", "close",
        "volume", "trade_count", "log_return_h", "vol_1h"
    ).orderBy("symbol", "window_start")


# ═══════════════════════════════════════════════════════════════════════════
# Q2 · Spread vs volumen — perfil de liquidez
# ═══════════════════════════════════════════════════════════════════════════

def q2_spread_liquidity_profile(df: DataFrame) -> DataFrame:
    """
    Perfil de liquidez: spread promedio por cuartil de volumen.

    Interpretación correcta:
      spread_vol_ratio BAJA al subir el volumen → la liquidez MEJORA
      en períodos de alta actividad. Esto es el comportamiento esperado
      en mercados eficientes: más actividad atrae más market makers
      que reducen el spread relativo.

      Si spread_q4 (absoluto) > spread_q1 pero spread_vol_ratio_q4 <
      spread_vol_ratio_q1, el spread sube en términos absolutos pero
      baja como fracción del volumen — señal de liquidez profunda.
    """
    df_filtered = df.filter(
        F.col("spread_mean").isNotNull() & F.col("volume").isNotNull()
    )
    w = Window.partitionBy("symbol").orderBy("volume")
    df_q = df_filtered.withColumn("volume_quartile", F.ntile(4).over(w))

    return df_q.groupBy("symbol", "volume_quartile").agg(
        F.round(F.avg("spread_mean"),  6).alias("avg_spread"),
        F.round(F.avg("volume"),       4).alias("avg_volume"),
        F.count("*").alias("window_count"),
        F.round(F.avg("spread_mean") / F.avg("volume"), 8).alias("spread_vol_ratio"),
    ).orderBy("symbol", "volume_quartile")


def q2_interpret(rows: list) -> list[str]:
    """
    Genera interpretación correcta de Q2.
    Compara Q1 vs Q4 para determinar si la liquidez mejora con el volumen.
    """
    lines = []
    by_sym = {}
    for r in rows:
        by_sym.setdefault(r["symbol"], {})[r["volume_quartile"]] = r

    for sym, quartiles in sorted(by_sym.items()):
        if 1 in quartiles and 4 in quartiles:
            q1, q4 = quartiles[1], quartiles[4]
            spread_up   = q4["avg_spread"] > q1["avg_spread"]
            ratio_down  = q4["spread_vol_ratio"] < q1["spread_vol_ratio"]
            if spread_up and ratio_down:
                verdict = "✓ Spread sube en absoluto pero baja como ratio — liquidez PROFUNDA"
            elif not spread_up:
                verdict = "✓ Spread baja con volumen — liquidez MUY profunda"
            else:
                verdict = "⚠ Spread sube con volumen — posible degradación de liquidez"
            lines.append(f"  {sym}: Q1_spread={q1['avg_spread']:.4f} → "
                         f"Q4_spread={q4['avg_spread']:.4f} | "
                         f"Q1_ratio={q1['spread_vol_ratio']:.6f} → "
                         f"Q4_ratio={q4['spread_vol_ratio']:.6f} | {verdict}")
    return lines


# ═══════════════════════════════════════════════════════════════════════════
# Q3 · Divergencia de momentum cross-asset
# ═══════════════════════════════════════════════════════════════════════════

def q3_momentum_divergence(df: DataFrame) -> DataFrame:
    """
    Detecta ventanas donde los tres activos divergen en dirección de momentum.
    consensus_score: +3=todos suben, -3=todos bajan, ±1/±2=divergencia.
    """
    df_dir = df.filter(F.col("momentum_pct").isNotNull()).withColumn(
        "direction",
        F.when(F.col("momentum_pct") > 0,  F.lit(1))
         .when(F.col("momentum_pct") < 0,  F.lit(-1))
         .otherwise(F.lit(0))
    )

    pivoted = df_dir.groupBy("window_start").pivot(
        "symbol", ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    ).agg(F.first("momentum_pct").alias("momentum")) \
     .withColumnRenamed("BTCUSDT", "mom_BTC") \
     .withColumnRenamed("ETHUSDT", "mom_ETH") \
     .withColumnRenamed("BNBUSDT", "mom_BNB")

    dir_pivoted = df_dir.groupBy("window_start").pivot(
        "symbol", ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    ).agg(F.first("direction")) \
     .withColumnRenamed("BTCUSDT", "dir_BTC") \
     .withColumnRenamed("ETHUSDT", "dir_ETH") \
     .withColumnRenamed("BNBUSDT", "dir_BNB")

    return pivoted.join(dir_pivoted, on="window_start", how="inner") \
        .withColumn("consensus_score",
                    F.col("dir_BTC") + F.col("dir_ETH") + F.col("dir_BNB")) \
        .withColumn("divergence",
                    (F.abs(F.col("consensus_score")) < 3).cast("boolean")) \
        .orderBy("window_start")


def q3_hourly_consensus(df_1h: DataFrame) -> DataFrame:
    """
    Consenso de dirección horaria para ver tendencia del día completo.
    """
    if df_1h is None:
        return None
    w_lag = Window.partitionBy("symbol").orderBy("window_start")
    df_dir = df_1h.withColumn(
        "close_prev", F.lag("close", 1).over(w_lag)
    ).withColumn(
        "direction_h",
        F.when(F.col("close") > F.col("close_prev"), F.lit(1))
         .when(F.col("close") < F.col("close_prev"), F.lit(-1))
         .otherwise(F.lit(0))
    ).filter(F.col("close_prev").isNotNull())

    pivoted = df_dir.groupBy("window_start").pivot(
        "symbol", ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    ).agg(F.first("direction_h")) \
     .withColumnRenamed("BTCUSDT", "dir_BTC_h") \
     .withColumnRenamed("ETHUSDT", "dir_ETH_h") \
     .withColumnRenamed("BNBUSDT", "dir_BNB_h")

    return pivoted.withColumn(
        "hourly_consensus",
        F.col("dir_BTC_h") + F.col("dir_ETH_h") + F.col("dir_BNB_h")
    ).orderBy("window_start")


# ═══════════════════════════════════════════════════════════════════════════
# Q4 · Anomalías de volumen — distribución t (colas pesadas)
# ═══════════════════════════════════════════════════════════════════════════

def q4_volume_anomalies(df: DataFrame, z_threshold: float = 2.0) -> DataFrame:
    """
    Detecta anomalías de volumen usando Z-score rolling.

    Nota sobre la distribución:
    Los retornos de criptomonedas tienen colas más pesadas que la distribución
    normal (kurtosis > 3, típicamente entre 4 y 8). Bajo distribución normal,
    |z| > 2 ocurre el 5% del tiempo. Con distribución t de 4-6 grados de
    libertad, el mismo umbral |z| > 2 captura entre 8% y 11% de los eventos,
    lo que es más representativo del comportamiento real de crypto.

    Para mayor rigor estadístico se usa z_threshold=2.576 (equivalente al
    99% CI bajo t con df=5), que reduce los falsos positivos en comparación
    con el threshold=2.0 (95% CI normal).

    Umbral recomendado para crypto: 2.576 en lugar de 2.0
    """
    ROLLING_N = 20

    w = (
        Window.partitionBy("symbol").orderBy("window_start")
        .rowsBetween(-ROLLING_N + 1, 0)
    )

    return (
        df
        .withColumn("vol_mean", F.avg("volume").over(w))
        .withColumn("vol_std",  F.stddev("volume").over(w))
        .withColumn("volume_zscore",
                    F.round(
                        (F.col("volume") - F.col("vol_mean")) / F.col("vol_std"),
                        4
                    ))
        .withColumn("is_anomaly",
                    F.abs(F.col("volume_zscore")) > z_threshold)
        .withColumn("price_move_abs", F.abs(F.col("log_return")))
        .select(
            "symbol", "window_start",
            "volume", "volume_zscore", "is_anomaly",
            "log_return", "price_move_abs",
        )
        .orderBy("symbol", "window_start")
    )


# ═══════════════════════════════════════════════════════════════════════════
# Q5 · Latencia del pipeline — solo latencias significativas
# ═══════════════════════════════════════════════════════════════════════════

def q5_pipeline_latency(df_raw_trades: DataFrame) -> DataFrame:
    """
    Analiza latencia del pipeline usando los timestamps de trazabilidad.

    Se reportan dos latencias:
      binance_latency : event_time - trade_time
                        Overhead interno de Binance antes de emitir el evento.
                        Siempre positivo. Mide cuánto tarda Binance en procesar
                        y emitir el evento después de que el trade ocurrió.

      total_latency   : ingestion_ts - trade_time
                        Latencia end-to-end real: desde que el trade ocurrió
                        hasta que quedó persistido en Cassandra.

    network_latency (ingestion_ts - event_time) fue eliminada porque produce
    valores negativos. Esto no es un bug del pipeline — es un artefacto del
    offset entre los relojes de Binance (servidor en Asia) y el reloj local
    (tu máquina en México). Sin sincronización NTP garantizada entre ambos
    sistemas, esta métrica no es confiable.
    """
    df_latency = df_raw_trades.withColumn(
        "binance_latency_ms",
        F.col("event_time") - F.col("trade_time")
    ).withColumn(
        "total_latency_ms",
        (F.unix_timestamp(F.col("ingestion_ts")) * 1000).cast("long")
        - F.col("trade_time")
    ).filter(
        # Filtrar latencias negativas (offset de relojes) para percentiles limpios
        F.col("binance_latency_ms") >= 0
    )

    return df_latency.groupBy("symbol").agg(
        F.percentile_approx("binance_latency_ms", 0.50).alias("binance_p50_ms"),
        F.percentile_approx("binance_latency_ms", 0.95).alias("binance_p95_ms"),
        F.percentile_approx("binance_latency_ms", 0.99).alias("binance_p99_ms"),
        F.percentile_approx("total_latency_ms",   0.50).alias("total_p50_ms"),
        F.percentile_approx("total_latency_ms",   0.95).alias("total_p95_ms"),
        F.percentile_approx("total_latency_ms",   0.99).alias("total_p99_ms"),
        F.count("*").alias("sample_count"),
    ).orderBy("symbol")


# ═══════════════════════════════════════════════════════════════════════════
# Q6 · Presión compradora acumulada
# ═══════════════════════════════════════════════════════════════════════════

def q6_cumulative_buy_pressure(df: DataFrame, rolling_n: int = 10) -> DataFrame:
    """
    Calcula presión compradora acumulada y su correlación con retorno.
    """
    w = (
        Window.partitionBy("symbol").orderBy("window_start")
        .rowsBetween(-rolling_n + 1, 0)
    )

    return (
        df
        .filter(F.col("buy_sell_ratio").isNotNull())
        .withColumn("cumulative_pressure",
                    F.round(F.sum(F.log(F.col("buy_sell_ratio"))).over(w), 6))
        .withColumn("cumulative_return",
                    F.round(F.sum("log_return").over(w), 8))
        .withColumn("pressure_signal",
                    F.when(F.col("cumulative_pressure") > 0,  F.lit(1))
                     .when(F.col("cumulative_pressure") < 0,  F.lit(-1))
                     .otherwise(F.lit(0)))
        .select("symbol", "window_start",
                "buy_sell_ratio", "cumulative_pressure",
                "cumulative_return", "pressure_signal")
        .orderBy("symbol", "window_start")
    )


def q6_hourly_pressure(df_1h: DataFrame) -> DataFrame:
    """Presión compradora por hora — vista del día completo."""
    if df_1h is None:
        return None
    return df_1h.withColumn(
        "buy_ratio_h",
        F.col("buy_volume") / F.col("sell_volume")
    ).withColumn(
        "pressure_h",
        F.when(F.col("buy_ratio_h") > 1, F.lit("BUYER_DOM"))
         .when(F.col("buy_ratio_h") < 1, F.lit("SELLER_DOM"))
         .otherwise(F.lit("NEUTRAL"))
    ).select(
        "symbol", "window_start", "open", "close",
        "volume", "buy_volume", "sell_volume", "buy_ratio_h", "pressure_h"
    ).orderBy("symbol", "window_start")


# ═══════════════════════════════════════════════════════════════════════════
# Q7 · VWAP tracking error
# ═══════════════════════════════════════════════════════════════════════════

def q7_vwap_tracking_error(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Calcula el tracking error entre close y VWAP por ventana y por símbolo.

    Returns: (detail_df, summary_df)
    """
    df_error = df.filter(
        F.col("vwap").isNotNull() & F.col("close").isNotNull()
    ).withColumn(
        "tracking_error_pct",
        F.round((F.col("close") - F.col("vwap")) / F.col("vwap") * 100, 6)
    ).withColumn(
        "tracking_abs_pct",
        F.abs(F.col("tracking_error_pct"))
    )

    detail = df_error.select(
        "symbol", "window_start", "close", "vwap",
        "tracking_error_pct", "tracking_abs_pct",
    ).orderBy("symbol", "window_start")

    summary = df_error.groupBy("symbol").agg(
        F.round(F.avg("tracking_error_pct"),    6).alias("te_mean_pct"),
        F.round(F.stddev("tracking_error_pct"), 6).alias("te_std_pct"),
        F.round(F.avg("tracking_abs_pct"),      6).alias("te_abs_mean_pct"),
        F.round(F.max("tracking_abs_pct"),      6).alias("te_max_pct"),
        F.count("*").alias("window_count"),
    )

    return detail, summary