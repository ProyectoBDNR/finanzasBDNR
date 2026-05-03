"""
analytics/run_demo.py
----------------------
Ejecuta las 7 queries analíticas con datos sintéticos y muestra resultados.

Uso:
    python -m analytics.run_demo
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.spark_session import get_spark
from processing.cleaner import clean_trades, clean_tickers
from processing.aggregator import compute_ohlcv, compute_spread_timeseries
from processing.job import make_demo_trades, make_demo_tickers
from feature_engine.features import compute_all_features, final_feature_set
from analytics.queries import (
    q1_volatility_regime,
    q2_spread_liquidity_profile,
    q3_momentum_divergence,
    q4_volume_anomalies,
    q5_pipeline_latency,
    q6_cumulative_buy_pressure,
    q7_vwap_tracking_error,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de display
# ─────────────────────────────────────────────────────────────────────────────

def header(title: str, question: str) -> None:
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"  → {question}")
    print(f"{'═' * 65}")


def subheader(text: str) -> None:
    print(f"\n  ── {text}")


def show(df, n: int = 9, cols: list[str] | None = None) -> None:
    target = df.select(cols) if cols else df
    target.show(n, truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline de datos (idéntico al de feature_engine/runner.py)
# ─────────────────────────────────────────────────────────────────────────────

def build_feature_df(spark: SparkSession):
    """Construye el DataFrame de features desde datos sintéticos."""
    df_raw_trades  = make_demo_trades(spark)
    df_raw_tickers = make_demo_tickers(spark)

    df_trades  = clean_trades(df_raw_trades)
    df_tickers = clean_tickers(df_raw_tickers)
    df_trades.cache()
    df_tickers.cache()

    df_ohlcv   = compute_ohlcv(df_trades, "1m")
    df_spread  = compute_spread_timeseries(df_tickers, "1m")
    df_features = compute_all_features(df_ohlcv)
    df_final    = final_feature_set(df_features, df_spread)
    df_final.cache()

    return df_final, df_raw_trades, df_trades, df_tickers


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    spark = get_spark("cryptoflow-analytics")
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "█" * 65)
    print("  CryptoFlow Analytics — Demo de Queries")
    print("█" * 65)

    print("\n► Construyendo dataset de features...")
    df, df_raw_trades, df_trades, df_tickers = build_feature_df(spark)
    total_windows = df.count()
    print(f"  Ventanas totales en el dataset: {total_windows}")
    print(f"  Símbolos: {[r.symbol for r in df.select('symbol').distinct().collect()]}")
    print(f"  Columnas: {len(df.columns)}")

    # ─────────────────────────────────────────────────────────────────────
    # Q1 · Régimen de volatilidad
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q1 · Régimen de Volatilidad",
        "¿En qué régimen opera cada activo y cuándo cambia de régimen?"
    )

    df_q1 = q1_volatility_regime(df)

    subheader("Distribución de regímenes por símbolo")
    df_q1.groupBy("symbol", "vol_regime").agg(
        F.count("*").alias("ventanas"),
        F.round(F.avg("rolling_volatility"), 8).alias("vol_promedio"),
    ).orderBy("symbol", "vol_regime").show(truncate=False)

    subheader("Transiciones de régimen detectadas")
    df_q1.filter(F.col("regime_change") == True).select(
        "symbol", "window_start", "prev_regime", "vol_regime",
        "rolling_volatility", "regime_duration_windows"
    ).show(8, truncate=False)

    subheader("Duración media por régimen (en ventanas de 1m)")
    df_q1.groupBy("symbol", "vol_regime").agg(
        F.round(F.avg("regime_duration_windows"), 2).alias("duracion_media_ventanas"),
        F.max("regime_duration_windows").alias("duracion_max_ventanas"),
    ).orderBy("symbol", "vol_regime").show(truncate=False)

    # ─────────────────────────────────────────────────────────────────────
    # Q2 · Spread vs liquidez
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q2 · Perfil de Liquidez — Spread vs Volumen",
        "¿El spread se degrada bajo alta actividad o la liquidez es profunda?"
    )

    df_q2 = q2_spread_liquidity_profile(df)

    subheader("Spread promedio por cuartil de volumen (Q1=bajo volumen, Q4=alto)")
    df_q2.show(12, truncate=False)

    subheader("Interpretación:")
    for row in df_q2.orderBy("symbol", "volume_quartile").collect():
        direction = "↑ spread sube con volumen" if row["avg_spread"] > 0 else ""
        print(f"  {row['symbol']} Q{row['volume_quartile']}: "
              f"spread={row['avg_spread']:.6f}  vol={row['avg_volume']:.4f}  "
              f"ratio={row['spread_vol_ratio']:.8f}  {direction}")

    # ─────────────────────────────────────────────────────────────────────
    # Q3 · Divergencia de momentum cross-asset
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q3 · Divergencia de Momentum Cross-Asset",
        "¿En qué ventanas BTC, ETH y BNB divergen en dirección de precio?"
    )

    df_q3 = q3_momentum_divergence(df)
    total_q3 = df_q3.count()
    divergent = df_q3.filter(F.col("divergence") == True).count()
    consensus = df_q3.filter(F.abs(F.col("consensus_score")) == 3).count()

    subheader("Resumen de consenso")
    print(f"  Ventanas totales analizadas : {total_q3}")
    print(f"  Con divergencia (no consenso): {divergent}  "
          f"({100*divergent/max(total_q3,1):.1f}%)")
    print(f"  Con consenso total (±3)      : {consensus}  "
          f"({100*consensus/max(total_q3,1):.1f}%)")

    subheader("Distribución del score de consenso (-3=todos bajan, +3=todos suben)")
    df_q3.groupBy("consensus_score").agg(
        F.count("*").alias("ventanas")
    ).orderBy("consensus_score").show(truncate=False)

    subheader("Ventanas de máxima divergencia (consensus_score = 0 o ±1)")
    df_q3.filter(F.abs(F.col("consensus_score")) <= 1).select(
        "window_start", "mom_BTC", "mom_ETH", "mom_BNB",
        "dir_BTC", "dir_ETH", "dir_BNB", "consensus_score",
    ).show(8, truncate=False)

    # ─────────────────────────────────────────────────────────────────────
    # Q4 · Picos de volumen anómalos
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q4 · Anomalías de Volumen (Z-score)",
        "¿Cuándo el volumen es estadísticamente anómalo? ¿Mueve el precio?"
    )

    df_q4 = q4_volume_anomalies(df, z_threshold=2.0)
    anomaly_count = df_q4.filter(F.col("is_anomaly") == True).count()
    total_q4 = df_q4.count()

    print(f"\n  Ventanas totales: {total_q4}")
    print(f"  Anomalías detectadas (|z| > 2): {anomaly_count}  "
          f"({100*anomaly_count/max(total_q4,1):.1f}%)")
    print(f"  (Esperado estadísticamente: ~5% bajo distribución normal)")

    subheader("Anomalías detectadas — ¿hay movimiento de precio asociado?")
    df_q4.filter(F.col("is_anomaly") == True).select(
        "symbol", "window_start", "volume", "volume_zscore",
        "log_return", "price_move_abs",
    ).orderBy(F.col("volume_zscore").desc()).show(10, truncate=False)

    subheader("Estadísticas de Z-score por símbolo")
    df_q4.groupBy("symbol").agg(
        F.round(F.avg("volume_zscore"),    4).alias("z_mean"),
        F.round(F.max("volume_zscore"),    4).alias("z_max"),
        F.round(F.min("volume_zscore"),    4).alias("z_min"),
        F.sum(F.col("is_anomaly").cast("int")).alias("anomalias"),
    ).orderBy("symbol").show(truncate=False)

    # ─────────────────────────────────────────────────────────────────────
    # Q5 · Latencia end-to-end
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q5 · Latencia End-to-End del Pipeline",
        "¿Cuánto tarda un trade desde Binance hasta Cassandra?"
    )

    df_q5 = q5_pipeline_latency(df_raw_trades)

    subheader("Percentiles de latencia por símbolo (ms)")
    df_q5.show(truncate=False)

    subheader("Interpretación de las tres latencias:")
    print("  binance_latency : event_time - trade_time")
    print("                    → overhead interno de Binance antes de emitir el evento")
    print("  network_latency : ingestion_ts - event_time")
    print("                    → tiempo en red + procesamiento del consumer Python")
    print("  total_latency   : ingestion_ts - trade_time")
    print("                    → latencia end-to-end real del pipeline")

    # ─────────────────────────────────────────────────────────────────────
    # Q6 · Presión compradora acumulada
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q6 · Presión Compradora Acumulada",
        "¿Hay períodos sostenidos de dominio comprador? ¿Preceden al alza?"
    )

    df_q6 = q6_cumulative_buy_pressure(df, rolling_n=10)

    subheader("Presión acumulada y retorno por símbolo (primeras ventanas con datos)")
    show(df_q6, n=12, cols=[
        "symbol", "window_start",
        "buy_sell_ratio", "cumulative_pressure",
        "cumulative_return", "pressure_signal",
    ])

    subheader("Resumen por señal de presión")
    df_q6.groupBy("symbol", "pressure_signal").agg(
        F.count("*").alias("ventanas"),
        F.round(F.avg("cumulative_return"), 6).alias("retorno_medio"),
        F.round(F.avg("buy_sell_ratio"),    4).alias("bsr_medio"),
    ).orderBy("symbol", "pressure_signal").show(truncate=False)

    # ─────────────────────────────────────────────────────────────────────
    # Q7 · VWAP tracking error
    # ─────────────────────────────────────────────────────────────────────
    header(
        "Q7 · VWAP Tracking Error",
        "¿Cuánto se desvía el close del VWAP? ¿Hay sesgo sistemático?"
    )

    df_q7_detail, df_q7_summary = q7_vwap_tracking_error(df)

    subheader("Resumen de tracking error por símbolo")
    df_q7_summary.show(truncate=False)

    subheader("Detalle por ventana — mayor tracking error (positivo y negativo)")
    df_q7_detail.orderBy(F.col("tracking_abs_pct").desc()).select(
        "symbol", "window_start", "close", "vwap",
        "tracking_error_pct", "tracking_abs_pct",
    ).show(8, truncate=False)

    subheader("Interpretación:")
    print("  te_mean_pct > 0 → el close tiende a estar sobre el VWAP")
    print("                    los trades grandes ocurren al inicio de la ventana")
    print("  te_mean_pct < 0 → el close tiende a estar bajo el VWAP")
    print("                    los trades grandes ocurren al final de la ventana")
    print("  te_abs_mean_pct → desviación típica — cuanto mayor, más asimétrica")
    print("                    es la distribución de trades dentro de la ventana")

    # ─────────────────────────────────────────────────────────────────────
    # Resumen final
    # ─────────────────────────────────────────────────────────────────────
    print("\n" + "█" * 65)
    print("  Resumen — Queries ejecutadas")
    print("█" * 65)
    queries = [
        ("Q1", "Régimen de volatilidad + transiciones",       "Spark Window"),
        ("Q2", "Spread vs volumen — perfil de liquidez",      "Spark ntile"),
        ("Q3", "Divergencia de momentum cross-asset",         "Spark pivot"),
        ("Q4", "Anomalías de volumen via Z-score",            "Spark Window"),
        ("Q5", "Latencia end-to-end del pipeline",            "Spark agg"),
        ("Q6", "Presión compradora acumulada rolling",        "Spark Window"),
        ("Q7", "VWAP tracking error por ventana",             "Spark agg"),
    ]
    print(f"\n  {'Query':<6} {'Descripción':<45} {'Técnica'}")
    print(f"  {'─'*6} {'─'*45} {'─'*15}")
    for q, desc, tech in queries:
        print(f"  {q:<6} {desc:<45} {tech}")

    print(f"\n  Dataset: {total_windows} ventanas · 3 símbolos · OHLCV 1m")
    print(f"  Columnas en df_final: {len(df.columns)}\n")

    spark.stop()


if __name__ == "__main__":
    run()