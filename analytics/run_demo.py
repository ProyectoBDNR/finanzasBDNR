"""
analytics/run_demo.py
----------------------
Ejecuta las 7 queries analíticas con datos sintéticos o reales.

Uso:
    # Demo con datos sintéticos
    python -m analytics.run_demo

    # Datos reales desde Cassandra
    python -m analytics.run_demo --date 2026-05-04
"""

from __future__ import annotations

import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.spark_session import get_spark
from processing.cleaner import clean_trades, clean_tickers
from processing.aggregator import compute_ohlcv, compute_spread_timeseries, compute_all_windows
from processing.job import make_demo_trades, make_demo_tickers
from feature_engine.features import compute_all_features, final_feature_set
from analytics.backtest import run_bsr_backtest, print_backtest_results
from analytics.queries import (
    q1_volatility_regime, q1_volatility_hourly,
    q2_spread_liquidity_profile, q2_interpret,
    q3_momentum_divergence, q3_hourly_consensus,
    q4_volume_anomalies,
    q5_pipeline_latency,
    q6_cumulative_buy_pressure, q6_hourly_pressure,
    q7_vwap_tracking_error,
)
from analytics.queries_microprice import q8_microprice_divergence_predicts_movement
from analytics.queries_cointegration import q9_cointegration_extreme_divergences
from analytics.queries_amihud import q10_amihud_cross_asset
from feature_engine.amihud import add_amihud_illiq

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")


def header(title: str, question: str) -> None:
    print(f"\n{'═'*65}")
    print(f"  {title}")
    print(f"  → {question}")
    print(f"{'═'*65}")


def subheader(text: str) -> None:
    print(f"\n  ── {text}")


def build_feature_df(spark: SparkSession, demo: bool = True, date: str = None):
    """Construye DataFrame de features desde datos sintéticos o Cassandra."""
    if demo:
        df_raw_trades  = make_demo_trades(spark)
        df_raw_tickers = make_demo_tickers(spark)
    else:
        df_raw_trades = (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table="raw_trades", keyspace=KEYSPACE).load()
            .filter(F.col("date") == date).cache()
        )
        df_raw_tickers = (
            spark.read.format("org.apache.spark.sql.cassandra")
            .options(table="raw_book_tickers", keyspace=KEYSPACE).load()
            .filter(F.col("date") == date).cache()
        )

    df_trades  = clean_trades(df_raw_trades)
    df_tickers = clean_tickers(df_raw_tickers)
    df_trades.cache()
    df_tickers.cache()

    df_ohlcv   = compute_ohlcv(df_trades, "1m")
    df_spread  = compute_spread_timeseries(df_tickers, "1m")
    df_features = compute_all_features(df_ohlcv)
    df_final    = final_feature_set(df_features, df_spread)
    # Amihud (2002) — añade columna amihud_illiq (bp/M USD) sobre el DataFrame
    df_final    = add_amihud_illiq(df_final, periods=60)
    df_final.cache()

    # Datos horarios y tablas auxiliares solo aplican en modo Cassandra real
    df_1h = None
    df_microprice = None
    df_zscore = None
    if not demo:
        try:
            df_1h = (
                spark.read.format("org.apache.spark.sql.cassandra")
                .options(table="ohlcv_1h", keyspace=KEYSPACE).load()
                .filter(F.date_format(F.col("window_start"), "yyyy-MM-dd") == date)
                .cache()
            )
        except Exception:
            df_1h = None

        # microprice_by_window — alimenta Q8
        try:
            df_microprice = (
                spark.read.format("org.apache.spark.sql.cassandra")
                .options(table="microprice_by_window", keyspace=KEYSPACE).load()
                .filter(F.date_format(F.col("window_start"), "yyyy-MM-dd") == date)
                .filter(F.col("window_label") == "1m")
                .cache()
            )
        except Exception:
            df_microprice = None

        # pairs_zscore_1m — alimenta Q9
        try:
            df_zscore = (
                spark.read.format("org.apache.spark.sql.cassandra")
                .options(table="pairs_zscore_1m", keyspace=KEYSPACE).load()
                .filter(F.date_format(F.col("window_start"), "yyyy-MM-dd") == date)
                .cache()
            )
        except Exception:
            df_zscore = None

    return df_final, df_raw_trades, df_1h, df_microprice, df_zscore


def run(demo: bool = True, date: str = None) -> None:
    spark = get_spark("cryptoflow-analytics")
    spark.sparkContext.setLogLevel("ERROR")

    title = "Demo de Queries" if demo else f"Analytics con datos reales — {date}"
    print("\n" + "█"*65)
    print(f"  CryptoFlow Analytics — {title}")
    print("█"*65)

    print("\n► Construyendo dataset de features...")
    df, df_raw_trades, df_1h, df_microprice, df_zscore = build_feature_df(
        spark, demo=demo, date=date
    )
    total_windows = df.count()
    print(f"  Ventanas totales: {total_windows}")
    print(f"  Símbolos: {[r.symbol for r in df.select('symbol').distinct().collect()]}")
    print(f"  Columnas: {len(df.columns)}")

    # ── Q1 ───────────────────────────────────────────────────────────────
    header("Q1 · Régimen de Volatilidad",
           "¿En qué régimen opera cada activo y cuándo cambia de régimen?")

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

    subheader("Duración media por régimen (ventanas de 1m)")
    df_q1.groupBy("symbol", "vol_regime").agg(
        F.round(F.avg("regime_duration_windows"), 2).alias("duracion_media"),
        F.max("regime_duration_windows").alias("duracion_max"),
    ).orderBy("symbol", "vol_regime").show(truncate=False)

    if df_1h is not None:
        subheader("Vista horaria — volatilidad del día completo (OHLCV 1h)")
        df_q1_h = q1_volatility_hourly(df_1h)
        if df_q1_h is not None:
            df_q1_h.select(
                "symbol", "window_start", "open", "close", "volume", "vol_1h"
            ).show(12, truncate=False)

    # ── Q2 ───────────────────────────────────────────────────────────────
    header("Q2 · Perfil de Liquidez — Spread vs Volumen",
           "¿El spread se degrada bajo alta actividad o la liquidez es profunda?")

    df_q2 = q2_spread_liquidity_profile(df)
    subheader("Spread promedio por cuartil de volumen (Q1=bajo, Q4=alto)")
    df_q2.show(12, truncate=False)

    subheader("Interpretación — comparación Q1 vs Q4 por símbolo:")
    rows = df_q2.orderBy("symbol", "volume_quartile").collect()
    for line in q2_interpret(rows):
        print(line)

    print("\n  Nota: spread_vol_ratio = spread / volumen.")
    print("  Un ratio MENOR en Q4 indica que el spread relativo al volumen")
    print("  baja con la actividad — señal de MAYOR liquidez en períodos activos.")

    # ── Q3 ───────────────────────────────────────────────────────────────
    header("Q3 · Divergencia de Momentum Cross-Asset",
           "¿En qué ventanas BTC, ETH y BNB divergen en dirección de precio?")

    df_q3 = q3_momentum_divergence(df)
    total_q3  = df_q3.count()
    divergent = df_q3.filter(F.col("divergence") == True).count()
    consensus = df_q3.filter(F.abs(F.col("consensus_score")) == 3).count()

    subheader("Resumen de consenso")
    print(f"  Ventanas totales     : {total_q3}")
    print(f"  Con divergencia      : {divergent}  ({100*divergent/max(total_q3,1):.1f}%)")
    print(f"  Con consenso total   : {consensus}  ({100*consensus/max(total_q3,1):.1f}%)")

    subheader("Distribución del score de consenso (-3=todos bajan, +3=todos suben)")
    df_q3.groupBy("consensus_score").agg(
        F.count("*").alias("ventanas")
    ).orderBy("consensus_score").show(truncate=False)

    subheader("Ventanas de máxima divergencia")
    df_q3.filter(F.abs(F.col("consensus_score")) <= 1).select(
        "window_start", "mom_BTC", "mom_ETH", "mom_BNB",
        "dir_BTC", "dir_ETH", "dir_BNB", "consensus_score",
    ).show(8, truncate=False)

    if df_1h is not None:
        subheader("Consenso horario — dirección del día completo (OHLCV 1h)")
        df_q3_h = q3_hourly_consensus(df_1h)
        if df_q3_h is not None:
            df_q3_h.show(12, truncate=False)

    # ── Q4 ───────────────────────────────────────────────────────────────
    header("Q4 · Anomalías de Volumen (distribución t, colas pesadas)",
           "¿Cuándo el volumen es estadísticamente anómalo? ¿Mueve el precio?")

    # Umbral 2.576 ≈ 99% CI bajo t(df=5) — más apropiado para crypto
    df_q4 = q4_volume_anomalies(df, z_threshold=2.576)
    anomaly_count = df_q4.filter(F.col("is_anomaly") == True).count()
    total_q4 = df_q4.count()

    print(f"\n  Ventanas totales: {total_q4}")
    print(f"  Anomalías (|z| > 2.576): {anomaly_count}  ({100*anomaly_count/max(total_q4,1):.1f}%)")
    print(f"  Umbral: 2.576 (equivalente a 99% CI bajo distribución t con df=5)")
    print(f"  Justificación: retornos de crypto tienen kurtosis > 3 (colas pesadas).")
    print(f"  Bajo distribución normal, |z|>2.576 ocurre ~1%. Con t(df=5) ~4-6%.")

    subheader("Anomalías detectadas — ¿hay movimiento de precio asociado?")
    df_q4.filter(F.col("is_anomaly") == True).select(
        "symbol", "window_start", "volume", "volume_zscore",
        "log_return", "price_move_abs",
    ).orderBy(F.col("volume_zscore").desc()).show(10, truncate=False)

    subheader("Estadísticas de Z-score por símbolo")
    df_q4.groupBy("symbol").agg(
        F.round(F.avg("volume_zscore"), 4).alias("z_mean"),
        F.round(F.max("volume_zscore"), 4).alias("z_max"),
        F.round(F.min("volume_zscore"), 4).alias("z_min"),
        F.sum(F.col("is_anomaly").cast("int")).alias("anomalias"),
    ).orderBy("symbol").show(truncate=False)

    # ── Q5 ───────────────────────────────────────────────────────────────
    header("Q5 · Latencia del Pipeline",
           "¿Cuánto tarda un trade desde Binance hasta Cassandra?")

    df_q5 = q5_pipeline_latency(df_raw_trades)
    subheader("Percentiles de latencia por símbolo (ms)")
    df_q5.show(truncate=False)

    subheader("Interpretación:")
    print("  binance_latency : event_time - trade_time")
    print("                    overhead interno de Binance (siempre positivo)")
    print("  total_latency   : ingestion_ts - trade_time")
    print("                    latencia end-to-end real del pipeline")
    print("\n  Nota: network_latency fue eliminada — producía valores negativos")
    print("  por el offset de relojes entre Binance (servidor en Asia) y")
    print("  la máquina local. Sin NTP sincronizado entre ambos sistemas,")
    print("  esta métrica no es confiable.")

    # ── Q6 ───────────────────────────────────────────────────────────────
    header("Q6 · Presión Compradora Acumulada",
           "¿Hay períodos sostenidos de dominio comprador? ¿Preceden al alza?")

    df_q6 = q6_cumulative_buy_pressure(df, rolling_n=10)
    subheader("Presión acumulada (primeras ventanas con datos)")
    df_q6.select(
        "symbol", "window_start", "buy_sell_ratio",
        "cumulative_pressure", "cumulative_return", "pressure_signal",
    ).show(12, truncate=False)

    subheader("Resumen por señal de presión")
    df_q6.groupBy("symbol", "pressure_signal").agg(
        F.count("*").alias("ventanas"),
        F.round(F.avg("cumulative_return"), 6).alias("retorno_medio"),
        F.round(F.avg("buy_sell_ratio"),    4).alias("bsr_medio"),
    ).orderBy("symbol", "pressure_signal").show(truncate=False)

    if df_1h is not None:
        subheader("Presión compradora horaria — día completo (OHLCV 1h)")
        df_q6_h = q6_hourly_pressure(df_1h)
        if df_q6_h is not None:
            df_q6_h.show(12, truncate=False)

    # ── Q7 ───────────────────────────────────────────────────────────────
    header("Q7 · VWAP Tracking Error",
           "¿Cuánto se desvía el close del VWAP? ¿Hay sesgo sistemático?")

    df_q7_detail, df_q7_summary = q7_vwap_tracking_error(df)
    subheader("Resumen de tracking error por símbolo")
    df_q7_summary.show(truncate=False)

    subheader("Detalle — mayor tracking error (positivo y negativo)")
    df_q7_detail.orderBy(F.col("tracking_abs_pct").desc()).select(
        "symbol", "window_start", "close", "vwap",
        "tracking_error_pct", "tracking_abs_pct",
    ).show(8, truncate=False)

    subheader("Interpretación:")
    print("  te_mean_pct > 0 → close > VWAP → trades grandes al inicio de ventana")
    print("  te_mean_pct < 0 → close < VWAP → trades grandes al final de ventana")
    print("  te_abs_mean_pct → mayor valor = distribución más asimétrica de trades")

    # ── Q8 — Micro-Price predice forward return (hftbacktest) ─────────────
    header("Q8 · Micro-Price divergence vs forward return",
           "¿La divergencia micro_price − mid_price[t] predice log_return[t+1]?")
    if df_microprice is None:
        print("  ⚠ microprice_by_window no disponible (demo o tabla vacía).")
    else:
        df_q8 = q8_microprice_divergence_predicts_movement(df, df_microprice)
        subheader("Forward return y hit rate por quintil de divergencia")
        df_q8.show(truncate=False)
        subheader("Interpretación:")
        print("  Esperado: avg_forward_return monotónicamente creciente con quintil_div.")
        print("  Hit rate del quintile 5 > 50% indica que la señal tiene poder predictivo")
        print("  (típico 52-56% en cripto HF).")

    # ── Q9 — Cointegración cross-asset (z-score por pares) ───────────────
    header("Q9 · Cointegración z-score · divergencias extremas",
           "¿Qué pares cripto presentan más eventos de divergencia (|z| > 2)?")
    if df_zscore is None:
        print("  ⚠ pairs_zscore_1m no disponible (demo o tabla vacía).")
    else:
        df_q9 = q9_cointegration_extreme_divergences(df_zscore, abs_threshold=2.0)
        subheader("Resumen de divergencias por par")
        df_q9.show(truncate=False)
        subheader("Interpretación:")
        print("  Pares más cointegrados (BTC-ETH) deberían tener menos eventos")
        print("  y rachas más cortas que pares menos cointegrados (BTC-BNB).")

    # ── Q10 — Amihud illiquidity por símbolo y régimen ────────────────────
    header("Q10 · Amihud illiquidity cross-asset",
           "¿Cuánto se mueve el precio por dólar transado, por régimen?")
    if "amihud_illiq" not in df.columns:
        print("  ⚠ amihud_illiq no disponible en df_features.")
    else:
        df_q10 = q10_amihud_cross_asset(df)
        subheader("Iliquidez (bp/M USD) por símbolo × régimen de volatilidad")
        df_q10.show(truncate=False)
        subheader("Interpretación:")
        print("  Esperado: illiq sube en régimen HIGH (market makers se retiran).")
        print("  Ranking esperado: BTC < ETH < BNB (BTC el más profundo).")

    # ── Backtest ──────────────────────────────────────────────────────────
    header("Backtest · BSR como señal predictiva",
           "¿Un BSR alto predice retornos positivos en las siguientes 3 ventanas?")
    try:
        bt = run_bsr_backtest(df, bsr_threshold=1.2, forward_windows=3)
        print_backtest_results(bt, bsr_threshold=1.2, forward_windows=3)
    except Exception as e:
        print(f"  ⚠ Backtest omitido: {e}")

    # ── Resumen ───────────────────────────────────────────────────────────
    print("\n" + "█"*65)
    print("  Resumen — Queries ejecutadas")
    print("█"*65)
    queries = [
        ("Q1", "Régimen de volatilidad + timeseries horario",  "Spark Window"),
        ("Q2", "Spread vs volumen — liquidez corregida",        "Spark ntile"),
        ("Q3", "Divergencia momentum + consenso horario",       "Spark pivot"),
        ("Q4", "Anomalías de volumen — dist. t colas pesadas",  "Spark Window"),
        ("Q5", "Latencia pipeline (sin network_latency)",       "Spark agg"),
        ("Q6", "Presión compradora + timeseries horario",       "Spark Window"),
        ("Q7", "VWAP tracking error por ventana",               "Spark agg"),
        ("Q8", "Micro-Price divergence → forward return",       "Spark ntile"),
        ("Q9", "Cointegración z-score · divergencias extremas", "Spark Window"),
        ("Q10","Amihud illiquidity por régimen (cross-asset)",  "Spark agg"),
        ("BT", "Backtest BSR → forward return (predictivo)",       "Spark Window"),
    ]
    print(f"\n  {'Query':<6} {'Descripción':<48} {'Técnica'}")
    print(f"  {'─'*6} {'─'*48} {'─'*15}")
    for q, desc, tech in queries:
        print(f"  {q:<6} {desc:<48} {tech}")
    print(f"\n  Dataset: {total_windows} ventanas · 3 símbolos · OHLCV 1m")
    print(f"  Columnas en df_final: {len(df.columns)}\n")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    args = parser.parse_args()
    if args.date:
        run(demo=False, date=args.date)
    else:
        run(demo=True)