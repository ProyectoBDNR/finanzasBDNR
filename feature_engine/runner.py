"""
feature_engine/runner.py
-------------------------
Orquestador del Feature Engine.

Toma datos limpios (o sintéticos en modo demo), calcula todas las features
y muestra los resultados tabulados. En producción escribe a Cassandra.

Uso:
    python -m feature_engine.runner           # demo con datos sintéticos
    python -m feature_engine.runner --date 2024-06-10   # producción
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.spark_session import get_spark
from processing.aggregator import compute_ohlcv, compute_spread_timeseries
from feature_engine.features import compute_all_features, final_feature_set

# Reutilizamos los generadores demo del job de processing
from processing.job import make_demo_trades, make_demo_tickers
from processing.cleaner import clean_trades, clean_tickers


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

def _section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def _show(df, n: int = 8, cols: list[str] | None = None) -> None:
    target = df.select(cols) if cols else df
    target.show(n, truncate=False)


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

def run(date: str, demo: bool = True) -> None:
    spark = get_spark("cryptoflow-features")
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n{'═' * 60}")
    print(f"  CryptoFlow — Feature Engine")
    print(f"  Fecha: {date}  |  Demo: {demo}")
    print(f"{'═' * 60}")

    # ── 1. Datos ──────────────────────────────────────────────────
    _section("1 · Datos de entrada")
    if demo:
        df_raw_trades  = make_demo_trades(spark)
        df_raw_tickers = make_demo_tickers(spark)
    else:
        from processing.job import read_raw_trades, read_raw_book_tickers
        df_raw_trades  = read_raw_trades(spark, date)
        df_raw_tickers = read_raw_book_tickers(spark, date)

    df_trades  = clean_trades(df_raw_trades)
    df_tickers = clean_tickers(df_raw_tickers)
    df_trades.cache()
    df_tickers.cache()

    print(f"  Trades limpios : {df_trades.count():,}")
    print(f"  Tickers limpios: {df_tickers.count():,}")

    # ── 2. OHLCV 1m ───────────────────────────────────────────────
    _section("2 · OHLCV 1 minuto")
    df_ohlcv = compute_ohlcv(df_trades, "1m")
    df_ohlcv.cache()

    print(f"  Ventanas generadas: {df_ohlcv.count()}")
    _show(df_ohlcv, cols=[
        "symbol", "window_start",
        "open", "high", "low", "close", "volume", "trade_count",
    ])

    # ── 3. Spread timeseries 1m ───────────────────────────────────
    _section("3 · Spread timeseries 1m (base para spread_mean)")
    df_spread = compute_spread_timeseries(df_tickers, "1m")
    df_spread.cache()
    _show(df_spread, cols=[
        "symbol", "window_start",
        "spread_mean", "spread_min", "spread_max", "tick_count",
    ])

    # ── 4. Features completas ─────────────────────────────────────
    _section("4 · Feature Engine completo")
    df_features = compute_all_features(df_ohlcv)

    # 4a. VWAP
    _section("  4a · VWAP  [Σ(price×qty) / Σ(qty)]")
    _show(df_features, cols=["symbol", "window_start", "open", "close", "vwap", "volume"])

    # 4b. log_return
    _section("  4b · log_return  [ln(close_t / close_{t-1})]")
    _show(df_features, cols=["symbol", "window_start", "close", "log_return"])

    # 4c. rolling_volatility
    _section("  4c · rolling_volatility  [stddev(log_return, N=10)]")
    _show(df_features, cols=["symbol", "window_start", "log_return", "rolling_volatility"])

    # 4d. momentum
    _section("  4d · momentum  [close_t - close_{t-5}]  &  momentum_pct")
    _show(df_features, cols=["symbol", "window_start", "close", "momentum", "momentum_pct"])

    # 4e. buy_sell_ratio
    _section("  4e · buy_sell_ratio  [buy_vol / sell_vol]")
    _show(df_features, cols=[
        "symbol", "window_start", "buy_volume", "sell_volume", "buy_sell_ratio",
    ])

    # ── 5. Dataset final unificado ────────────────────────────────
    _section("5 · Dataset final  (features + spread)")
    df_final = final_feature_set(df_features, df_spread)
    count_final = df_final.count()
    print(f"  Filas totales: {count_final}")
    print(f"  Columnas     : {len(df_final.columns)}")
    print(f"  Columnas     : {df_final.columns}\n")

    _show(df_final, cols=[
        "symbol", "window_start",
        "vwap", "log_return", "rolling_volatility",
        "momentum_pct", "buy_sell_ratio",
        "spread_mean", "spread_mean_pct",
        "trade_count",
    ])

    # ── 6. Estadísticas descriptivas ─────────────────────────────
    _section("6 · Estadísticas descriptivas por símbolo")
    df_final.groupBy("symbol").agg(
        F.round(F.avg("vwap"),               2).alias("vwap_avg"),
        F.round(F.avg("rolling_volatility"), 8).alias("vol_avg"),
        F.round(F.avg("spread_mean"),        6).alias("spread_avg"),
        F.round(F.avg("momentum_pct"),       6).alias("momentum_avg"),
        F.round(F.avg("buy_sell_ratio"),     4).alias("bsr_avg"),
        F.count("*").alias("windows"),
    ).orderBy("symbol").show(truncate=False)

    # ── 7. Escritura a Cassandra (solo producción) ────────────────
    if not demo:
        _section("7 ·  features_by_window")

        # spread_timeseries no tiene window_label — seleccionamos solo
        # las columnas definidas en el DDL de schemas/cassandra.cql
        df_spread_cassandra = df_spread.withColumn(
            "date", F.date_format(F.col("window_start"), "yyyy-MM-dd")
        ).select(
            "symbol", "date", "window_start",
            "spread_mean", "spread_min", "spread_max", "spread_std",
            "mid_price_mean", "tick_count",
        )

        print(f"  Filas a escribir: {df_spread_cassandra.count()}")
        _show(df_spread_cassandra, cols=[
            "symbol", "window_start",
            "spread_mean", "spread_min", "spread_max", "tick_count",
        ])

        df_spread_cassandra.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="spread_timeseries", keyspace="cryptoflow") \
            .mode("append") \
            .save()

        _section("7 ·  features_by_window")
        print(f"  Filas a escribir: {df_final.count()}")
        _show(df_final, cols=[
            "symbol", "window_start",
            "vwap", "log_return", "rolling_volatility",
            "momentum_pct", "buy_sell_ratio",
            "spread_mean", "trade_count",
        ])

        df_final.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="features_by_window", keyspace="cryptoflow") \
            .mode("append") \
            .save()

    # Liberar caché
    df_trades.unpersist()
    df_tickers.unpersist()
    df_ohlcv.unpersist()
    df_spread.unpersist()

    print(f"\n{'═' * 60}")
    print("  Feature Engine completado.")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", default=False)
    parser.add_argument("--date", default=None)
    args = parser.parse_args()

    # Si viene --date asumimos producción; si viene --demo o nada, demo.
    if args.date is not None:
        demo = False
        date = args.date
    else:
        demo = True
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    run(date=date, demo=demo)