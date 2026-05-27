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
from feature_engine.amihud import add_amihud_illiq

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
    _section("5 · Dataset final  (features + spread + amihud)")
    df_final = final_feature_set(df_features, df_spread)
    # Amihud (2002): price impact realizado, escala bp/M USD.
    # Se aplica acá para que se persista junto al resto de features.
    df_final = add_amihud_illiq(df_final, periods=60)
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

    # ── 7. Persistencia en Cassandra ─────────────────────────────
    _section("7 ·  features_by_window")
    # features_by_window: una fila por (symbol, window_label, window_start)
    # window_label ya está en df_final (viene de compute_ohlcv)
    cols_features = [
        "symbol", "window_label", "window_start", "window_end",
        "vwap", "log_return", "rolling_volatility", "realized_volatility",
        "momentum", "momentum_pct", "buy_sell_ratio", "return_autocorr",
        "spread_mean", "spread_mean_pct", "spread_min", "spread_max",
        "spread_std", "mid_price_mean", "tick_count", "obi",
        "open", "high", "low", "close", "volume",
        "trade_count", "buy_volume", "sell_volume",
        "amihud_illiq",   # Amihud (2002) — bp por millón USD operado
    ]
    # Solo columnas que existen en df_final
    cols_to_write = [c for c in cols_features if c in df_final.columns]
    df_features_write = df_final.select(cols_to_write).dropna(subset=["window_start"])
    n_features = df_features_write.count()
    print(f"  Filas a escribir: {n_features}")
    _show(df_features_write, cols=[
        "symbol", "window_start",
        "spread_mean", "spread_min", "spread_max", "tick_count",
    ])
    try:
        (
            df_features_write.write
            .format("org.apache.spark.sql.cassandra")
            .options(table="features_by_window", keyspace="cryptoflow")
            .mode("append")
            .save()
        )
        print("  ✓ features_by_window escritas")
    except Exception as e:
        print(f"  ⚠ features_by_window omitido: {e}")

    # spread_timeseries: necesita window_label como partition key
    try:
        spread_cols = [
            "symbol", "window_label", "window_start", "window_end",
            "spread_mean", "spread_min", "spread_max", "spread_std",
            "spread_mean_pct", "mid_price_mean", "tick_count",
            "bid_qty_mean", "ask_qty_mean", "obi",
        ]
        # df_spread no tiene window_label ni window_end — los agregamos
        from pyspark.sql.functions import lit
        df_spread_write = df_spread
        if "window_label" not in df_spread.columns:
            df_spread_write = df_spread_write.withColumn("window_label", lit("1m"))
        if "window_end" not in df_spread.columns:
            df_spread_write = df_spread_write.withColumn("window_end", F.col("window_start"))
        cols_spread = [c for c in spread_cols if c in df_spread_write.columns]
        df_spread_write = df_spread_write.select(cols_spread)
        (
            df_spread_write.write
            .format("org.apache.spark.sql.cassandra")
            .options(table="spread_timeseries", keyspace="cryptoflow")
            .mode("append")
            .save()
        )
        print("  ✓ spread_timeseries escritas")
    except Exception as e:
        print(f"  ⚠ spread_timeseries omitido: {e}")

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
    parser.add_argument(
        "--date",
        default=None,
    )
    args = parser.parse_args()
    # Si no se pasa --date, usar demo. Si se pasa --date, producción.
    if args.date is None:
        date  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        demo  = True
    else:
        date  = args.date
        demo  = args.demo   # solo demo si se pasa --demo explícitamente
    run(date=date, demo=demo)