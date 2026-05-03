"""
processing/job.py
------------------
Job principal de Spark para CryptoFlow Analytics.

Orquesta el pipeline completo:
    1. Leer raw_trades y raw_book_tickers desde Cassandra
    2. Limpiar y deduplicar (processing/cleaner.py)
    3. Enriquecer con metadata de CoinGecko (enrichment/coingecko.py)
    4. Agregar en ventanas OHLCV (processing/aggregator.py)
    5. Persistir resultados en Cassandra

Uso:
    # Modo normal (lee Cassandra real)
    python -m processing.job

    # Modo demo con datos sintéticos (sin Cassandra)
    python -m processing.job --demo

    # Mock CoinGecko (sin request HTTP)
    COINGECKO_MOCK=true python -m processing.job --demo

Variables de entorno:
    CASSANDRA_HOSTS    (default: 127.0.0.1)
    CASSANDRA_PORT     (default: 9042)
    CASSANDRA_KEYSPACE (default: cryptoflow)
    COINGECKO_MOCK     (default: false)
    PROCESS_DATE       (default: hoy en UTC, formato yyyy-mm-dd)
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, BooleanType, TimestampType
)

from processing.spark_session import get_spark
from processing.cleaner import clean_trades, clean_tickers
from processing.aggregator import compute_all_windows, compute_spread_timeseries, WINDOWS
from enrichment.coingecko import get_all_metadata

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")


# ---------------------------------------------------------------------------
# 1. Lectura desde Cassandra
# ---------------------------------------------------------------------------

def read_raw_trades(spark: SparkSession, date: str) -> DataFrame:
    """
    Lee raw_trades para una fecha específica (partition key = symbol + date).
    Filtra por date en el pushdown para evitar full table scan.
    """
    return (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table="raw_trades", keyspace=KEYSPACE)
        .load()
        .filter(F.col("date") == date)
    )


def read_raw_book_tickers(spark: SparkSession, date: str) -> DataFrame:
    """Lee raw_book_tickers para una fecha específica."""
    return (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table="raw_book_tickers", keyspace=KEYSPACE)
        .load()
        .filter(F.col("date") == date)
    )


# ---------------------------------------------------------------------------
# 2. Enriquecimiento con CoinGecko
# ---------------------------------------------------------------------------

def build_metadata_df(spark: SparkSession, use_mock: bool = True) -> DataFrame:
    """
    Convierte la metadata de CoinGecko en un DataFrame de Spark.
    Una fila por símbolo — se hace broadcast join contra trades.

    Broadcast join: la metadata tiene 3 filas (una por símbolo).
    Spark la envía a cada executor en memoria — sin shuffle, sin particiones.
    """
    records = get_all_metadata(use_mock=use_mock)
    schema = StructType([
        StructField("symbol",             StringType(),  True),
        StructField("coingecko_id",       StringType(),  True),
        StructField("market_cap_usd",     DoubleType(),  True),
        StructField("market_cap_rank",    LongType(),    True),
        StructField("circulating_supply", DoubleType(),  True),
        StructField("total_supply",       DoubleType(),  True),
        StructField("category",           StringType(),  True),
        StructField("description",        StringType(),  True),
    ])
    return spark.createDataFrame(records, schema=schema)


def enrich_trades(df_trades: DataFrame, df_meta: DataFrame) -> DataFrame:
    """
    Join broadcast entre trades limpios y metadata de CoinGecko.

    F.broadcast() fuerza a Spark a enviar df_meta (3 filas) a cada
    executor en lugar de hacer un shuffle de df_trades.
    """
    return df_trades.join(
        F.broadcast(df_meta),
        on="symbol",
        how="left",     # left para no perder trades de símbolos sin metadata
    )


# ---------------------------------------------------------------------------
# 3. Escritura en Cassandra
# ---------------------------------------------------------------------------

def write_ohlcv(df: DataFrame, table: str) -> None:
    """
    Escribe un DataFrame OHLCV en Cassandra.
    Modo: append — nunca sobreescribe particiones existentes.
    """
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .mode("append")
        .save()
    )


# ---------------------------------------------------------------------------
# 4. Demo con datos sintéticos (sin Cassandra real)
# ---------------------------------------------------------------------------

def make_demo_trades(spark: SparkSession) -> DataFrame:
    """
    Genera un DataFrame de trades sintéticos para demostración.
    Simula 10 minutos de actividad para 3 símbolos.
    """
    import random
    from datetime import timedelta

    random.seed(42)
    base_prices = {"BTCUSDT": 67_000.0, "ETHUSDT": 3_500.0, "BNBUSDT": 580.0}
    base_ts = int(datetime(2024, 6, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    rows = []
    for sym, base_price in base_prices.items():
        price = base_price
        for i in range(200):                      # 200 trades por símbolo
            price *= (1 + random.gauss(0, 0.0002))
            trade_time = base_ts + i * 3_000      # cada ~3 segundos
            rows.append((
                sym,
                "2024-06-10",
                trade_time,                        # trade_time (ms)
                trade_time + 2,                    # event_time (ms)
                1_000_000 + i,                     # agg_trade_id
                round(price, 2),                   # price
                round(random.uniform(0.001, 2.0), 6),  # quantity
                random.choice([True, False]),       # is_buyer_maker
                f"trace-{sym}-{i}",                # trace_id (string para demo)
                datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc),  # ingestion_ts
            ))

    schema = StructType([
        StructField("symbol",         StringType(),   False),
        StructField("date",           StringType(),   False),
        StructField("trade_time",     LongType(),     False),
        StructField("event_time",     LongType(),     False),
        StructField("agg_trade_id",   LongType(),     False),
        StructField("price",          DoubleType(),   False),
        StructField("quantity",       DoubleType(),   False),
        StructField("is_buyer_maker", BooleanType(),  False),
        StructField("trace_id",       StringType(),   False),
        StructField("ingestion_ts",   TimestampType(), False),
    ])
    return spark.createDataFrame(rows, schema=schema)


def make_demo_tickers(spark: SparkSession) -> DataFrame:
    """Genera book tickers sintéticos para demostración."""
    import random

    random.seed(99)
    base_prices = {"BTCUSDT": 67_000.0, "ETHUSDT": 3_500.0, "BNBUSDT": 580.0}
    base_ts = int(datetime(2024, 6, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    rows = []
    for sym, base_price in base_prices.items():
        price = base_price
        for i in range(300):                       # más frecuente que trades
            price *= (1 + random.gauss(0, 0.0001))
            spread = random.uniform(0.5, 5.0)
            event_time = base_ts + i * 2_000
            rows.append((
                sym,
                "2024-06-10",
                event_time,
                round(price - spread / 2, 2),      # best_bid_price
                round(random.uniform(0.5, 10.0), 4),
                round(price + spread / 2, 2),      # best_ask_price
                round(random.uniform(0.5, 10.0), 4),
                round(spread, 4),
                f"trace-ticker-{sym}-{i}",
                datetime.fromtimestamp(event_time / 1000, tz=timezone.utc),
            ))

    schema = StructType([
        StructField("symbol",          StringType(),   False),
        StructField("date",            StringType(),   False),
        StructField("event_time",      LongType(),     False),
        StructField("best_bid_price",  DoubleType(),   False),
        StructField("best_bid_qty",    DoubleType(),   False),
        StructField("best_ask_price",  DoubleType(),   False),
        StructField("best_ask_qty",    DoubleType(),   False),
        StructField("spread",          DoubleType(),   False),
        StructField("trace_id",        StringType(),   False),
        StructField("ingestion_ts",    TimestampType(), False),
    ])
    return spark.createDataFrame(rows, schema=schema)


# ---------------------------------------------------------------------------
# 5. Pipeline principal
# ---------------------------------------------------------------------------

def run(date: str, demo: bool = False, use_mock_coingecko: bool = True) -> None:
    """
    Ejecuta el pipeline completo para una fecha dada.

    Args:
        date:                Fecha a procesar en formato yyyy-mm-dd
        demo:                Si True, usa datos sintéticos (sin Cassandra)
        use_mock_coingecko:  Si True, usa mock de CoinGecko (sin HTTP)
    """
    spark = get_spark("cryptoflow-processing")
    spark.sparkContext.setLogLevel("WARN")   # silenciar logs de Spark internos

    print(f"\n{'='*60}")
    print(f"  CryptoFlow Processing Job")
    print(f"  Fecha: {date}  |  Demo: {demo}")
    print(f"{'='*60}\n")

    # ── 1. Ingesta ────────────────────────────────────────────────
    print("► [1/5] Leyendo datos raw...")
    if demo:
        df_raw_trades  = make_demo_trades(spark)
        df_raw_tickers = make_demo_tickers(spark)
    else:
        df_raw_trades  = read_raw_trades(spark, date)
        df_raw_tickers = read_raw_book_tickers(spark, date)

    print("\n=== DEBUG RAW TRADES ===")
    df_raw_trades.printSchema()
    df_raw_trades.show(5, False)

    print("\n=== DEBUG RAW TICKERS ===")
    df_raw_tickers.printSchema()
    df_raw_tickers.show(5, False)

    print(f"  raw_trades:  {df_raw_trades.count():,} filas")
    print(f"  raw_tickers: {df_raw_tickers.count():,} filas")

    # ── 2. Limpieza ───────────────────────────────────────────────
    print("\n► [2/5] Limpiando y deduplicando...")

    df_raw_tickers.select(
    "symbol",
    "event_time",
    "ingestion_ts"
    ).show(20, False)

    df_trades  = clean_trades(df_raw_trades)
    df_tickers = clean_tickers(df_raw_tickers)

    print("\n=== DEBUG CLEAN TRADES ===")
    df_trades.printSchema()
    df_trades.show(5, False)

    print("\n=== DEBUG CLEAN TICKERS ===")
    df_tickers.printSchema()
    df_tickers.show(5, False)

    trades_clean_count  = df_trades.count()
    tickers_clean_count = df_tickers.count()
    print(f"  trades limpios:  {trades_clean_count:,}")
    print(f"  tickers limpios: {tickers_clean_count:,}")

    # Cachear: se va a usar en múltiples agregaciones
    df_trades.cache()
    df_tickers.cache()

    # ── 3. Enriquecimiento ────────────────────────────────────────
    print("\n► [3/5] Enriqueciendo con CoinGecko...")
    df_meta = build_metadata_df(spark, use_mock=use_mock_coingecko)
    df_enriched = enrich_trades(df_trades, df_meta)
    print("  Metadata disponible para símbolos:")
    df_meta.select("symbol", "market_cap_rank", "market_cap_usd", "category").show(
        truncate=False
    )

    # ── 4. Agregaciones OHLCV ─────────────────────────────────────
    print("► [4/5] Calculando OHLCV por ventanas...")
    ohlcv_by_window = compute_all_windows(df_trades)

    for label, df_ohlcv in ohlcv_by_window.items():
        count = df_ohlcv.count()
        print(f"\n  OHLCV {label} — {count} ventanas")
        df_ohlcv.select(
            "symbol", "window_start", "open", "high", "low", "close",
            "volume", "trade_count"
        ).show(6, truncate=False)

    # Spread timeseries (1m)
    print("► Calculando spread timeseries (1m)...")
    df_spread = compute_spread_timeseries(df_tickers, "1m")
    df_spread.select(
        "symbol", "window_start", "spread_mean", "spread_min", "spread_max", "tick_count"
    ).show(6, truncate=False)

    # ── 5. Persistencia ───────────────────────────────────────────
    if not demo:
        print("\n► [5/5] Escribiendo en Cassandra...")
        table_map = {"1m": "ohlcv_1m", "5m": "ohlcv_5m", "1h": "ohlcv_1h"}
        for label, df_ohlcv in ohlcv_by_window.items():
            write_ohlcv(df_ohlcv, table_map[label])
            print(f"  Escritos {label} → {table_map[label]}")
    else:
        print("\n► [5/5] Modo demo — escritura en Cassandra omitida.")

    # Liberar caché
    df_trades.unpersist()
    df_tickers.unpersist()

    print(f"\n{'='*60}")
    print("  Job completado.")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CryptoFlow Spark Processing Job")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Ejecutar con datos sintéticos sin Cassandra",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Fecha a procesar (yyyy-mm-dd). Default: hoy UTC.",
    )
    parser.add_argument(
        "--mock-coingecko",
        action="store_true",
        default=True,
        help="Usar mock de CoinGecko sin request HTTP (default: True)",
    )
    args = parser.parse_args()

    run(
        date=args.date,
        demo=args.demo,
        use_mock_coingecko=args.mock_coingecko,
    )