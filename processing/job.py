"""
processing/job.py
------------------
Job principal de Spark para CryptoFlow Analytics.

Pipeline:
    1. Leer raw_trades y raw_book_tickers desde Cassandra
    2. Limpiar y deduplicar (processing/cleaner.py)
    3. Agregar en ventanas OHLCV (processing/aggregator.py)
    4. Persistir resultados en Cassandra

Nota: CoinGecko fue eliminado del pipeline. Los tres activos monitoreados
(BTC, ETH, BNB) son conocidos y no requieren metadata externa para los
features cuantitativos que calcula este sistema.

Uso:
    # Datos reales desde Cassandra
    python -m processing.job --date 2026-05-04

    # Datos sintéticos (sin Cassandra)
    python -m processing.job --demo

Variables de entorno:
    CASSANDRA_HOSTS    (default: 127.0.0.1)
    CASSANDRA_PORT     (default: 9042)
    CASSANDRA_KEYSPACE (default: cryptoflow)
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
from processing.aggregator import compute_all_windows, compute_spread_timeseries
from feature_engine.microprice import (
    compute_micro_price_raw, aggregate_micro_price_by_window,
)
from feature_engine.cointegration import compute_pairwise_zscore, PAIRS
from feature_engine.amihud import add_amihud_illiq
from analytics.export import (
    export_meta, export_ohlcv, export_spread,
    export_features, export_latency,
)

KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")


# ---------------------------------------------------------------------------
# Lectura desde Cassandra
# ---------------------------------------------------------------------------

def read_raw_trades(spark: SparkSession, date: str) -> DataFrame:
    """Lee raw_trades filtrando por fecha para evitar full table scan."""
    return (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table="raw_trades", keyspace=KEYSPACE)
        .load()
        .filter(F.col("date") == date)
    )


def read_raw_book_tickers(spark: SparkSession, date: str) -> DataFrame:
    """Lee raw_book_tickers filtrando por fecha."""
    return (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table="raw_book_tickers", keyspace=KEYSPACE)
        .load()
        .filter(F.col("date") == date)
    )


# ---------------------------------------------------------------------------
# Escritura en Cassandra
# ---------------------------------------------------------------------------

def write_ohlcv(df: DataFrame, table: str) -> None:
    """
    Escribe DataFrame OHLCV en Cassandra de forma idempotente.

    Usa mode("append") con el conector Cassandra — que internamente
    hace UPSERT (INSERT ... IF NOT EXISTS) por clave primaria.
    Si el scheduler corre dos veces para la misma fecha, los registros
    existentes no se duplican porque la PRIMARY KEY (symbol, window_label,
    window_start) garantiza unicidad en Cassandra.

    Nota: a diferencia de bases de datos relacionales, Cassandra no
    soporta "overwrite por partición" via Spark. La idempotencia viene
    garantizada por la semántica UPSERT del protocolo CQL — una escritura
    con la misma clave primaria simplemente sobrescribe el valor existente.
    """
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=KEYSPACE)
        .mode("append")   # UPSERT por PRIMARY KEY — idempotente en Cassandra
        .save()
    )


def write_spread(df: DataFrame) -> None:
    """Escribe spread_timeseries en Cassandra."""
    cols_cassandra = [
        "symbol", "date", "window_start",
        "spread_mean", "spread_min", "spread_max", "spread_std",
        "mid_price_mean", "tick_count",
    ]
    available = [c for c in cols_cassandra if c in df.columns]
    df_out = df.withColumn(
        "date", F.date_format(F.col("window_start"), "yyyy-MM-dd")
    ).select(available)
    (
        df_out.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="spread_timeseries", keyspace=KEYSPACE)
        .mode("append")
        .save()
    )


def write_microprice(df: DataFrame) -> None:
    """
    Escribe microprice_by_window en Cassandra (mode append → UPSERT por PK).
    Idempotente: PK ((symbol, window_label), window_start).
    """
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="microprice_by_window", keyspace=KEYSPACE)
        .mode("append")
        .save()
    )


def write_pairs_zscore(df: DataFrame) -> None:
    """
    Escribe pairs_zscore_1m en Cassandra (mode append → UPSERT por PK).
    PK ((sym_dom, sym_hedge, lookback), window_start).
    """
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .options(table="pairs_zscore_1m", keyspace=KEYSPACE)
        .mode("append")
        .save()
    )


# ---------------------------------------------------------------------------
# Datos sintéticos para modo demo
# ---------------------------------------------------------------------------

def make_demo_trades(spark: SparkSession) -> DataFrame:
    """Genera trades sintéticos para demostración (sin Cassandra)."""
    import random
    from datetime import timedelta

    random.seed(42)
    base_prices = {"BTCUSDT": 67_000.0, "ETHUSDT": 3_500.0, "BNBUSDT": 580.0}
    base_ts = int(datetime(2024, 6, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    rows = []
    for sym, base_price in base_prices.items():
        price = base_price
        for i in range(200):
            price *= (1 + random.gauss(0, 0.0002))
            trade_time = base_ts + i * 3_000
            rows.append((
                sym, "2024-06-10",
                trade_time, trade_time + 2,
                1_000_000 + i,
                round(price, 2),
                round(random.uniform(0.001, 2.0), 6),
                random.choice([True, False]),
                f"trace-{sym}-{i}",
                datetime.fromtimestamp(trade_time / 1000, tz=timezone.utc),
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
        for i in range(300):
            price *= (1 + random.gauss(0, 0.0001))
            spread = random.uniform(0.5, 5.0)
            event_time = base_ts + i * 2_000
            rows.append((
                sym, "2024-06-10",
                event_time,
                round(price - spread / 2, 2),
                round(random.uniform(0.5, 10.0), 4),
                round(price + spread / 2, 2),
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
# Pipeline principal
# ---------------------------------------------------------------------------

def run(date: str, demo: bool = False) -> None:
    """
    Ejecuta el pipeline completo para una fecha dada.

    Args:
        date: Fecha a procesar (yyyy-mm-dd)
        demo: Si True usa datos sintéticos sin necesitar Cassandra
    """
    spark = get_spark("cryptoflow-processing")
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*60}")
    print(f"  CryptoFlow Processing Job")
    print(f"  Fecha: {date}  |  Demo: {demo}")
    print(f"{'='*60}\n")

    # ── 1. Ingesta ────────────────────────────────────────────────
    print("► [1/4] Leyendo datos raw...")
    if demo:
        df_raw_trades  = make_demo_trades(spark)
        df_raw_tickers = make_demo_tickers(spark)
    else:
        df_raw_trades  = read_raw_trades(spark, date)
        df_raw_tickers = read_raw_book_tickers(spark, date)

    trades_raw  = df_raw_trades.count()
    tickers_raw = df_raw_tickers.count()
    print(f"  raw_trades:  {trades_raw:,} filas")
    print(f"  raw_tickers: {tickers_raw:,} filas")

    # ── 2. Limpieza ───────────────────────────────────────────────
    print("\n► [2/4] Limpiando y deduplicando...")
    df_trades  = clean_trades(df_raw_trades)
    df_tickers = clean_tickers(df_raw_tickers)

    trades_clean  = df_trades.count()
    tickers_clean = df_tickers.count()

    dedup_trades  = trades_raw - trades_clean
    dedup_tickers = tickers_raw - tickers_clean
    print(f"  Trades:  {trades_raw:,} raw → {trades_clean:,} limpios "
          f"({dedup_trades:,} duplicados/inválidos eliminados)")
    print(f"  Tickers: {tickers_raw:,} raw → {tickers_clean:,} limpios "
          f"({dedup_tickers:,} duplicados/inválidos eliminados)")

    df_trades.cache()
    df_tickers.cache()

    # ── 3. Agregaciones OHLCV ─────────────────────────────────────
    print("\n► [3/4] Calculando OHLCV y spread timeseries...")
    ohlcv_by_window = compute_all_windows(df_trades)

    for label, df_ohlcv in ohlcv_by_window.items():
        count = df_ohlcv.count()
        print(f"  OHLCV {label}: {count} ventanas")

    spreads_by_window = {
        label: compute_spread_timeseries(df_tickers, label)
        for label in ["1m", "5m", "1h"]
    }
    for label, df_sp in spreads_by_window.items():
        print(f"  Spread {label}: {df_sp.count()} ventanas")
    df_spread = spreads_by_window["1m"]  # alias para compatibilidad

    # Micro-Price (hftbacktest) — agrega por las mismas ventanas que spread.
    # Se hace una sola vez en raw y se agrupa después.
    print("\n► Calculando Micro-Price (hftbacktest)...")
    df_mp_raw = compute_micro_price_raw(df_tickers)
    df_mp_raw.cache()
    microprice_by_window = {
        label: aggregate_micro_price_by_window(df_mp_raw, label)
        for label in ["1m", "5m", "1h"]
    }
    for label, df_mp in microprice_by_window.items():
        print(f"  Micro-Price {label}: {df_mp.count()} ventanas")

    # Cointegración cross-asset (BTC-ETH, BTC-BNB, ETH-BNB) sobre ohlcv_1m.
    # Es la primera feature que cruza símbolos — se computa una vez sobre 1m.
    print("\n► Calculando z-score de cointegración rolling (pairs)...")
    try:
        df_zscore = compute_pairwise_zscore(spark, ohlcv_by_window["1m"], PAIRS)
        df_zscore.cache()
        zscore_count = df_zscore.count()
        print(f"  pairs_zscore_1m: {zscore_count} filas ({len(PAIRS)} pares)")
    except Exception as e:
        df_zscore = None
        print(f"  ⚠ cointegración omitida: {e}")

    # ── 4. Persistencia ───────────────────────────────────────────
    if not demo:
        print("\n► [4/4] Escribiendo en Cassandra...")
        table_map = {"1m": "ohlcv_1m", "5m": "ohlcv_5m", "1h": "ohlcv_1h"}
        for label, df_ohlcv in ohlcv_by_window.items():
            write_ohlcv(df_ohlcv, table_map[label])
            print(f"  ✓ {table_map[label]}: {df_ohlcv.count()} ventanas")
        for label, df_sp in spreads_by_window.items():
            sp_count = df_sp.count()
            if sp_count > 0:
                try:
                    write_spread(df_sp)
                    print(f"  ✓ spread_timeseries {label}: {sp_count} ventanas")
                except Exception as e:
                    print(f"  ⚠ spread_timeseries {label} omitido: {e}")
        for label, df_mp in microprice_by_window.items():
            mp_count = df_mp.count()
            if mp_count > 0:
                try:
                    write_microprice(df_mp)
                    print(f"  ✓ microprice_by_window {label}: {mp_count} ventanas")
                except Exception as e:
                    print(f"  ⚠ microprice_by_window {label} omitido: {e}")
        if df_zscore is not None and zscore_count > 0:
            try:
                write_pairs_zscore(df_zscore)
                print(f"  ✓ pairs_zscore_1m: {zscore_count} filas")
            except Exception as e:
                print(f"  ⚠ pairs_zscore_1m omitido: {e}")

        # ── Exportar JSON para el dashboard ──────────────────────────
        print("\n► Exportando datos para el dashboard...")
        try:
            from feature_engine.features import compute_all_features, final_feature_set

            # Exportar OHLCV para las tres resoluciones
            export_ohlcv(
                ohlcv_by_window.get("1m"),
                df_ohlcv_5m=ohlcv_by_window.get("5m"),
                df_ohlcv_1h=ohlcv_by_window.get("1h"),
            )

            # Importar enriquecimiento estático (metadata por símbolo)
            from processing.enrichment import enrich_features

            # Exportar features para cada resolución
            for res_label in ["1m", "5m", "1h"]:
                df_ohlcv_res = ohlcv_by_window.get(res_label)
                df_spread_res = spreads_by_window.get(res_label)
                if df_ohlcv_res is not None and df_ohlcv_res.count() > 0:
                    df_feat_res  = compute_all_features(df_ohlcv_res)
                    df_final_res = final_feature_set(df_feat_res, df_spread_res) if df_spread_res else df_feat_res
                    # Cruce con dataset estático (Etapa 4): metadata por símbolo
                    try:
                        df_final_res = enrich_features(df_final_res, spark)
                    except Exception as enrich_err:
                        print(f"  ⚠ enrich_features omitido ({res_label}): {enrich_err}")
                    # Amihud (2002): price impact realizado por dólar operado
                    try:
                        df_final_res = add_amihud_illiq(df_final_res, periods=60)
                    except Exception as amihud_err:
                        print(f"  ⚠ add_amihud_illiq omitido ({res_label}): {amihud_err}")
                    export_features(df_final_res, label=res_label)

            # Meta usa 1m como referencia principal
            df_ohlcv_1m = ohlcv_by_window.get("1m")
            df_feat_1m  = compute_all_features(df_ohlcv_1m)
            df_final_1m = final_feature_set(df_feat_1m, spreads_by_window.get("1m"))

            export_meta(date, trades_raw, tickers_raw,
                        trades_clean, tickers_clean,
                        sum(df.count() for df in ohlcv_by_window.values()))
            export_spread(spreads_by_window)
            export_latency(df_raw_trades)
            print("  ✓ analytics/data/ actualizado (1m · 5m · 1h)")
        except Exception as e:
            import traceback
            print(f"  ⚠ Export JSON omitido: {e}")
            traceback.print_exc()
    else:
        print("\n► [4/4] Modo demo — escritura omitida.")

    df_trades.unpersist()
    df_tickers.unpersist()
    try:
        df_mp_raw.unpersist()
    except Exception:
        pass
    if df_zscore is not None:
        try:
            df_zscore.unpersist()
        except Exception:
            pass

    print(f"\n{'='*60}")
    print(f"  Job completado — {date}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CryptoFlow Spark Processing Job")
    parser.add_argument("--demo",  action="store_true",
                        help="Datos sintéticos sin Cassandra")
    parser.add_argument("--date",  type=str,
                        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        help="Fecha a procesar (yyyy-mm-dd)")
    args = parser.parse_args()
    run(date=args.date, demo=args.demo)