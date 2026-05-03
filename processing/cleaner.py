"""
processing/cleaner.py
----------------------
Limpieza, deduplicación y normalización de DataFrames raw de Cassandra.

Cada función es pura: recibe un DataFrame, retorna un DataFrame.
Sin efectos secundarios — fácil de testear y de encadenar.

Pipeline aplicado en orden:
    1. drop_nulls       → elimina filas con campos críticos nulos
    2. validate_ranges  → filtra precios/cantidades fuera de rango
    3. cast_types       → garantiza tipos correctos tras la lectura de Cassandra
    4. add_derived      → columnas derivadas (trade_datetime, log_price)
    5. deduplicate      → elimina duplicados por clave de negocio
"""

from __future__ import annotations


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, BooleanType, TimestampType, StringType
)

# ---------------------------------------------------------------------------
# Constantes de validación
# ---------------------------------------------------------------------------

# Rangos razonables para activos cripto en condiciones normales de mercado.
# Son intencionalmente amplios para no filtrar eventos legítimos extremos.
PRICE_MIN   = 0.000_001   # cubre monedas de muy bajo valor
PRICE_MAX   = 10_000_000  # por encima de cualquier precio histórico de BTC
QTY_MIN     = 0.000_000_1
QTY_MAX     = 1_000_000

# Campos que deben estar presentes en cada evento
TRADE_REQUIRED_COLS   = ["symbol", "price", "quantity", "trade_time"]
TICKER_REQUIRED_COLS  = ["symbol", "best_bid_price", "best_ask_price", "event_time"]

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------

def _ensure_ingestion_ts(df: DataFrame) -> DataFrame:
    """Garantiza columna ingestion_ts para deduplicación."""
    if "ingestion_ts" not in df.columns:
        return df.withColumn("ingestion_ts", F.current_timestamp())
    return df


def _normalize_event_time(df: DataFrame) -> DataFrame:
    """
    Binance WS puede traer:
      - E
      - eventTime
      - event_time
    """
    if "event_time" in df.columns:
        return df
    if "E" in df.columns:
        return df.withColumn("event_time", F.col("E"))
    if "eventTime" in df.columns:
        return df.withColumn("event_time", F.col("eventTime"))
    return df


# ---------------------------------------------------------------------------W
# 1. Drop nulls
# ---------------------------------------------------------------------------

def drop_nulls(df: DataFrame, required_cols: list[str]) -> DataFrame:
    """
    Elimina filas donde alguno de los campos requeridos es nulo.
    Loguea cuántas filas se eliminaron (sin acción costosa de count en producción).
    """
    return df.dropna(subset=required_cols)


# ---------------------------------------------------------------------------
# 2. Validación de rangos
# ---------------------------------------------------------------------------

def validate_trades(df: DataFrame) -> DataFrame:
    """
    Filtra trades con precio o cantidad fuera de rango válido.
    También filtra trade_time absurdos (< año 2020 o > año 2030).
    """
    ts_min = 1_577_836_800_000   # 2020-01-01 en ms
    ts_max = 1_893_456_000_000   # 2030-01-01 en ms

    return df.filter(
        (F.col("price").isNotNull()) &
        (F.col("quantity").isNotNull()) &
        (F.col("trade_time").isNotNull()) &
        (F.col("price") >= PRICE_MIN) &
        (F.col("price") <= PRICE_MAX) &
        (F.col("quantity") >= QTY_MIN) &
        (F.col("quantity") <= QTY_MAX) &
        (F.col("trade_time") >= ts_min) &
        (F.col("trade_time") <= ts_max)
    )


def validate_tickers(df: DataFrame) -> DataFrame:
    """
    Filtra book tickers con spread negativo o bid/ask fuera de rango.
    Un spread negativo indica error de datos (ask < bid).
    También filtra tickers con event_time=0 (timestamp inválido).
    """
    ts_min = 1_577_836_800_000   # 2020-01-01 en ms
    ts_max = 1_893_456_000_000   # 2030-01-01 en ms
    return df.filter(
        (F.col("best_bid_price").isNotNull()) &
        (F.col("best_ask_price").isNotNull()) &
        (F.col("event_time").isNotNull()) &
        (F.col("best_bid_price") >= PRICE_MIN) &
        (F.col("best_ask_price") >= PRICE_MIN) &
        (F.col("best_ask_price") >= F.col("best_bid_price")) &
        (F.col("event_time") >= ts_min) &
        (F.col("event_time") <= ts_max)
    )


# ---------------------------------------------------------------------------
# 3. Cast de tipos
# ---------------------------------------------------------------------------

def cast_trade_types(df: DataFrame) -> DataFrame:
    """
    Garantiza los tipos correctos en raw_trades tras leer de Cassandra.
    Cassandra puede devolver decimales como strings dependiendo del conector.
    """
    return (
        df
        .withColumn("price",        F.col("price").cast(DoubleType()))
        .withColumn("quantity",     F.col("quantity").cast(DoubleType()))
        .withColumn("trade_time",   F.col("trade_time").cast(LongType()))
        .withColumn("event_time",   F.col("event_time").cast(LongType()))
        .withColumn("agg_trade_id", F.col("agg_trade_id").cast(LongType()))
        .withColumn("is_buyer_maker", F.col("is_buyer_maker").cast(BooleanType()))
        .withColumn("symbol", F.upper(F.col("symbol")))
    )


def cast_ticker_types(df: DataFrame) -> DataFrame:
    """Tipos correctos en raw_book_tickers."""
    return (
        df
        .transform(_normalize_event_time)
        .withColumn("best_bid_price", F.col("best_bid_price").cast(DoubleType()))
        .withColumn("best_ask_price", F.col("best_ask_price").cast(DoubleType()))
        .withColumn("best_bid_qty",   F.col("best_bid_qty").cast(DoubleType()))
        .withColumn("best_ask_qty",   F.col("best_ask_qty").cast(DoubleType()))
        .withColumn("event_time",     F.col("event_time").cast(LongType()))
        .withColumn("symbol", F.upper(F.col("symbol")))
    )


# ---------------------------------------------------------------------------
# 4. Columnas derivadas
# ---------------------------------------------------------------------------

def add_trade_derived_cols(df: DataFrame) -> DataFrame:
    """
    Agrega columnas calculadas útiles para agregaciones posteriores.

      trade_datetime  → timestamp legible desde trade_time (ms → TimestampType)
      price_qty       → price × quantity, necesario para VWAP
      log_price       → ln(price), para calcular log_return en la Feature Engine
      side            → 'buy' / 'sell' desde is_buyer_maker
                        (is_buyer_maker=True significa que el comprador es el maker
                         → orden pasiva → el agressor es el vendedor)
    """
    return (
        df
        .withColumn(
            "trade_datetime",
            F.to_timestamp((F.col("trade_time") / 1000).cast("double"))
        )
        .withColumn("price_qty", F.col("price") * F.col("quantity"))
        .withColumn(
            "log_price",
            F.when(F.col("price") > 0, F.log(F.col("price")))
        )
        .withColumn(
            "side",
            F.when(F.col("is_buyer_maker"), F.lit("sell")).otherwise(F.lit("buy"))
        )
    )


def add_ticker_derived_cols(df: DataFrame) -> DataFrame:
    """
    Agrega columnas derivadas al book ticker.

      event_datetime  → timestamp legible
      spread          → recalculado (puede diferir del almacenado si hubo error)
      mid_price       → (bid + ask) / 2, útil para análisis de microestructura
    """
    return (
        df
        .withColumn(
            "event_datetime",
            F.to_timestamp((F.col("event_time") / 1000).cast("double"))
        )
        .withColumn(
            "spread",
            F.col("best_ask_price") - F.col("best_bid_price")
        )
        .withColumn(
            "mid_price",
            (F.col("best_bid_price") + F.col("best_ask_price")) / 2
        )
    )


# ---------------------------------------------------------------------------
# 5. Deduplicación
# ---------------------------------------------------------------------------

def deduplicate_trades(df: DataFrame) -> DataFrame:
    """
    Elimina trades duplicados.

    Estrategia: deduplica por (symbol, agg_trade_id).
    agg_trade_id es el identificador único de Binance para trades agregados.
    En caso de duplicado, conserva la fila con menor ingestion_ts
    (la primera vez que fue ingestada).

    No se usa dropDuplicates() directamente porque conservaría una fila
    arbitraria; aquí queremos control explícito.
    """
    from pyspark.sql.window import Window

    df = _ensure_ingestion_ts(df)

    window = Window.partitionBy("symbol", "agg_trade_id").orderBy("ingestion_ts")

    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def deduplicate_tickers(df: DataFrame) -> DataFrame:
    """
    Elimina book tickers duplicados.

    Estrategia: deduplica por (symbol, event_time).
    Dos eventos con el mismo symbol+event_time son el mismo snapshot de bid/ask.
    """
    from pyspark.sql.window import Window

    df = _ensure_ingestion_ts(df)

    window = Window.partitionBy("symbol", "event_time").orderBy("ingestion_ts")

    return (
        df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Pipelines completos (convenencia)
# ---------------------------------------------------------------------------

def clean_trades(df: DataFrame) -> DataFrame:
    """
    Aplica el pipeline completo de limpieza sobre raw_trades.
    Orden importante: drop_nulls antes de validate_ranges para evitar
    comparaciones con nulos que generan falsos positivos.
    """
    return (
        df
        .transform(cast_trade_types)
        .transform(lambda d: drop_nulls(d, TRADE_REQUIRED_COLS))
        .transform(validate_trades)
        .transform(add_trade_derived_cols)
        .transform(deduplicate_trades)
    )


def clean_tickers(df: DataFrame) -> DataFrame:
    """Pipeline completo de limpieza sobre raw_book_tickers."""
    return (
        df
        .transform(cast_ticker_types)
        .transform(lambda d: drop_nulls(d, TICKER_REQUIRED_COLS))
        .transform(validate_tickers)
        .transform(add_ticker_derived_cols)
        .transform(deduplicate_tickers)
    )