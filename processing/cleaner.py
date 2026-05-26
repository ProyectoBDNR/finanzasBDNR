"""
processing/cleaner.py
----------------------
Limpieza, deduplicación y normalización de eventos raw de Cassandra.

¿Qué limpiamos y por qué?
--------------------------
La fuente de datos (Binance WebSocket) ES confiable — es un exchange
institucional con uptime > 99.9%. Lo que limpiamos son anomalías del
PIPELINE, no de la fuente:

1. DEDUPLICACIÓN — el WebSocket puede re-emitir el mismo evento si hay
   una reconexión. El consumer inserta el evento cada vez que lo recibe,
   sin saber si ya existe en Cassandra. El resultado: un mismo trade
   (identificado por agg_trade_id) puede aparecer 2-3 veces en raw_trades.
   → Solucion: conservamos solo la primera ingesta (menor ingestion_ts).

2. TIMESTAMPS INVÁLIDOS — event_time=0 ocurría cuando Binance no incluía
   el campo 'E' en el payload bookTicker. Ya corregido en el consumer
   con fallback al reloj del sistema, pero el filtro por rango 2020-2030
   es defensivo para cualquier corrupción futura de timestamps.

3. PRECIOS/CANTIDADES NULOS — el parser puede producir None si el campo
   llega malformado en el JSON. dropna elimina estas filas antes de
   hacer operaciones numéricas.

4. SPREAD NEGATIVO — ask < bid es físicamente imposible en un mercado
   funcional. Indica un error de parsing o un tick corrupto.
   Se filtra antes de calcular el spread timeseries.

5. NORMALIZACIÓN DE TIPOS — el conector Cassandra-Spark puede devolver
   ciertos campos como StringType dependiendo de la versión. cast_types
   garantiza los tipos correctos para las operaciones de Spark.

Pipeline aplicado en orden (el orden importa):
    1. drop_nulls       → elimina filas con campos críticos nulos
    2. cast_types       → garantiza tipos numéricos correctos
    3. validate_ranges  → filtra valores fuera de rango válido
    4. add_derived      → columnas calculadas (log_price, side, mid_price)
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

# Rangos intencionalmente amplios — no filtramos eventos extremos legítimos.
# El objetivo es eliminar errores de parsing, no outliers de mercado.
PRICE_MIN = 0.000_001    # cubre monedas de muy bajo valor (shitcoins)
PRICE_MAX = 10_000_000   # por encima de cualquier precio histórico de BTC
QTY_MIN   = 0.000_000_1
QTY_MAX   = 1_000_000

# Rango válido de timestamps en millisegundos
TS_MIN = 1_577_836_800_000   # 2020-01-01 en ms
TS_MAX = 1_893_456_000_000   # 2030-01-01 en ms

# Campos que deben estar presentes (no nulos) en cada evento
TRADE_REQUIRED_COLS  = ["symbol", "price", "quantity", "trade_time", "trace_id"]
TICKER_REQUIRED_COLS = ["symbol", "best_bid_price", "best_ask_price", "event_time", "trace_id"]


# ---------------------------------------------------------------------------
# 1. Drop nulls
# ---------------------------------------------------------------------------

def drop_nulls(df: DataFrame, required_cols: list[str]) -> DataFrame:
    """Elimina filas donde algún campo crítico es nulo."""
    return df.dropna(subset=required_cols)


# ---------------------------------------------------------------------------
# 2. Cast de tipos
# ---------------------------------------------------------------------------

def cast_trade_types(df: DataFrame) -> DataFrame:
    """Garantiza tipos correctos en raw_trades tras leer de Cassandra."""
    return (
        df
        .withColumn("price",          F.col("price").cast(DoubleType()))
        .withColumn("quantity",       F.col("quantity").cast(DoubleType()))
        .withColumn("trade_time",     F.col("trade_time").cast(LongType()))
        .withColumn("event_time",     F.col("event_time").cast(LongType()))
        .withColumn("agg_trade_id",   F.col("agg_trade_id").cast(LongType()))
        .withColumn("is_buyer_maker", F.col("is_buyer_maker").cast(BooleanType()))
        .withColumn("symbol",         F.upper(F.col("symbol")))
    )


def cast_ticker_types(df: DataFrame) -> DataFrame:
    """Garantiza tipos correctos en raw_book_tickers."""
    return (
        df
        .withColumn("best_bid_price", F.col("best_bid_price").cast(DoubleType()))
        .withColumn("best_ask_price", F.col("best_ask_price").cast(DoubleType()))
        .withColumn("best_bid_qty",   F.col("best_bid_qty").cast(DoubleType()))
        .withColumn("best_ask_qty",   F.col("best_ask_qty").cast(DoubleType()))
        .withColumn("event_time",     F.col("event_time").cast(LongType()))
        .withColumn("symbol",         F.upper(F.col("symbol")))
    )


# ---------------------------------------------------------------------------
# 3. Validación de rangos
# ---------------------------------------------------------------------------

def validate_trades(df: DataFrame) -> DataFrame:
    """
    Filtra trades con valores fuera de rango válido.
    Un trade con price=0 o trade_time=0 es un error de parsing,
    no un evento legítimo del mercado.
    """
    return df.filter(
        (F.col("price")      >= PRICE_MIN) &
        (F.col("price")      <= PRICE_MAX) &
        (F.col("quantity")   >= QTY_MIN)   &
        (F.col("quantity")   <= QTY_MAX)   &
        (F.col("trade_time") >= TS_MIN)    &
        (F.col("trade_time") <= TS_MAX)
    )


def validate_tickers(df: DataFrame) -> DataFrame:
    """
    Filtra book tickers inválidos.
    Spread negativo (ask < bid) y event_time=0 son errores de pipeline,
    no condiciones de mercado.
    """
    return df.filter(
        (F.col("best_bid_price") >= PRICE_MIN) &
        (F.col("best_ask_price") >= PRICE_MIN) &
        (F.col("best_ask_price") >= F.col("best_bid_price")) &  # no spread negativo
        (F.col("event_time")     >= TS_MIN) &
        (F.col("event_time")     <= TS_MAX)
    )


# ---------------------------------------------------------------------------
# 4. Columnas derivadas
# ---------------------------------------------------------------------------

def add_trade_derived_cols(df: DataFrame) -> DataFrame:
    """
    Agrega columnas calculadas necesarias para las agregaciones posteriores.

    trade_datetime : timestamp legible (ms → Timestamp) para windowing
    price_qty      : price × quantity, necesario para VWAP = Σ(pq) / Σ(q)
    log_price      : ln(price), para calcular log_return en el Feature Engine
    side           : 'buy' o 'sell' desde is_buyer_maker.
                     is_buyer_maker=True → el comprador puso la orden límite
                     (maker), por lo tanto el agresor (vendedor) ejecutó
                     contra ella → el trade fue iniciado por un vendedor.
    """
    return (
        df
        .withColumn("trade_datetime",
                    F.to_timestamp(F.col("trade_time") / 1000))
        .withColumn("price_qty",
                    F.col("price") * F.col("quantity"))
        .withColumn("log_price",
                    F.log(F.col("price")))
        .withColumn("side",
                    F.when(F.col("is_buyer_maker"), F.lit("sell"))
                     .otherwise(F.lit("buy")))
    )


def add_ticker_derived_cols(df: DataFrame) -> DataFrame:
    """
    Agrega columnas derivadas al book ticker.

    event_datetime : timestamp legible para joins temporales
    spread         : recalculado desde bid/ask (más preciso que el almacenado)
    mid_price      : (bid + ask) / 2, referencia de precio sin sesgo direccional
    """
    return (
        df
        .withColumn("event_datetime",
                    F.to_timestamp(F.col("event_time") / 1000))
        .withColumn("spread",
                    F.col("best_ask_price") - F.col("best_bid_price"))
        .withColumn("mid_price",
                    (F.col("best_bid_price") + F.col("best_ask_price")) / 2)
    )


# ---------------------------------------------------------------------------
# 5. Deduplicación
# ---------------------------------------------------------------------------

def deduplicate_trades(df: DataFrame) -> DataFrame:
    """
    Elimina trades duplicados por (symbol, agg_trade_id).

    agg_trade_id es el ID único de Binance para un trade agregado.
    Si el consumer se reconectó, el mismo trade puede estar insertado
    múltiples veces en raw_trades. Conservamos la primera inserción
    (menor ingestion_ts) para mantener la trazabilidad original.
    """
    from pyspark.sql.window import Window

    w = Window.partitionBy("symbol", "agg_trade_id").orderBy("ingestion_ts")
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def deduplicate_tickers(df: DataFrame) -> DataFrame:
    """
    Elimina book tickers duplicados por (symbol, event_time).

    El mismo snapshot bid/ask puede llegar dos veces si el consumer
    reconectó y Binance re-emitió el último estado del order book.
    """
    from pyspark.sql.window import Window

    w = Window.partitionBy("symbol", "event_time").orderBy("ingestion_ts")
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Pipelines públicos
# ---------------------------------------------------------------------------

def clean_trades(df: DataFrame) -> DataFrame:
    """
    Pipeline completo de limpieza para raw_trades.
    El orden es crítico: drop_nulls antes de validate para evitar
    comparaciones numéricas contra None que producen falsos positivos.
    """
    return (
        df
        .transform(lambda d: drop_nulls(d, TRADE_REQUIRED_COLS))
        .transform(cast_trade_types)
        .transform(validate_trades)
        .transform(add_trade_derived_cols)
        .transform(deduplicate_trades)
    )


def clean_tickers(df: DataFrame) -> DataFrame:
    """Pipeline completo de limpieza para raw_book_tickers."""
    return (
        df
        .transform(lambda d: drop_nulls(d, TICKER_REQUIRED_COLS))
        .transform(cast_ticker_types)
        .transform(validate_tickers)
        .transform(add_ticker_derived_cols)
        .transform(deduplicate_tickers)
    )