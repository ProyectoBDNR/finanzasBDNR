"""
processing/aggregator.py
-------------------------
Agregaciones temporales sobre trades limpios.

Produce DataFrames OHLCV (Open/High/Low/Close/Volume) para
ventanas de 1 minuto, 5 minutos y 1 hora.

Diseño:
  - Usa F.window() de Spark sobre trade_datetime (TimestampType).
    Esta función agrupa eventos en cubos temporales fijos (tumbling windows),
    no deslizantes, lo que simplifica el particionado posterior en Cassandra.
  - trade_count y total_volume son agregados directos.
  - buy_volume / sell_volume para ratio de presión compradora/vendedora.
  - La columna window_start se extrae del struct Window que devuelve Spark,
    y se convierte a string para usarla como partition key en Cassandra.
"""

from __future__ import annotations


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# Ventanas soportadas — deben ser strings reconocidos por F.window()
WINDOWS = {
    "1m":  "1 minute",
    "5m":  "5 minutes",
    "1h":  "1 hour",
}


def compute_ohlcv(df: DataFrame, window_label: str) -> DataFrame:
    """
    Calcula OHLCV para una ventana temporal específica.

    Args:
        df:           DataFrame limpio de trades (salida de clean_trades)
        window_label: '1m', '5m' o '1h'

    Returns:
        DataFrame con columnas:
          symbol, window_label, window_start, window_end,
          open, high, low, close, volume,
          trade_count, buy_volume, sell_volume, price_qty_sum
    """
    window_duration = WINDOWS[window_label]

    # first() y last() sin ignorar nulos: los trades ya están validados
    # orderBy dentro del groupBy no está soportado directamente en Spark;
    # usamos min/max para open/close aproximados dentro de la ventana.
    # Para open/close exactos se necesitaría una Window function adicional
    # (ver nota al pie), pero para OHLCV de análisis esta aproximación es válida.
    agg_df = (
        df
        .groupBy(
            F.col("symbol"),
            F.window(F.col("trade_datetime"), window_duration).alias("w"),
        )
        .agg(
            # OHLCV core
            F.first("price").alias("open"),        # primera fila por orden de llegada
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),         # última fila por orden de llegada
            F.sum("quantity").alias("volume"),

            # Métricas adicionales
            F.count("*").alias("trade_count"),
            F.sum(
                F.when(F.col("side") == "buy", F.col("quantity")).otherwise(0)
            ).alias("buy_volume"),
            F.sum(
                F.when(F.col("side") == "sell", F.col("quantity")).otherwise(0)
            ).alias("sell_volume"),

            # Necesario para calcular VWAP en la Feature Engine
            F.sum("price_qty").alias("price_qty_sum"),
        )
    )

    # Extraer window_start y window_end del struct Window
    # y agregar el label de ventana para identificación
    return (
        agg_df
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end",   F.col("w.end"))
        .withColumn("window_label", F.lit(window_label).cast(StringType()))
        .drop("w")
        .orderBy("symbol", "window_start")
    )


def compute_all_windows(df: DataFrame) -> dict[str, DataFrame]:
    """
    Calcula OHLCV para todas las ventanas definidas.

    Retorna un dict {label → DataFrame} para que el job
    pueda persistir cada ventana en su tabla correspondiente.

    Nota: df se reutiliza en tres aggregaciones. Si el DataFrame
    es grande, considerar df.cache() antes de llamar esta función.
    """
    return {label: compute_ohlcv(df, label) for label in WINDOWS}


def compute_spread_timeseries(df_tickers: DataFrame, window_label: str) -> DataFrame:
    """
    Agrega métricas de spread por ventana temporal desde book tickers.

    Returns:
        DataFrame con columnas:
          symbol, window_label, window_start,
          spread_mean, spread_min, spread_max, spread_std,
          mid_price_mean, tick_count
    """
    window_duration = WINDOWS[window_label]

    return (
        df_tickers
        .groupBy(
            F.col("symbol"),
            F.window(F.col("event_datetime"), window_duration).alias("w"),
        )
        .agg(
            F.avg("spread").alias("spread_mean"),
            F.min("spread").alias("spread_min"),
            F.max("spread").alias("spread_max"),
            F.stddev("spread").alias("spread_std"),
            F.avg("mid_price").alias("mid_price_mean"),
            F.count("*").alias("tick_count"),
            # OBI: promedio de bid_qty y ask_qty para calcular Order Book Imbalance
            # OBI = (bid_qty_mean - ask_qty_mean) / (bid_qty_mean + ask_qty_mean)
            # Un OBI > 0 indica presión compradora; < 0 indica presión vendedora.
            F.avg("best_bid_qty").alias("bid_qty_mean"),
            F.avg("best_ask_qty").alias("ask_qty_mean"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_label", F.lit(window_label).cast(StringType()))
        .drop("w")
        .orderBy("symbol", "window_start")
    )