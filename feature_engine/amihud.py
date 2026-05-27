"""
feature_engine/amihud.py
-------------------------
Amihud (2002) illiquidity ratio sobre features_by_window.

Referencia
──────────
Amihud, Y. (2002). *Illiquidity and stock returns: cross-section and
time-series effects*. Journal of Financial Markets 5(1), 31-56.
DOI: 10.1016/S1386-4181(01)00024-6.

Qué mide
────────
El **price impact realizado**: cuánto se mueve el precio por dólar de
volumen ejecutado. Es distinto al spread cotizado (costo cotizado en el
libro) y al OBI (presión latente). El Amihud captura el costo *realizado*
de consumir liquidez en el mercado.

Fórmula original (Amihud 2002, eq. 1):

    ILLIQ_t = mean_{t-N+1..t} ( |log_return| / volume_usd )

Donde `volume_usd` es el monto operado en dólares en la ventana. En este
pipeline usamos `price_qty_sum = Σ(price * qty)` por ventana, ya en USD,
calculado por `processing.aggregator.compute_ohlcv()`. Si esa columna no
está disponible (p. ej. al leer directamente de la tabla persistida
`features_by_window` cuyo schema en `migration_v2.cql` no incluye
`price_qty_sum`), se cae al proxy `close * volume`, que aproxima el
volume_usd asumiendo precio promedio ≈ precio de cierre.

Unidad final
────────────
Para que el ratio sea legible humanamente se escala por `1e10`, lo que lo
expresa aproximadamente como **basis points por millón USD** operado.
Valores típicos para BTC suelen estar < 1; valores >> 1 indican alto
price impact (poca profundidad).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


_AMIHUD_SCALE = 1e10  # bp por millón USD (para legibilidad humana)


def add_amihud_illiq(df: DataFrame, periods: int = 60) -> DataFrame:
    """
    Agrega columna `amihud_illiq` al DataFrame de features.

    Calcula el Amihud illiquidity ratio (Amihud 2002, JFM 5(1)) sobre una
    ventana deslizante de `periods` ventanas, partitionando por símbolo
    y ordenando por window_start.

    Args:
        df:      DataFrame con columnas `symbol`, `window_start`,
                 `log_return` y `price_qty_sum`. Si `price_qty_sum` no
                 existe, se usa `close * volume` como proxy de volume_usd.
        periods: Número de ventanas en el promedio rolling (default: 60).
                 Con OHLCV 1m esto corresponde a una hora de mercado.

    Returns:
        DataFrame con columna nueva `amihud_illiq`, escalada por 1e10
        (bp por millón USD) y redondeada a 6 decimales. Sin tabla nueva.

    Notas
    ─────
    * Defensivo frente a `volume_usd = 0` o nulo: el ratio instantáneo
      es null en esa ventana y se excluye del promedio rolling.
    * Las primeras `periods - 1` filas por símbolo tienen menos datos
      disponibles; Spark calcula la media con los datos parciales (no
      devuelve NaN). Si se desea exigir ventana completa, filtrar a
      posteriori por `row_number() >= periods`.
    """
    if "price_qty_sum" in df.columns:
        volume_usd = F.col("price_qty_sum")
    else:
        # Fallback: proxy USD-volume con close * volume cuando se lee
        # directamente de la tabla persistida sin price_qty_sum.
        volume_usd = F.col("close") * F.col("volume")

    illiq_inst = F.when(
        volume_usd > 0,
        F.abs(F.col("log_return")) / volume_usd,
    ).otherwise(F.lit(None))

    w_roll = (
        Window
        .partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-(periods - 1), 0)
    )

    return (
        df
        .withColumn("_illiq_inst", illiq_inst)
        .withColumn(
            "amihud_illiq",
            F.round(F.avg("_illiq_inst").over(w_roll) * F.lit(_AMIHUD_SCALE), 6),
        )
        .drop("_illiq_inst")
    )
