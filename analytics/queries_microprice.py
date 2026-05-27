"""
analytics/queries_microprice.py
--------------------------------
Queries analíticas que dependen del Micro-Price (hftbacktest — obi_mm).

Se mantiene en un módulo aparte de ``analytics/queries.py`` para no tocar
las queries Q1–Q7 ya validadas. Las funciones aquí están pensadas para
correrse después de haber escrito ``cryptoflow.microprice_by_window``
mediante el job de :mod:`feature_engine.microprice`.

Referencia:
    https://github.com/nkaz001/hftbacktest
    Notebook "Market Making with Alpha - Order Book Imbalance",
    función ``obi_mm``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# Q8 · ¿La divergencia del Micro-Price predice el siguiente retorno?
# ═══════════════════════════════════════════════════════════════════════════

def q8_microprice_divergence_predicts_movement(
    df_features: DataFrame,
    df_microprice: DataFrame,
) -> DataFrame:
    """
    Evalúa si ``micro_price_div_mean_bps[t]`` predice ``log_return[t+1]``.

    Hipótesis
    ─────────
    El Micro-Price refleja el precio justo ponderado por la asimetría
    volumétrica del Nivel 1. Cuando ``micro_price > mid_price`` (divergencia
    positiva en bps) el libro tiene MENOS liquidez en asks que en bids,
    señal de presión compradora latente: el próximo trade tenderá a barrer
    asks → ``log_return[t+1]`` debería ser positivo en promedio.

    Procedimiento
    ─────────────
    1. Join por ``(symbol, window_label, window_start)`` entre ``features``
       (que tiene ``log_return``) y ``microprice`` (que tiene
       ``micro_price_div_mean_bps``).
    2. Para cada ventana, captura el ``log_return`` de la **siguiente**
       ventana usando ``F.lead`` sobre
       ``Window.partitionBy(symbol).orderBy(window_start)``.
    3. Bucketiza ``micro_price_div_mean_bps`` en quintiles por símbolo con
       ``F.ntile(5)`` — quintile 1 = divergencia más negativa, quintile 5 =
       divergencia más positiva.
    4. Agrega por ``(symbol, quintile_div)``:

       * ``avg_forward_return`` — promedio del retorno de la ventana
         siguiente.
       * ``hit_rate_pct``       — % de ventanas en que
         ``log_return[t+1] > 0``.
       * ``n_windows``          — tamaño de la celda (para evaluar
         significancia).

    Lectura esperada
    ────────────────
    Si el Micro-Price tiene poder predictivo:

    * ``avg_forward_return`` debería ser monotónicamente creciente con
      ``quintile_div`` (Q1 negativo → Q5 positivo).
    * ``hit_rate_pct`` para el quintile 5 debería estar significativamente
      por encima de 50% (típicamente 52–56% en cripto HF). Un hit rate
      cercano a 50% indica que el mercado ya descontó la señal.

    Args:
        df_features:   DataFrame de ``cryptoflow.features_by_window`` con,
            como mínimo, ``symbol``, ``window_label``, ``window_start``,
            ``log_return``.
        df_microprice: DataFrame de ``cryptoflow.microprice_by_window`` con
            ``symbol``, ``window_label``, ``window_start``,
            ``micro_price_div_mean_bps``.

    Returns:
        DataFrame con columnas:
        ``symbol``, ``quintile_div``, ``avg_forward_return``,
        ``hit_rate_pct``, ``n_windows``.
        Ordenado por ``(symbol, quintile_div)``.
    """
    joined = df_features.join(
        F.broadcast(
            df_microprice.select(
                "symbol",
                "window_label",
                "window_start",
                "micro_price_div_mean_bps",
            )
        ),
        on=["symbol", "window_label", "window_start"],
        how="inner",
    )

    w_sym = Window.partitionBy("symbol").orderBy("window_start")
    with_forward = (
        joined
        .withColumn("forward_log_return", F.lead("log_return", 1).over(w_sym))
        .filter(F.col("forward_log_return").isNotNull())
        .filter(F.col("micro_price_div_mean_bps").isNotNull())
    )

    w_quintile = Window.partitionBy("symbol").orderBy("micro_price_div_mean_bps")
    bucketed = with_forward.withColumn(
        "quintile_div", F.ntile(5).over(w_quintile)
    )

    return (
        bucketed
        .groupBy("symbol", "quintile_div")
        .agg(
            F.round(F.avg("forward_log_return"), 10).alias("avg_forward_return"),
            F.round(
                F.avg((F.col("forward_log_return") > 0).cast("double")) * F.lit(100.0),
                4,
            ).alias("hit_rate_pct"),
            F.count("*").alias("n_windows"),
        )
        .orderBy("symbol", "quintile_div")
    )
