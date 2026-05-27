"""
analytics/queries_cointegration.py
-----------------------------------
Queries analíticas sobre la tabla `cryptoflow.pairs_zscore_1m`.

Estas queries complementan las de `analytics/queries.py` con la primera
vista cross-asset del proyecto: divergencias extremas de cointegración
entre pares cripto.

Hipótesis principal
───────────────────
Los pares mejor cointegrados (típicamente BTC-ETH, los dos activos con
mayor capitalización y flujos de orden más correlacionados) deberían
exhibir MENOS eventos de divergencia extrema (|z_score| > umbral) que
pares menos cointegrados como BTC-BNB. Si la hipótesis se confirma en
los datos, el z-score sirve como señal accionable de stat-arb.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# Q9 · Divergencias extremas de cointegración por par
# ═══════════════════════════════════════════════════════════════════════════

def q9_cointegration_extreme_divergences(
    df_zscore: DataFrame,
    abs_threshold: float = 2.0,
) -> DataFrame:
    """
    Resumen por par de los eventos donde |z_score| supera `abs_threshold`.

    Para cada `(sym_dom, sym_hedge)` reporta:
      - n_events                : número de filas con |z_score| > abs_threshold
      - max_abs_z               : máximo |z_score| observado en eventos
      - avg_spread_pct_in_events: spread promedio (%) durante esos eventos
      - longest_streak_windows  : racha más larga de ventanas consecutivas
                                  en estado de divergencia extrema

    Interpretación
    ──────────────
    Pares más cointegrados deberían tener `n_events` y
    `longest_streak_windows` menores. Si un par exhibe rachas largas, el
    spread tarda en re-convergir → señal débil/ruidosa para reversión.

    Notas
    ─────
    - El cálculo del streak agrupa filas consecutivas marcadas como
      "extreme" usando la técnica clásica de `sum(change_flag)` sobre la
      ventana ordenada: cada vez que cambia el estado se incrementa el
      identificador de grupo. Solo se cuentan los grupos en estado
      extremo.
    - Se asume que `df_zscore` viene ordenado o, al menos, contiene la
      columna `window_start` para definir el orden temporal. La función
      no requiere un orden previo del DataFrame.
    """
    df = df_zscore.filter(F.col("z_score").isNotNull())

    extreme_flag = (F.abs(F.col("z_score")) > F.lit(abs_threshold)).cast("int")

    w_order = Window.partitionBy("sym_dom", "sym_hedge").orderBy("window_start")
    w_running = w_order.rowsBetween(Window.unboundedPreceding, 0)

    df_marked = (
        df
        .withColumn("is_extreme", extreme_flag)
        .withColumn(
            "state_change",
            (F.col("is_extreme") != F.coalesce(F.lag("is_extreme").over(w_order), F.lit(-1)))
            .cast("int"),
        )
        .withColumn("state_group", F.sum("state_change").over(w_running))
    )

    # Longitud de cada bloque consecutivo (solo conservamos los que son extremos).
    streak_lengths = (
        df_marked
        .filter(F.col("is_extreme") == 1)
        .groupBy("sym_dom", "sym_hedge", "state_group")
        .agg(F.count("*").alias("streak_len"))
    )

    longest_streak = streak_lengths.groupBy("sym_dom", "sym_hedge").agg(
        F.max("streak_len").alias("longest_streak_windows")
    )

    summary = (
        df_marked
        .filter(F.col("is_extreme") == 1)
        .groupBy("sym_dom", "sym_hedge")
        .agg(
            F.count("*").alias("n_events"),
            F.round(F.max(F.abs(F.col("z_score"))), 4).alias("max_abs_z"),
            F.round(F.avg("spread_pct"), 6).alias("avg_spread_pct_in_events"),
        )
    )

    return (
        summary
        .join(longest_streak, on=["sym_dom", "sym_hedge"], how="left")
        .select(
            "sym_dom",
            "sym_hedge",
            "n_events",
            "max_abs_z",
            "avg_spread_pct_in_events",
            "longest_streak_windows",
        )
        .orderBy("sym_dom", "sym_hedge")
    )
