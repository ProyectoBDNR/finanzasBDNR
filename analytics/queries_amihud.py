"""
analytics/queries_amihud.py
----------------------------
Q10 — Análisis cross-asset del Amihud illiquidity ratio por régimen de
volatilidad.

Referencia
──────────
Amihud, Y. (2002). *Illiquidity and stock returns: cross-section and
time-series effects*. Journal of Financial Markets 5(1), 31-56.

Hipótesis a defender
────────────────────
1. En régimen `HIGH` de volatilidad, el Amihud sube (menos liquidez por
   dólar): los market makers retiran cotizaciones y el price impact por
   dólar operado aumenta.
2. Ranking esperado de iliquidez (de menor a mayor):
       BTC < ETH < BNB
   BTC es el activo más profundo del top-3 por capitalización y volumen,
   y BNB el menos profundo de los tres en el mercado spot de Binance.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ═══════════════════════════════════════════════════════════════════════════
# Q10 · Amihud cross-asset por régimen de volatilidad
# ═══════════════════════════════════════════════════════════════════════════

def q10_amihud_cross_asset(df_features: DataFrame) -> DataFrame:
    """
    Reporta el Amihud illiquidity ratio por símbolo y régimen de
    volatilidad.

    Espera el DataFrame `features_by_window` con `amihud_illiq` ya
    calculado (ver `feature_engine.amihud.add_amihud_illiq`). Si la
    columna `vol_regime` no existe, se calcula inline usando p33/p66 de
    `rolling_volatility` por símbolo — mismo criterio que `queries.q1`.

    Returns
    -------
    DataFrame con columnas:
        symbol, vol_regime, illiq_mean, illiq_p50, illiq_p95, n_windows

    Interpretación esperada (hipótesis):
      * Para cada símbolo, illiq_mean(HIGH) > illiq_mean(LOW): la
        liquidez se contrae en regímenes volátiles.
      * Cross-asset, BNB > ETH > BTC en cada régimen: BTC es el más
        profundo.
    """
    df = df_features.filter(F.col("amihud_illiq").isNotNull())

    if "vol_regime" not in df.columns:
        # Mismo criterio que queries.q1_volatility_regime: percentiles por
        # símbolo para que el umbral se adapte a la escala de cada activo.
        pct_df = df.groupBy("symbol").agg(
            F.percentile_approx("rolling_volatility", 0.33).alias("p33"),
            F.percentile_approx("rolling_volatility", 0.66).alias("p66"),
        )
        df = df.join(F.broadcast(pct_df), on="symbol", how="left").withColumn(
            "vol_regime",
            F.when(F.col("rolling_volatility").isNull(), F.lit("UNKNOWN"))
             .when(F.col("rolling_volatility") <= F.col("p33"), F.lit("LOW"))
             .when(F.col("rolling_volatility") <= F.col("p66"), F.lit("MED"))
             .otherwise(F.lit("HIGH"))
        )

    return (
        df
        .groupBy("symbol", "vol_regime")
        .agg(
            F.round(F.avg("amihud_illiq"), 6).alias("illiq_mean"),
            F.round(F.percentile_approx("amihud_illiq", 0.50), 6).alias("illiq_p50"),
            F.round(F.percentile_approx("amihud_illiq", 0.95), 6).alias("illiq_p95"),
            F.count("*").alias("n_windows"),
        )
        .orderBy("symbol", "vol_regime")
    )
