"""
feature_engine/cointegration.py
--------------------------------
Cointegración rolling z-score entre pares de criptomonedas.

Motivación
──────────
Todas las features existentes en `feature_engine/features.py` se calculan
**por símbolo individual** (VWAP, log_return, OBI, etc.). Este módulo
introduce la primera feature **cross-asset**: el z-score del spread de
cointegración entre pares (BTC-ETH, BTC-BNB, ETH-BNB).

La cointegración (Engle & Granger, 1987) identifica combinaciones lineales
de series no estacionarias cuyo residuo sí lo es. En cripto, los pares
mayores tienden a co-moverse en régimen normal y divergen en eventos
idiosincráticos — el z-score del spread captura esa divergencia.

Implementación
──────────────
La lógica sigue el método `get_spread_and_z_score` del controlador
`stat_arb.py` del proyecto Hummingbot
(https://github.com/hummingbot/hummingbot/blob/master/controllers/generic/stat_arb.py,
Apache 2.0, líneas ~382-402), adaptado a Spark con OLS closed-form y
ventanas rolling.

Referencias
───────────
- Engle, R. F., & Granger, C. W. J. (1987). "Co-integration and Error
  Correction: Representation, Estimation, and Testing". Econometrica,
  55(2), 251-276.
- Avellaneda, M., & Lee, J.-H. (2010). "Statistical Arbitrage in the U.S.
  Equities Market". Quantitative Finance, 10(7).
- Hummingbot stat_arb controller (Apache 2.0):
  https://github.com/hummingbot/hummingbot/blob/master/controllers/generic/stat_arb.py
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


PAIRS: list[tuple[str, str]] = [
    ("BTCUSDT", "ETHUSDT"),
    ("BTCUSDT", "BNBUSDT"),
    ("ETHUSDT", "BNBUSDT"),
]


def compute_pairwise_zscore(
    spark: SparkSession,
    df_ohlcv_1m: DataFrame,
    pairs: list[tuple[str, str]] = PAIRS,
    lookback: int = 300,
) -> DataFrame:
    """
    Calcula el z-score rolling del spread de cointegración para cada par.

    Pasos
    ─────
    1. Pivota `ohlcv_1m` a wide format: una columna por símbolo con `close`.
    2. Calcula retornos simples por símbolo y los acumula en log-space
       (`log1p` + `cumsum` + `exp`) para estabilidad numérica frente
       al producto directo.
    3. Para cada par (dom, hedge) en una ventana rolling de `lookback`
       velas 1m:
         - OLS closed-form:
               beta  = (n·Σxy − Σx·Σy) / (n·Σxx − (Σx)²)
               alpha = (Σy − β·Σx) / n
         - spread_pct = (cum_hedge − (alpha + beta·cum_dom))
                        / (alpha + beta·cum_dom) · 100
         - z_score    = (spread_pct − mean(spread_pct))
                        / stddev_pop(spread_pct)
       todo sobre la misma ventana rolling.
    4. Solo se emiten filas cuando hay `lookback` puntos completos en la
       ventana.

    CRÍTICO — bug de look-ahead a evitar
    ────────────────────────────────────
    La ventana debe ser `rowsBetween(-(lookback-1), 0)`. Si se usa
    `rowsBetween(0, lookback-1)` el modelo "ve el futuro" y el z-score
    se convierte en un oráculo. El test `test_shock_injection_no_lookahead`
    en `tests/test_cointegration.py` caza exactamente este bug.

    Args
    ────
    spark        : SparkSession activa (parámetro reservado para
                   transformaciones que requieran broadcast u otros
                   recursos del cluster).
    df_ohlcv_1m  : DataFrame con columnas `symbol`, `window_start`, `close`
                   tal como sale de `cryptoflow.ohlcv_1m`.
    pairs        : Lista de tuplas (sym_dom, sym_hedge). Default `PAIRS`.
    lookback     : Número de velas 1m en la ventana rolling. Default 300
                   (5 horas a granularidad 1m).

    Returns
    ───────
    DataFrame con esquema:
        window_start  : timestamp
        sym_dom       : string
        sym_hedge     : string
        lookback      : int
        alpha         : double
        beta          : double
        spread_pct    : double
        z_score       : double

    Notas de estabilidad numérica
    ─────────────────────────────
    - Se usa `log1p` + `sum` + `exp` para los cum-returns. El producto
      directo con `F.product`/multiplicación recursiva acumula error de
      punto flotante mucho más rápido.
    - Se usa `stddev_pop` (no `stddev`) para evitar la corrección de
      Bessel sobre ventanas pequeñas, donde `stddev` introduce un sesgo
      adicional al usar `n-1` en lugar de `n`.
    """
    symbols_needed = sorted({s for pair in pairs for s in pair})

    wide = (
        df_ohlcv_1m
        .filter(F.col("symbol").isin(symbols_needed))
        .groupBy("window_start")
        .pivot("symbol", symbols_needed)
        .agg(F.first("close"))
    )

    results: list[DataFrame] = []
    w_ts = Window.orderBy("window_start")
    w_roll = Window.orderBy("window_start").rowsBetween(-(lookback - 1), 0)

    for sym_dom, sym_hedge in pairs:
        df = (
            wide
            .withColumn(
                "r_dom",
                F.col(sym_dom) / F.lag(sym_dom).over(w_ts) - F.lit(1.0),
            )
            .withColumn(
                "r_hedge",
                F.col(sym_hedge) / F.lag(sym_hedge).over(w_ts) - F.lit(1.0),
            )
            .filter(F.col("r_dom").isNotNull() & F.col("r_hedge").isNotNull())
            .withColumn("log1p_dom",   F.log1p(F.col("r_dom")))
            .withColumn("log1p_hedge", F.log1p(F.col("r_hedge")))
            .withColumn("cum_dom",   F.exp(F.sum("log1p_dom").over(w_ts)))
            .withColumn("cum_hedge", F.exp(F.sum("log1p_hedge").over(w_ts)))
        )

        df = (
            df
            .withColumn("n",   F.count("cum_dom").over(w_roll))
            .withColumn("sx",  F.sum("cum_dom").over(w_roll))
            .withColumn("sy",  F.sum("cum_hedge").over(w_roll))
            .withColumn(
                "sxx",
                F.sum(F.col("cum_dom") * F.col("cum_dom")).over(w_roll),
            )
            .withColumn(
                "sxy",
                F.sum(F.col("cum_dom") * F.col("cum_hedge")).over(w_roll),
            )
            .withColumn(
                "beta",
                (F.col("n") * F.col("sxy") - F.col("sx") * F.col("sy"))
                / (F.col("n") * F.col("sxx") - F.col("sx") * F.col("sx")),
            )
            .withColumn(
                "alpha",
                (F.col("sy") - F.col("beta") * F.col("sx")) / F.col("n"),
            )
            .filter(F.col("n") >= F.lit(lookback))
        )

        df = (
            df
            .withColumn(
                "y_pred",
                F.col("alpha") + F.col("beta") * F.col("cum_dom"),
            )
            .withColumn(
                "spread_pct",
                (F.col("cum_hedge") - F.col("y_pred")) / F.col("y_pred") * F.lit(100.0),
            )
            .withColumn("mu_s", F.avg("spread_pct").over(w_roll))
            .withColumn("sd_s", F.stddev_pop("spread_pct").over(w_roll))
            .withColumn(
                "z_score",
                (F.col("spread_pct") - F.col("mu_s")) / F.col("sd_s"),
            )
            .withColumn("sym_dom",   F.lit(sym_dom))
            .withColumn("sym_hedge", F.lit(sym_hedge))
            .withColumn("lookback",  F.lit(lookback))
            .select(
                "window_start",
                "sym_dom",
                "sym_hedge",
                "lookback",
                "alpha",
                "beta",
                "spread_pct",
                "z_score",
            )
        )
        results.append(df)

    return reduce(lambda a, b: a.unionByName(b), results)
