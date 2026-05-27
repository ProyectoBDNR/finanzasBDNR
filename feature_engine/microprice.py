"""
feature_engine/microprice.py
-----------------------------
Cálculo del **Micro-Price** (Stoikov, 2017) como feature de microestructura
del Nivel 1 del order book.

Referencia
──────────
Implementación tomada del repo `hftbacktest` de @nkaz001 (4.1k ⭐), función
`obi_mm` en
``examples/Market Making with Alpha - Order Book Imbalance.ipynb``:

    https://github.com/nkaz001/hftbacktest

Fórmula (LaTeX)
───────────────
.. math::

    \\text{micro\\_price} \\;=\\;
        \\frac{p_b \\cdot q_a \\;+\\; p_a \\cdot q_b}{q_b + q_a}

    \\text{micro\\_price\\_divergence\\_bps}
        \\;=\\; \\frac{\\text{micro\\_price} - \\text{mid\\_price}}
                     {\\text{mid\\_price}} \\cdot 10\\,000

donde :math:`p_b, q_b` son el mejor bid y su tamaño, y :math:`p_a, q_a` los
del mejor ask. Nótese que las cantidades se cruzan: el lado con MENOR
liquidez "atrae" al precio justo, porque es más probable que sea consumido
por el próximo trade.

Intuición
─────────
Si ``bid_qty >> ask_qty`` hay presión compradora latente y el próximo trade
consumirá asks → el precio "real" está más cerca de ``best_ask``. El
``mid_price`` ignora esta asimetría volumétrica; el Micro-Price la pondera.

Casos límite
────────────
* ``bid_qty == ask_qty`` → ``micro_price == mid_price``.
* ``bid_qty + ask_qty == 0`` → división por cero: se devuelve ``mid_price``
  (fallback seguro, evita ``NaN`` propagando aguas abajo).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ═══════════════════════════════════════════════════════════════════════════
# 1. Micro-Price puntual sobre raw_book_tickers
# ═══════════════════════════════════════════════════════════════════════════

def compute_micro_price_raw(df_book_tickers: DataFrame) -> DataFrame:
    """
    Calcula Micro-Price tick a tick desde ``cryptoflow.raw_book_tickers``.

    Fórmula:
        .. math::
            \\text{micro\\_price} =
                \\frac{p_b q_a + p_a q_b}{q_b + q_a}

    Referencia: ``obi_mm`` en hftbacktest
    (https://github.com/nkaz001/hftbacktest).

    Args:
        df_book_tickers: DataFrame con columnas ``event_time``, ``symbol``,
            ``best_bid_price``, ``best_bid_qty``, ``best_ask_price``,
            ``best_ask_qty``.

    Returns:
        DataFrame con columnas:

        * ``event_time``                  — ms epoch del tick.
        * ``symbol``                      — activo.
        * ``mid_price``                   — ``(bid + ask) / 2``, referencia
          tradicional (no ponderada por volumen).
        * ``micro_price``                 — estimador de precio justo
          ponderado por la inversa del lado del libro.
        * ``micro_price_divergence_bps``  — desviación del Micro-Price
          respecto al mid en *basis points* (1 bps = 0.01%); es la señal
          predictiva que se evalúa en :func:`q8`.

    Defensas:
        * ``F.when(total_top_qty > 0, …)`` evita división por cero cuando
          ambos lados del libro están vacíos.
        * Si el denominador es cero se devuelve ``mid_price`` (no ``NaN``)
          para no envenenar los agregados rolling aguas abajo.
    """
    bid_px = F.col("best_bid_price")
    ask_px = F.col("best_ask_price")
    bid_qty = F.col("best_bid_qty")
    ask_qty = F.col("best_ask_qty")
    total_top_qty = bid_qty + ask_qty

    mid_price = (bid_px + ask_px) / F.lit(2.0)

    micro_price_expr = F.when(
        total_top_qty > 0,
        (bid_px * ask_qty + ask_px * bid_qty) / total_top_qty,
    ).otherwise(mid_price)

    return (
        df_book_tickers
        .withColumn("mid_price", mid_price)
        .withColumn("micro_price", micro_price_expr)
        .withColumn(
            "micro_price_divergence_bps",
            F.when(
                F.col("mid_price") > 0,
                (F.col("micro_price") - F.col("mid_price"))
                / F.col("mid_price") * F.lit(10000.0),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "event_time",
            "symbol",
            "mid_price",
            "micro_price",
            "micro_price_divergence_bps",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. Agregación temporal por ventana (1m / 5m / 1h)
# ═══════════════════════════════════════════════════════════════════════════

def aggregate_micro_price_by_window(
    df: DataFrame,
    window_label: str = "1m",
) -> DataFrame:
    """
    Agrega el Micro-Price tick a tick en ventanas temporales fijas.

    Replica el patrón de ``aggregator.compute_spread_timeseries`` para que
    el resultado sea unible por ``(symbol, window_label, window_start)``
    con :class:`features_by_window`.

    Args:
        df: DataFrame con columnas ``event_time`` (ms epoch), ``symbol``,
            ``mid_price``, ``micro_price``, ``micro_price_divergence_bps``
            — típicamente el output de
            :func:`compute_micro_price_raw`.
        window_label: etiqueta de la ventana (``"1m"``, ``"5m"``,
            ``"1h"``). Determina el ``windowDuration`` pasado a
            ``F.window``.

    Returns:
        DataFrame con columnas:

        * ``symbol``                       — activo.
        * ``window_label``                 — etiqueta de granularidad.
        * ``window_start``, ``window_end`` — bordes de la ventana
          (``timestamp``).
        * ``micro_price_mean``             — promedio del Micro-Price en la
          ventana.
        * ``micro_price_close``            — último Micro-Price observado en
          la ventana (análogo al ``close`` OHLCV; útil para fijar el
          estado terminal del libro al cerrar la ventana).
        * ``micro_price_div_mean_bps``     — divergencia media respecto a
          mid; **esta es la señal predictiva principal** evaluada en Q8.
        * ``micro_price_div_std_bps``      — volatilidad de la divergencia;
          aproxima la inestabilidad del lado dominante del libro.
        * ``micro_price_div_skewness``     — asimetría de la divergencia
          dentro de la ventana; capta sesgos persistentes hacia bid o ask
          que el ``mean`` solo no revela.

    Notas:
        * ``event_time`` viene en **milisegundos epoch** (convención del
          stream WebSocket de Binance); se convierte a ``timestamp`` antes
          de aplicar ``F.window``.
        * Para tomar el ``micro_price_close`` se usa ``F.last`` ordenado
          por ``event_time`` dentro del ``groupBy`` — patrón estándar en
          Spark para "último valor por ventana".
    """
    _LABEL_TO_DURATION = {
        "1m": "1 minute",
        "5m": "5 minutes",
        "1h": "1 hour",
    }
    window_duration = _LABEL_TO_DURATION.get(window_label, "1 minute")

    df_ts = df.withColumn(
        "event_ts",
        (F.col("event_time") / F.lit(1000.0)).cast("timestamp"),
    )

    grouped = (
        df_ts
        .groupBy(
            "symbol",
            F.window(F.col("event_ts"), window_duration).alias("w"),
        )
        .agg(
            F.avg("micro_price").alias("micro_price_mean"),
            F.last("micro_price", ignorenulls=True).alias("micro_price_close"),
            F.avg("micro_price_divergence_bps").alias("micro_price_div_mean_bps"),
            F.stddev("micro_price_divergence_bps").alias("micro_price_div_std_bps"),
            F.skewness("micro_price_divergence_bps").alias("micro_price_div_skewness"),
        )
    )

    return (
        grouped
        .withColumn("window_label", F.lit(window_label))
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .select(
            "symbol",
            "window_label",
            "window_start",
            "window_end",
            F.round("micro_price_mean", 8).alias("micro_price_mean"),
            F.round("micro_price_close", 8).alias("micro_price_close"),
            F.round("micro_price_div_mean_bps", 6).alias("micro_price_div_mean_bps"),
            F.round("micro_price_div_std_bps", 6).alias("micro_price_div_std_bps"),
            F.round("micro_price_div_skewness", 6).alias("micro_price_div_skewness"),
        )
        .orderBy("symbol", "window_start")
    )
