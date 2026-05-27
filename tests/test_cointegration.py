"""
tests/test_cointegration.py
----------------------------
Tests para `feature_engine.cointegration.compute_pairwise_zscore`.

Se cubren tres escenarios obligatorios:

1. `test_perfect_cointegration`
   Cuando `cum_hedge = 2 · cum_dom + 0.01` el OLS rolling debe recuperar
   alpha ≈ 0.01 y beta ≈ 2.0, y el |z_score| en el último punto debe ser
   pequeño (≈ 0, dentro de la tolerancia numérica del cálculo).

2. `test_shock_injection_no_lookahead`
   Inyectando un shock de +20% en `cum_hedge` en el índice
   `lookback + 5`, el |z_score| en ese punto debe superar 2.5 (la señal
   es capturada) PERO en cualquier punto anterior NO debe superar 1
   (el shock no "se filtra hacia el pasado" — bug clásico de
   look-ahead que se produciría si la ventana fuera
   `rowsBetween(0, lookback-1)`).

3. `test_insufficient_window_no_output`
   Si la serie tiene menos puntos que `lookback`, el output debe estar
   vacío y la función no debe lanzar excepciones.

Ejecutar: python -m pytest tests/test_cointegration.py -v
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from feature_engine.cointegration import PAIRS, compute_pairwise_zscore


# ---------------------------------------------------------------------------
# SparkSession compartida
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("cointegration-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


OHLCV_SCHEMA = StructType([
    StructField("symbol",       StringType(),    False),
    StructField("window_start", TimestampType(), False),
    StructField("close",        DoubleType(),    False),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_TS = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _make_close_from_cum(cum_series: list[float], close0: float) -> list[float]:
    """
    Dado un cum-return objetivo `cum_t` (con cum_0 = 1), reconstruye la
    serie de `close` tal que close_t / close_0 = cum_t.

    Esto garantiza que la implementación de `compute_pairwise_zscore`
    (que calcula cum-returns internamente con log1p+sum+exp) produzca
    exactamente la serie de cum-returns deseada en sus tests.
    """
    return [close0 * c for c in cum_series]


def _build_rows(
    sym_dom: str,
    sym_hedge: str,
    cum_dom: list[float],
    cum_hedge: list[float],
    close0_dom: float = 100.0,
    close0_hedge: float = 200.0,
):
    """
    Construye filas (symbol, window_start, close) para `sym_dom` y
    `sym_hedge` a partir de cum-returns objetivo.

    La primera fila tiene cum=1 (close=close0). Las siguientes se
    re-escalan según `cum_dom` / `cum_hedge`.
    """
    n = len(cum_dom)
    assert len(cum_hedge) == n, "cum_dom y cum_hedge deben tener mismo largo"

    # Insertamos un punto previo con cum=1 (close0) para que después del
    # cálculo de retornos y cumprod la serie cum coincida exactamente
    # con la deseada desde el índice 0 del usuario.
    full_dom = [1.0] + list(cum_dom)
    full_hedge = [1.0] + list(cum_hedge)
    closes_dom   = _make_close_from_cum(full_dom,   close0_dom)
    closes_hedge = _make_close_from_cum(full_hedge, close0_hedge)

    rows = []
    for i, (cd, ch) in enumerate(zip(closes_dom, closes_hedge)):
        ts = BASE_TS + timedelta(minutes=i)
        rows.append((sym_dom,   ts, float(cd)))
        rows.append((sym_hedge, ts, float(ch)))
    return rows


# ---------------------------------------------------------------------------
# 1. Cointegración perfecta
# ---------------------------------------------------------------------------

def test_perfect_cointegration(spark):
    """
    cum_hedge = 2·cum_dom + 0.01 (relación lineal exacta).

    En este caso el spread teórico es 0 y, por consiguiente, su stddev
    es 0 → el z_score sería 0/0 (indefinido). Para evitar la
    indeterminación introducimos una perturbación numérica
    insignificante en cum_dom (random walk muy pequeño) que mantiene
    la relación afín con tolerancia ≪ 1%.
    """
    lookback = 50
    n = lookback + 10

    # cum_dom: random walk centrado en 1.0 con incrementos muy pequeños
    # para que beta/alpha del OLS sean estables y el spread sea ≈ 0.
    rng_state = 12345
    cum_dom = []
    val = 1.0
    for i in range(n):
        # Pseudorandom determinístico (LCG simple) para reproducibilidad.
        rng_state = (rng_state * 1103515245 + 12345) & 0x7FFFFFFF
        delta = ((rng_state / 0x7FFFFFFF) - 0.5) * 0.001  # ±0.05%
        val *= (1.0 + delta)
        cum_dom.append(val)

    alpha_true, beta_true = 0.01, 2.0
    cum_hedge = [alpha_true + beta_true * c for c in cum_dom]

    rows = _build_rows("BTCUSDT", "ETHUSDT", cum_dom, cum_hedge)
    df = spark.createDataFrame(rows, schema=OHLCV_SCHEMA)

    pairs = [("BTCUSDT", "ETHUSDT")]
    out = (
        compute_pairwise_zscore(spark, df, pairs=pairs, lookback=lookback)
        .orderBy("window_start")
        .collect()
    )

    assert len(out) > 0, "No se produjo ninguna fila para serie cointegrada"

    last = out[-1]
    assert math.isclose(last["beta"],  beta_true,  abs_tol=1e-6), (
        f"beta esperado ≈ {beta_true}, obtenido {last['beta']}"
    )
    assert math.isclose(last["alpha"], alpha_true, abs_tol=1e-6), (
        f"alpha esperado ≈ {alpha_true}, obtenido {last['alpha']}"
    )
    assert abs(last["z_score"]) < 0.5, (
        f"|z_score| en el último punto debería ser < 0.5 para serie "
        f"cointegrada, obtenido {last['z_score']}"
    )


# ---------------------------------------------------------------------------
# 2. Shock inyectado — NO debe filtrarse hacia el pasado
# ---------------------------------------------------------------------------

def test_shock_injection_no_lookahead(spark):
    """
    A una serie cointegrada se le inyecta un shock de +20% en
    `cum_hedge[lookback+5]`. Tras la inyección, el z_score en ese
    punto debe ser > 2.5 (señal capturada). Pero en cualquier punto
    ANTERIOR el |z_score| NO debe superar 1 (el shock no "se filtra
    hacia el pasado" — esto caza el bug de look-ahead que se produce
    si la ventana es `rowsBetween(0, lookback-1)`).
    """
    lookback = 30
    n = lookback + 20
    shock_idx = lookback + 5
    shock_size = 0.20  # +20%

    cum_dom = [1.0 + 0.0001 * i for i in range(n)]
    cum_hedge = [0.01 + 2.0 * c for c in cum_dom]
    cum_hedge[shock_idx] *= (1.0 + shock_size)

    rows = _build_rows("BTCUSDT", "ETHUSDT", cum_dom, cum_hedge)
    df = spark.createDataFrame(rows, schema=OHLCV_SCHEMA)

    pairs = [("BTCUSDT", "ETHUSDT")]
    out = (
        compute_pairwise_zscore(spark, df, pairs=pairs, lookback=lookback)
        .orderBy("window_start")
        .collect()
    )

    assert len(out) > 0, "No se produjeron filas para la serie con shock"

    # Mapeo timestamp → fila para localizar el shock.
    shock_ts = BASE_TS + timedelta(minutes=shock_idx + 1)  # +1 por la fila base

    shock_row = next((r for r in out if r["window_start"] == shock_ts), None)
    assert shock_row is not None, (
        "No se encontró fila correspondiente al instante del shock"
    )
    assert abs(shock_row["z_score"]) > 2.5, (
        f"|z_score| en t=shock debería ser > 2.5, obtenido "
        f"{shock_row['z_score']}"
    )

    # Cualquier fila ANTES del shock no debería estar contaminada.
    pre_shock = [r for r in out if r["window_start"] < shock_ts]
    assert len(pre_shock) > 0, (
        "Se necesita al menos una fila previa al shock para validar el "
        "no-leakage; ajustar el tamaño de la serie."
    )
    for r in pre_shock:
        assert abs(r["z_score"]) <= 1.0, (
            f"Bug de look-ahead detectado: |z_score|={r['z_score']} en "
            f"t={r['window_start']} (anterior al shock en {shock_ts}). "
            f"La ventana rolling debe ser rowsBetween(-(lookback-1), 0)."
        )


# ---------------------------------------------------------------------------
# 3. Ventana insuficiente — output vacío sin crash
# ---------------------------------------------------------------------------

def test_insufficient_window_no_output(spark):
    """
    Con menos puntos que `lookback`, la función no debe lanzar
    excepciones y el output debe estar vacío (no NaN / no rows).
    """
    lookback = 100
    n = 10  # << lookback

    cum_dom = [1.0 + 0.001 * i for i in range(n)]
    cum_hedge = [0.01 + 2.0 * c for c in cum_dom]

    rows = _build_rows("BTCUSDT", "ETHUSDT", cum_dom, cum_hedge)
    df = spark.createDataFrame(rows, schema=OHLCV_SCHEMA)

    out = compute_pairwise_zscore(
        spark, df, pairs=[("BTCUSDT", "ETHUSDT")], lookback=lookback,
    ).collect()

    assert out == [], (
        f"Se esperaba output vacío con n={n} < lookback={lookback}, "
        f"obtenidas {len(out)} filas"
    )


# ---------------------------------------------------------------------------
# Sanity: constantes exportadas
# ---------------------------------------------------------------------------

def test_pairs_constant():
    assert ("BTCUSDT", "ETHUSDT") in PAIRS
    assert ("BTCUSDT", "BNBUSDT") in PAIRS
    assert ("ETHUSDT", "BNBUSDT") in PAIRS
    assert len(PAIRS) == 3
