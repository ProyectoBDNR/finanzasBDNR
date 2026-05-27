"""
tests/test_microprice.py
-------------------------
Verifica la corrección matemática del Micro-Price.

Estrategia: la fórmula se implementa en Python puro y se contrastan los
casos extremos contra los valores esperados calculados a mano. Esto
valida la lógica sin necesitar Spark ni Cassandra — sigue el patrón ya
establecido en :mod:`tests.test_features`.

Casos cubiertos:
    1. ``total_top_qty == 0`` → ``micro_price == mid_price``
       (no NaN, no DivisionByZero).
    2. ``bid_qty >> ask_qty``  → ``micro_price > mid_price``.
    3. ``bid_qty << ask_qty``  → ``micro_price < mid_price``.
    4. ``bid_qty == ask_qty``  → ``micro_price == mid_price``.

Adicionalmente se verifica la conversión a basis points y que el
``micro_price`` siempre cae dentro del rango ``[best_bid, best_ask]``.

Ejecutar: python -m pytest tests/test_microprice.py -v
"""

import math
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — replican la fórmula de feature_engine/microprice.py
# ─────────────────────────────────────────────────────────────────────────────

def mid_price(bid: float, ask: float) -> float:
    """``(bid + ask) / 2``."""
    return (bid + ask) / 2.0


def micro_price(
    bid: float,
    bid_qty: float,
    ask: float,
    ask_qty: float,
) -> float:
    """
    Implementación de referencia de la fórmula Micro-Price.

    Fórmula:
        micro_price = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)

    Si ``bid_qty + ask_qty == 0``, se devuelve ``mid_price`` como fallback
    seguro (mismo comportamiento que ``F.when(total > 0, …)``).
    """
    total = bid_qty + ask_qty
    if total <= 0:
        return mid_price(bid, ask)
    return (bid * ask_qty + ask * bid_qty) / total


def micro_price_divergence_bps(
    micro: float,
    mid: float,
) -> float:
    """``(micro - mid) / mid * 10_000`` — divergencia en basis points."""
    if mid <= 0:
        return 0.0
    return (micro - mid) / mid * 10_000.0


# ─────────────────────────────────────────────────────────────────────────────
# Tests — casos del prompt
# ─────────────────────────────────────────────────────────────────────────────

class TestMicroPriceBoundaryCases:

    def test_zero_total_top_qty_returns_mid_price(self):
        # Ambos lados del libro vacíos → fallback a mid_price (no NaN)
        bid, ask = 100.0, 101.0
        result = micro_price(bid, 0.0, ask, 0.0)
        assert result == pytest.approx(mid_price(bid, ask))
        assert not math.isnan(result)

    def test_bid_qty_much_greater_than_ask_qty(self):
        # bid_qty >> ask_qty → presión compradora latente → micro > mid
        bid, ask = 100.0, 101.0
        mp = micro_price(bid, 1000.0, ask, 1.0)
        assert mp > mid_price(bid, ask)
        # Y tiende a best_ask cuando el desbalance es extremo
        assert mp == pytest.approx(ask, rel=1e-2)

    def test_ask_qty_much_greater_than_bid_qty(self):
        # ask_qty >> bid_qty → presión vendedora latente → micro < mid
        bid, ask = 100.0, 101.0
        mp = micro_price(bid, 1.0, ask, 1000.0)
        assert mp < mid_price(bid, ask)
        # Y tiende a best_bid cuando el desbalance es extremo
        assert mp == pytest.approx(bid, rel=1e-2)

    def test_symmetric_qty_equals_mid_price(self):
        # bid_qty == ask_qty → micro_price = (bid + ask) / 2 = mid_price
        bid, ask = 100.0, 101.0
        mp = micro_price(bid, 5.0, ask, 5.0)
        assert mp == pytest.approx(mid_price(bid, ask))


# ─────────────────────────────────────────────────────────────────────────────
# Tests — propiedades matemáticas
# ─────────────────────────────────────────────────────────────────────────────

class TestMicroPriceProperties:

    def test_always_between_bid_and_ask(self):
        # Invariante: micro_price ∈ [best_bid, best_ask] para libro sano
        bid, ask = 67000.0, 67005.0
        for bq, aq in [(0.5, 1.2), (10.0, 0.1), (0.1, 10.0), (1.0, 1.0)]:
            mp = micro_price(bid, bq, ask, aq)
            assert bid <= mp <= ask, f"micro={mp} fuera de [{bid}, {ask}] para bq={bq}, aq={aq}"

    def test_divergence_positive_when_micro_above_mid(self):
        # bid_qty >> ask_qty → div_bps > 0
        bid, ask = 100.0, 101.0
        mp = micro_price(bid, 100.0, ask, 1.0)
        mid = mid_price(bid, ask)
        div = micro_price_divergence_bps(mp, mid)
        assert div > 0

    def test_divergence_negative_when_micro_below_mid(self):
        # ask_qty >> bid_qty → div_bps < 0
        bid, ask = 100.0, 101.0
        mp = micro_price(bid, 1.0, ask, 100.0)
        mid = mid_price(bid, ask)
        div = micro_price_divergence_bps(mp, mid)
        assert div < 0

    def test_divergence_zero_when_symmetric(self):
        # Libro simétrico → divergencia nula
        bid, ask = 100.0, 101.0
        mp = micro_price(bid, 5.0, ask, 5.0)
        mid = mid_price(bid, ask)
        assert micro_price_divergence_bps(mp, mid) == pytest.approx(0.0, abs=1e-10)

    def test_divergence_in_realistic_bps_range(self):
        # Sanidad: para un libro BTC típico con spread de 5 USD sobre 67000,
        # la divergencia máxima posible es del orden del spread relativo.
        bid, ask = 67000.0, 67005.0
        mp = micro_price(bid, 100.0, ask, 0.01)
        mid = mid_price(bid, ask)
        div = micro_price_divergence_bps(mp, mid)
        # Spread relativo ≈ 5/67002.5 * 10000 ≈ 0.75 bps. La divergencia
        # nunca puede excederlo en magnitud (porque micro ∈ [bid, ask]).
        spread_rel_bps = (ask - bid) / mid * 10_000.0
        assert abs(div) <= spread_rel_bps + 1e-9

    def test_known_value(self):
        # Caso numérico exacto:
        #   bid=100, ask=102, bid_qty=3, ask_qty=1
        #   micro = (100*1 + 102*3) / (3+1) = (100 + 306) / 4 = 406 / 4 = 101.5
        #   mid   = 101
        mp = micro_price(100.0, 3.0, 102.0, 1.0)
        assert mp == pytest.approx(101.5)
        assert mp > mid_price(100.0, 102.0)
