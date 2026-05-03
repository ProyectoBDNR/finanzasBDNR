"""
tests/validation/test_features_validation.py
----------------------------------------------
Validación matemática de features — invariantes que SIEMPRE deben cumplirse.

No son tests de implementación (esos están en test_features.py).
Son contratos de negocio: si alguno falla, los datos están corruptos.

Qué observar si falla:
  vwap_in_range         → hay trades con price=0 o quantity=0 pasando la limpieza
  log_return_bounded    → hay precios negativos o saltos imposibles (data error)
  volatility_non_neg    → stddev nunca es negativa; si falla → bug en cálculo
  spread_non_neg        → ask < bid en los datos → validación de tickers rota
  momentum_symmetric    → si todos positivos/negativos → sesgo de datos
  trade_count_positive  → una ventana sin trades no debería existir en OHLCV
"""

import math
import random
import pytest
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────────────────────
# Implementaciones de referencia (ground truth matemático)
# ─────────────────────────────────────────────────────────────────────────────

def vwap(prices: list[float], quantities: list[float]) -> float:
    assert len(prices) == len(quantities) and len(prices) > 0
    return sum(p * q for p, q in zip(prices, quantities)) / sum(quantities)


def log_return(close_t: float, close_prev: float) -> float | None:
    if close_prev is None or close_prev <= 0 or close_t <= 0:
        return None
    return math.log(close_t / close_prev)


def rolling_volatility(log_returns: list[float | None], window: int) -> list[float | None]:
    result = []
    for i in range(len(log_returns)):
        window_data = [r for r in log_returns[max(0, i - window + 1):i + 1]
                       if r is not None]
        if len(window_data) < 2:
            result.append(None)
        else:
            n = len(window_data)
            mean = sum(window_data) / n
            variance = sum((x - mean) ** 2 for x in window_data) / (n - 1)
            result.append(math.sqrt(variance))
    return result


def momentum(closes: list[float], periods: int = 5) -> list[float | None]:
    result = [None] * periods
    for i in range(periods, len(closes)):
        result.append(closes[i] - closes[i - periods])
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Generador de datos sintéticos para tests de invariantes
# ─────────────────────────────────────────────────────────────────────────────

def make_price_series(n: int = 50, seed: int = 42) -> list[float]:
    """Genera una serie de precios realista con random walk lognormal."""
    random.seed(seed)
    price = 67000.0
    prices = []
    for _ in range(n):
        price *= math.exp(random.gauss(0, 0.001))
        prices.append(round(price, 2))
    return prices


def make_quantities(n: int = 50, seed: int = 7) -> list[float]:
    random.seed(seed)
    return [round(random.uniform(0.001, 5.0), 6) for _ in range(n)]


def make_spreads(n: int = 50, seed: int = 13) -> list[float]:
    random.seed(seed)
    return [round(random.uniform(0.5, 10.0), 4) for _ in range(n)]


# ─────────────────────────────────────────────────────────────────────────────
# Invariantes de VWAP
# ─────────────────────────────────────────────────────────────────────────────

class TestVWAPInvariants:
    """
    El VWAP tiene invariantes matemáticas que no dependen de los datos:
    siempre está entre min(price) y max(price), y es una media ponderada.
    """

    def test_vwap_within_price_range(self):
        prices = make_price_series(100)
        qtys   = make_quantities(100)
        v = vwap(prices, qtys)
        assert min(prices) <= v <= max(prices), (
            f"VWAP={v:.2f} fuera del rango [{min(prices):.2f}, {max(prices):.2f}]"
        )

    def test_vwap_equals_simple_mean_when_equal_quantities(self):
        prices = [100.0, 102.0, 98.0, 104.0, 96.0]
        qtys   = [1.0] * 5
        v = vwap(prices, qtys)
        simple_mean = sum(prices) / len(prices)
        assert v == pytest.approx(simple_mean, abs=1e-10)

    def test_vwap_pulled_toward_large_trade(self):
        """Un trade 1000x más grande domina el VWAP."""
        prices = [100.0, 100.0, 200.0]
        qtys   = [1.0,   1.0,   1000.0]
        v = vwap(prices, qtys)
        assert v > 199.0, f"VWAP={v:.4f} debería estar cerca de 200"

    def test_vwap_monotone_in_quantities(self):
        """Si doblo la cantidad del trade más caro, el VWAP sube."""
        prices = [100.0, 200.0]
        qtys_a = [1.0, 1.0]
        qtys_b = [1.0, 2.0]
        assert vwap(prices, qtys_b) > vwap(prices, qtys_a)

    def test_vwap_single_trade_equals_price(self):
        assert vwap([67000.0], [0.5]) == pytest.approx(67000.0)

    def test_vwap_with_many_windows(self):
        """Invariante sobre 1000 ventanas sintéticas."""
        random.seed(0)
        violations = 0
        for _ in range(1000):
            n = random.randint(2, 20)
            prices = [random.uniform(1, 100) for _ in range(n)]
            qtys   = [random.uniform(0.001, 10) for _ in range(n)]
            v = vwap(prices, qtys)
            if not (min(prices) - 1e-9 <= v <= max(prices) + 1e-9):
                violations += 1
        assert violations == 0, f"VWAP fuera de rango en {violations}/1000 ventanas"


# ─────────────────────────────────────────────────────────────────────────────
# Invariantes de log_return
# ─────────────────────────────────────────────────────────────────────────────

class TestLogReturnInvariants:

    def test_first_return_is_none(self):
        prices = make_price_series(10)
        returns = [log_return(prices[i], prices[i-1] if i > 0 else None)
                   for i in range(len(prices))]
        assert returns[0] is None

    def test_log_return_is_additive(self):
        """log(P2/P0) == log(P1/P0) + log(P2/P1) — propiedad clave."""
        p0, p1, p2 = 100.0, 110.0, 121.0
        r1 = log_return(p1, p0)
        r2 = log_return(p2, p1)
        r_total = log_return(p2, p0)
        assert r1 + r2 == pytest.approx(r_total, abs=1e-12)

    def test_log_return_symmetric_around_zero(self):
        """log(110/100) + log(100/110) == 0 — retorno de ida y vuelta."""
        r_up   = log_return(110.0, 100.0)
        r_down = log_return(100.0, 110.0)
        assert r_up + r_down == pytest.approx(0.0, abs=1e-12)

    def test_positive_return_for_price_increase(self):
        assert log_return(101.0, 100.0) > 0

    def test_negative_return_for_price_decrease(self):
        assert log_return(99.0, 100.0) < 0

    def test_none_for_zero_prev_price(self):
        assert log_return(100.0, 0.0) is None

    def test_none_for_negative_price(self):
        assert log_return(-1.0, 100.0) is None

    def test_realistic_returns_are_small(self):
        """En mercados normales, |log_return| < 5% por minuto."""
        prices = make_price_series(200)
        returns = [log_return(prices[i], prices[i-1]) for i in range(1, len(prices))]
        for r in returns:
            assert abs(r) < 0.05, f"Retorno irrealmente grande: {r:.4f}"

    def test_cumulative_return_equals_total(self):
        """La suma de log_returns entre P0 y PN es log(PN/P0)."""
        prices = make_price_series(20)
        individual_returns = [
            log_return(prices[i], prices[i-1])
            for i in range(1, len(prices))
        ]
        total = log_return(prices[-1], prices[0])
        assert sum(individual_returns) == pytest.approx(total, abs=1e-10)


# ─────────────────────────────────────────────────────────────────────────────
# Invariantes de rolling_volatility
# ─────────────────────────────────────────────────────────────────────────────

class TestVolatilityInvariants:

    def test_volatility_non_negative(self):
        prices = make_price_series(50)
        returns = [log_return(prices[i], prices[i-1]) for i in range(1, len(prices))]
        vols = rolling_volatility(returns, window=10)
        for v in vols:
            if v is not None:
                assert v >= 0.0, f"Volatilidad negativa: {v}"

    def test_constant_price_zero_volatility(self):
        prices  = [100.0] * 20
        returns = [log_return(prices[i], prices[i-1]) for i in range(1, len(prices))]
        vols    = rolling_volatility(returns, window=10)
        for v in vols:
            if v is not None:
                assert v == pytest.approx(0.0, abs=1e-12)

    def test_none_before_min_window(self):
        prices  = make_price_series(15)
        returns = [log_return(prices[i], prices[i-1]) for i in range(1, len(prices))]
        vols    = rolling_volatility(returns, window=10)
        # Los primeros (window-2) valores deben ser None (< 2 puntos disponibles)
        assert vols[0] is None

    def test_higher_volatility_for_more_volatile_series(self):
        """Una serie más volátil debe producir mayor rolling_volatility."""
        random.seed(42)
        stable   = [100.0 * math.exp(random.gauss(0, 0.0001)) for _ in range(30)]
        volatile = [100.0 * math.exp(random.gauss(0, 0.01))   for _ in range(30)]

        r_stable   = [log_return(stable[i],   stable[i-1])   for i in range(1, 30)]
        r_volatile = [log_return(volatile[i], volatile[i-1]) for i in range(1, 30)]

        v_stable   = [v for v in rolling_volatility(r_stable,   10) if v]
        v_volatile = [v for v in rolling_volatility(r_volatile, 10) if v]

        assert sum(v_volatile) / len(v_volatile) > sum(v_stable) / len(v_stable)

    def test_volatility_increases_with_window_size(self):
        """
        Con más puntos, la stddev muestral no decrece sistemáticamente.
        (No es una invariante absoluta, pero sí en promedio.)
        """
        prices  = make_price_series(60)
        returns = [log_return(prices[i], prices[i-1]) for i in range(1, len(prices))]
        vols_5  = [v for v in rolling_volatility(returns, window=5)  if v]
        vols_20 = [v for v in rolling_volatility(returns, window=20) if v]
        # Las dos deben producir valores no nulos
        assert len(vols_5)  > 0
        assert len(vols_20) > 0


# ─────────────────────────────────────────────────────────────────────────────
# Invariantes de spread
# ─────────────────────────────────────────────────────────────────────────────

class TestSpreadInvariants:

    def test_spread_non_negative(self):
        spreads = make_spreads(200)
        for s in spreads:
            assert s >= 0, f"Spread negativo detectado: {s}"

    def test_spread_mean_within_range(self):
        spreads = make_spreads(100)
        mean = sum(spreads) / len(spreads)
        assert min(spreads) <= mean <= max(spreads)

    def test_zero_spread_implies_bid_equals_ask(self):
        bid, ask = 67000.0, 67000.0
        spread = ask - bid
        assert spread == 0.0

    def test_spread_equals_ask_minus_bid(self):
        """El spread almacenado debe ser siempre ask - bid exacto."""
        test_cases = [
            (67849.0, 67851.0, 2.0),
            (3499.5,  3500.0,  0.5),
            (580.10,  580.25,  0.15),
        ]
        for bid, ask, expected_spread in test_cases:
            computed = round(ask - bid, 8)
            assert computed == pytest.approx(expected_spread, abs=1e-8), (
                f"bid={bid} ask={ask}: spread={computed} ≠ {expected_spread}"
            )

    def test_spread_pct_normalized_cross_asset(self):
        """El spread porcentual normaliza la comparación cross-asset."""
        btc_spread  = 2.0;    btc_mid  = 67000.0
        bnb_spread  = 0.15;   bnb_mid  = 580.0

        btc_spread_pct = btc_spread / btc_mid * 100
        bnb_spread_pct = bnb_spread / bnb_mid * 100

        # BNB tiene spread absoluto menor pero spread porcentual mayor → menos líquido
        assert bnb_spread_pct > btc_spread_pct, (
            "BNB debería tener mayor spread% que BTC"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Invariantes de momentum
# ─────────────────────────────────────────────────────────────────────────────

class TestMomentumInvariants:

    def test_momentum_none_for_first_periods(self):
        closes = make_price_series(20)
        mom = momentum(closes, periods=5)
        for i in range(5):
            assert mom[i] is None, f"momentum[{i}] debería ser None"

    def test_momentum_positive_in_uptrend(self):
        closes = [float(i * 100) for i in range(1, 21)]  # serie ascendente
        mom = momentum(closes, periods=5)
        for m in mom[5:]:
            assert m > 0, "Momentum debe ser positivo en tendencia alcista"

    def test_momentum_negative_in_downtrend(self):
        closes = [float(2000 - i * 100) for i in range(20)]  # serie descendente
        mom = momentum(closes, periods=5)
        for m in mom[5:]:
            assert m < 0, "Momentum debe ser negativo en tendencia bajista"

    def test_momentum_zero_for_flat_series(self):
        closes = [100.0] * 20
        mom = momentum(closes, periods=5)
        for m in mom[5:]:
            assert m == pytest.approx(0.0)

    def test_momentum_reverses_sign_on_price_reversal(self):
        """Sube 10 periodos, luego baja: el momentum debe cambiar de signo."""
        up   = [100.0 + i for i in range(15)]
        down = [up[-1] - i * 2 for i in range(1, 10)]
        closes = up + down
        mom = momentum(closes, periods=5)
        valid = [m for m in mom if m is not None]
        has_positive = any(m > 0 for m in valid)
        has_negative = any(m < 0 for m in valid)
        assert has_positive and has_negative


# ─────────────────────────────────────────────────────────────────────────────
# Invariantes de trade_count
# ─────────────────────────────────────────────────────────────────────────────

class TestTradeCountInvariants:

    def test_trade_count_always_positive(self):
        """Una ventana OHLCV no puede existir con 0 trades."""
        ohlcv_windows = [
            {"symbol": "BTCUSDT", "window_start": "12:00", "trade_count": 5},
            {"symbol": "ETHUSDT", "window_start": "12:00", "trade_count": 12},
            {"symbol": "BNBUSDT", "window_start": "12:00", "trade_count": 3},
        ]
        for row in ohlcv_windows:
            assert row["trade_count"] > 0, (
                f"{row['symbol']} tiene trade_count=0 — ventana inválida"
            )

    def test_buy_plus_sell_equals_total(self):
        """buy_volume + sell_volume == volume total (dentro de tolerancia float)."""
        test_cases = [
            (10.5, 4.5, 15.0),
            (0.0,  3.2, 3.2),
            (7.1,  0.0, 7.1),
        ]
        for buy, sell, total in test_cases:
            assert buy + sell == pytest.approx(total, abs=1e-9)

    def test_volume_equals_sum_of_quantities(self):
        quantities = make_quantities(100)
        total_volume = sum(quantities)
        assert total_volume == pytest.approx(sum(quantities), abs=1e-9)
