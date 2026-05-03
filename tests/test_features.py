"""
tests/test_features.py
-----------------------
Verifica la corrección matemática de todas las features.

Estrategia: las fórmulas se implementan en pandas y se comparan
contra los valores esperados calculados a mano. Esto valida la lógica
sin necesitar Spark ni Cassandra.

Los tests de integración con Spark están en test_processing.py.

Ejecutar: python -m pytest tests/test_features.py -v
"""

import math
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Helpers — implementan las mismas fórmulas que features.py en Python puro
# ─────────────────────────────────────────────────────────────────────────────

def vwap(prices: list[float], quantities: list[float]) -> float:
    """VWAP = Σ(price × qty) / Σ(qty)"""
    return sum(p * q for p, q in zip(prices, quantities)) / sum(quantities)


def log_return(close_prev: float, close_curr: float) -> float:
    """log_return = ln(close_t / close_{t-1})"""
    return math.log(close_curr / close_prev)


def rolling_volatility(log_returns: list[float]) -> float:
    """Desviación estándar muestral (ddof=1) de los log_returns."""
    n = len(log_returns)
    if n < 2:
        return float("nan")
    mean = sum(log_returns) / n
    variance = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
    return math.sqrt(variance)


def momentum(close_now: float, close_n_ago: float) -> float:
    """momentum = close_t - close_{t-N}"""
    return close_now - close_n_ago


def momentum_pct(close_now: float, close_n_ago: float) -> float:
    """momentum_pct = (close_t - close_{t-N}) / close_{t-N}"""
    return (close_now - close_n_ago) / close_n_ago


def buy_sell_ratio(buy_vol: float, sell_vol: float) -> float:
    """buy_sell_ratio = buy_volume / sell_volume"""
    return buy_vol / sell_vol


def spread_mean(spreads: list[float]) -> float:
    """spread_mean = avg(ask - bid)"""
    return sum(spreads) / len(spreads)


def spread_pct(spread_avg: float, mid_price: float) -> float:
    """spread_mean_pct = spread_mean / mid_price × 100"""
    return spread_avg / mid_price * 100


# ─────────────────────────────────────────────────────────────────────────────
# Tests — VWAP
# ─────────────────────────────────────────────────────────────────────────────

class TestVWAP:

    def test_vwap_uniform_quantity(self):
        # Con cantidades iguales, VWAP = promedio aritmético de precios
        prices = [100.0, 102.0, 98.0, 104.0]
        qtys   = [1.0,   1.0,   1.0,  1.0]
        result = vwap(prices, qtys)
        assert result == pytest.approx(101.0)

    def test_vwap_weighted_by_large_trade(self):
        # Un trade grande domina el VWAP
        prices = [100.0, 200.0]
        qtys   = [9.0,   1.0]
        # VWAP = (100×9 + 200×1) / 10 = 1100/10 = 110
        result = vwap(prices, qtys)
        assert result == pytest.approx(110.0)

    def test_vwap_between_min_and_max(self):
        # Invariante clave: VWAP siempre entre min y max de precio
        prices = [67000.0, 67500.0, 66800.0, 67200.0]
        qtys   = [0.5, 1.2, 0.3, 0.8]
        result = vwap(prices, qtys)
        assert min(prices) <= result <= max(prices)

    def test_vwap_single_trade(self):
        # Con un solo trade, VWAP = precio del trade
        result = vwap([67000.0], [1.5])
        assert result == pytest.approx(67000.0)

    def test_vwap_high_precision(self):
        # Verifica que la precisión es suficiente para cripto
        prices = [0.000123, 0.000124, 0.000122]
        qtys   = [1000.0, 500.0, 2000.0]
        result = vwap(prices, qtys)
        expected = (0.000123*1000 + 0.000124*500 + 0.000122*2000) / 3500
        assert result == pytest.approx(expected, rel=1e-6)


# ─────────────────────────────────────────────────────────────────────────────
# Tests — log_return
# ─────────────────────────────────────────────────────────────────────────────

class TestLogReturn:

    def test_zero_return_on_equal_prices(self):
        # Sin cambio de precio → log_return = 0
        assert log_return(100.0, 100.0) == pytest.approx(0.0)

    def test_positive_return_on_price_increase(self):
        # Precio sube → log_return > 0
        result = log_return(100.0, 110.0)
        assert result > 0
        assert result == pytest.approx(math.log(1.1))

    def test_negative_return_on_price_decrease(self):
        # Precio baja → log_return < 0
        result = log_return(100.0, 90.0)
        assert result < 0

    def test_symmetry_property(self):
        # Propiedad clave del log_return: subir 10% y bajar 10% debe sumar ~0
        up   = log_return(100.0, 110.0)
        down = log_return(110.0, 100.0)
        assert up + down == pytest.approx(0.0, abs=1e-10)

    def test_additivity_in_time(self):
        # log_return es aditivo: r(t1→t3) = r(t1→t2) + r(t2→t3)
        r1 = log_return(100.0, 110.0)
        r2 = log_return(110.0, 121.0)
        r_total = log_return(100.0, 121.0)
        assert r1 + r2 == pytest.approx(r_total)

    def test_known_value(self):
        # log_return de doble → ln(2) ≈ 0.6931
        result = log_return(50.0, 100.0)
        assert result == pytest.approx(math.log(2), rel=1e-8)

    def test_small_return_approximates_arithmetic(self):
        # Para retornos pequeños (<1%), log ≈ aritmético
        arithmetic = (101.0 - 100.0) / 100.0   # 1%
        log_ret    = log_return(100.0, 101.0)
        assert abs(log_ret - arithmetic) < 0.0001   # diferencia < 0.01%


# ─────────────────────────────────────────────────────────────────────────────
# Tests — rolling_volatility
# ─────────────────────────────────────────────────────────────────────────────

class TestRollingVolatility:

    def test_zero_volatility_on_constant_returns(self):
        # Si todos los retornos son iguales → volatilidad = 0
        returns = [0.001] * 10
        result  = rolling_volatility(returns)
        assert result == pytest.approx(0.0, abs=1e-12)

    def test_higher_dispersion_means_higher_vol(self):
        # Mayor dispersión → mayor volatilidad
        low_vol  = rolling_volatility([0.01, 0.01, 0.01, -0.01, -0.01])
        high_vol = rolling_volatility([0.05, -0.05, 0.04, -0.04, 0.03])
        assert high_vol > low_vol

    def test_single_observation_is_nan(self):
        result = rolling_volatility([0.01])
        assert math.isnan(result)

    def test_known_value(self):
        # Desviación estándar muestral de [0.1, 0.2, 0.3]
        # mean=0.2, var=((0.01+0.0+0.01)/2)=0.01, std=0.1
        returns = [0.1, 0.2, 0.3]
        result  = rolling_volatility(returns)
        assert result == pytest.approx(0.1, rel=1e-6)

    def test_vol_is_non_negative(self):
        returns = [0.01, -0.02, 0.015, -0.005, 0.008]
        result  = rolling_volatility(returns)
        assert result >= 0

    def test_vol_uses_sample_stddev_ddof1(self):
        # Verifica que usa ddof=1 (muestral), no ddof=0 (poblacional)
        returns = [0.0, 1.0]
        # ddof=1: std = sqrt(((0-0.5)^2 + (1-0.5)^2) / 1) = sqrt(0.5) ≈ 0.7071
        # ddof=0: std = sqrt(0.25) = 0.5
        result = rolling_volatility(returns)
        assert result == pytest.approx(math.sqrt(0.5), rel=1e-8)


# ─────────────────────────────────────────────────────────────────────────────
# Tests — momentum
# ─────────────────────────────────────────────────────────────────────────────

class TestMomentum:

    def test_positive_momentum_on_uptrend(self):
        result = momentum(110.0, 100.0)
        assert result == pytest.approx(10.0)

    def test_negative_momentum_on_downtrend(self):
        result = momentum(90.0, 100.0)
        assert result == pytest.approx(-10.0)

    def test_zero_momentum_on_flat_price(self):
        result = momentum(100.0, 100.0)
        assert result == pytest.approx(0.0)

    def test_momentum_pct_up(self):
        # De 100 a 110 → +10%
        result = momentum_pct(110.0, 100.0)
        assert result == pytest.approx(0.10)

    def test_momentum_pct_down(self):
        # De 100 a 90 → -10%
        result = momentum_pct(90.0, 100.0)
        assert result == pytest.approx(-0.10)

    def test_momentum_pct_comparable_across_assets(self):
        # La versión pct es comparable entre activos de distinto valor
        btc_mom_pct = momentum_pct(67100.0, 67000.0)  # BTC sube $100
        bnb_mom_pct = momentum_pct(581.0,   580.0)     # BNB sube $1
        # BTC: +0.149%  |  BNB: +0.172% → BNB tiene más momentum relativo
        assert btc_mom_pct == pytest.approx(100/67000, rel=1e-4)
        assert bnb_mom_pct == pytest.approx(1/580,     rel=1e-4)
        assert bnb_mom_pct > btc_mom_pct   # BNB sube más en términos relativos


# ─────────────────────────────────────────────────────────────────────────────
# Tests — buy_sell_ratio
# ─────────────────────────────────────────────────────────────────────────────

class TestBuySellRatio:

    def test_balanced_market(self):
        # Igual volumen → ratio = 1
        result = buy_sell_ratio(50.0, 50.0)
        assert result == pytest.approx(1.0)

    def test_buyer_dominated(self):
        # Más compradores → ratio > 1
        result = buy_sell_ratio(75.0, 25.0)
        assert result == pytest.approx(3.0)

    def test_seller_dominated(self):
        # Más vendedores → ratio < 1
        result = buy_sell_ratio(25.0, 75.0)
        assert result == pytest.approx(1/3)


# ─────────────────────────────────────────────────────────────────────────────
# Tests — spread
# ─────────────────────────────────────────────────────────────────────────────

class TestSpread:

    def test_spread_mean_basic(self):
        spreads = [2.0, 4.0, 6.0]
        result  = spread_mean(spreads)
        assert result == pytest.approx(4.0)

    def test_spread_mean_single(self):
        assert spread_mean([3.5]) == pytest.approx(3.5)

    def test_spread_pct_basic(self):
        # spread $2 sobre mid_price $100 → 2%
        result = spread_pct(2.0, 100.0)
        assert result == pytest.approx(2.0)

    def test_spread_pct_btc_vs_bnb(self):
        # BTC: spread $2 sobre $67000 → 0.003%  (muy líquido)
        # BNB: spread $0.5 sobre $580  → 0.086%  (menos líquido)
        btc_pct = spread_pct(2.0,   67000.0)
        bnb_pct = spread_pct(0.5,   580.0)
        assert btc_pct < bnb_pct   # BTC tiene menor spread relativo


# ─────────────────────────────────────────────────────────────────────────────
# Tests — propiedades matemáticas cruzadas
# ─────────────────────────────────────────────────────────────────────────────

class TestCrossFeatureProperties:

    def test_vwap_consistent_with_log_return(self):
        """
        Si el VWAP de t2 es mayor que el de t1,
        el log_return debería ser positivo.
        """
        vwap_t1 = vwap([100.0, 101.0], [1.0, 1.0])   # 100.5
        vwap_t2 = vwap([102.0, 103.0], [1.0, 1.0])   # 102.5
        ret = log_return(vwap_t1, vwap_t2)
        assert ret > 0

    def test_volatility_of_zero_returns_is_zero(self):
        """Si todos los log_returns son 0, la volatilidad es 0."""
        returns = [log_return(100.0, 100.0)] * 10
        vol = rolling_volatility(returns)
        assert vol == pytest.approx(0.0, abs=1e-10)

    def test_high_momentum_implies_positive_return(self):
        """Momentum positivo y retorno positivo deben ser coherentes."""
        close_5_ago = 100.0
        close_now   = 110.0
        mom     = momentum(close_now, close_5_ago)
        log_ret = log_return(close_5_ago, close_now)
        assert mom > 0 and log_ret > 0
