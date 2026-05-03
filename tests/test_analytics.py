"""
tests/test_analytics.py
------------------------
Verifica la corrección matemática de las 7 queries analíticas.
Sin Spark — todas las fórmulas se prueban en Python puro.

Ejecutar: python -m pytest tests/test_analytics.py -v
"""

import math
import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Implementaciones Python puro — espejo de las fórmulas en queries.py
# ─────────────────────────────────────────────────────────────────────────────

def classify_regime(vol: float, p33: float, p66: float) -> str:
    if vol is None or math.isnan(vol):
        return "UNKNOWN"
    if vol <= p33:
        return "LOW"
    if vol <= p66:
        return "MED"
    return "HIGH"


def regime_transition(seq: list[str]) -> list[bool]:
    """True en cada posición donde el régimen cambia respecto al anterior."""
    changes = [False]  # primera fila nunca es transición
    for i in range(1, len(seq)):
        changes.append(seq[i] != seq[i - 1])
    return changes


def spread_quartile_profile(vols: list[float], spreads: list[float]) -> dict:
    """Spread promedio por cuartil de volumen (Q1=más bajo, Q4=más alto)."""
    n = len(vols)
    assert n == len(spreads)
    sorted_pairs = sorted(zip(vols, spreads), key=lambda x: x[0])
    q_size = n // 4
    result = {}
    for q in range(1, 5):
        start = (q - 1) * q_size
        end = q * q_size if q < 4 else n
        chunk_spreads = [s for _, s in sorted_pairs[start:end]]
        result[q] = sum(chunk_spreads) / len(chunk_spreads)
    return result


def momentum_direction(momentum_pct: float | None) -> int:
    if momentum_pct is None:
        return 0
    if momentum_pct > 0:
        return 1
    if momentum_pct < 0:
        return -1
    return 0


def consensus_score(dirs: list[int]) -> int:
    return sum(dirs)


def volume_zscore(volume: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0
    return (volume - mean) / std


def rolling_mean_std(values: list[float], window: int, idx: int):
    start = max(0, idx - window + 1)
    chunk = values[start : idx + 1]
    n = len(chunk)
    if n < 2:
        return sum(chunk) / n if chunk else 0, 0.0
    mean = sum(chunk) / n
    variance = sum((x - mean) ** 2 for x in chunk) / (n - 1)
    return mean, math.sqrt(variance)


def tracking_error_pct(close: float, vwap: float) -> float:
    return (close - vwap) / vwap * 100


def cumulative_pressure(buy_sell_ratios: list[float], window: int, idx: int) -> float:
    start = max(0, idx - window + 1)
    chunk = buy_sell_ratios[start : idx + 1]
    return sum(math.log(r) for r in chunk if r > 0)


def latency_percentile(latencies: list[float], pct: float) -> float:
    sorted_l = sorted(latencies)
    idx = int(math.ceil(pct * len(sorted_l))) - 1
    return sorted_l[max(0, idx)]


# ─────────────────────────────────────────────────────────────────────────────
# Q1 — Régimen de volatilidad
# ─────────────────────────────────────────────────────────────────────────────

class TestQ1VolatilityRegime:

    def test_low_regime_below_p33(self):
        assert classify_regime(0.001, p33=0.002, p66=0.004) == "LOW"

    def test_med_regime_between_percentiles(self):
        assert classify_regime(0.003, p33=0.002, p66=0.004) == "MED"

    def test_high_regime_above_p66(self):
        assert classify_regime(0.005, p33=0.002, p66=0.004) == "HIGH"

    def test_exactly_at_p33_is_low(self):
        assert classify_regime(0.002, p33=0.002, p66=0.004) == "LOW"

    def test_exactly_at_p66_is_med(self):
        assert classify_regime(0.004, p33=0.002, p66=0.004) == "MED"

    def test_none_volatility_is_unknown(self):
        assert classify_regime(None, p33=0.002, p66=0.004) == "UNKNOWN"

    def test_transition_detection_basic(self):
        regimes = ["LOW", "LOW", "HIGH", "HIGH", "MED"]
        transitions = regime_transition(regimes)
        assert transitions == [False, False, True, False, True]

    def test_no_transitions_in_constant_regime(self):
        regimes = ["MED"] * 5
        assert regime_transition(regimes) == [False, False, False, False, False]

    def test_first_element_never_transition(self):
        regimes = ["HIGH", "LOW"]
        assert regime_transition(regimes)[0] is False

    def test_every_change_detected(self):
        regimes = ["LOW", "MED", "HIGH", "LOW"]
        transitions = regime_transition(regimes)
        assert sum(transitions) == 3  # 3 cambios


# ─────────────────────────────────────────────────────────────────────────────
# Q2 — Spread vs liquidez
# ─────────────────────────────────────────────────────────────────────────────

class TestQ2SpreadLiquidity:

    def test_four_quartiles_produced(self):
        vols = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        spreads = [0.1] * 8
        profile = spread_quartile_profile(vols, spreads)
        assert set(profile.keys()) == {1, 2, 3, 4}

    def test_higher_volume_higher_spread(self):
        """Si el spread sube con el volumen, Q4 > Q1."""
        vols   = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        # Spread proporcional al volumen
        spreads = [v * 0.1 for v in vols]
        profile = spread_quartile_profile(vols, spreads)
        assert profile[4] > profile[1]

    def test_flat_spread_same_across_quartiles(self):
        vols   = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        spreads = [0.5] * 8
        profile = spread_quartile_profile(vols, spreads)
        for q in [1, 2, 3, 4]:
            assert profile[q] == pytest.approx(0.5)


# ─────────────────────────────────────────────────────────────────────────────
# Q3 — Divergencia de momentum cross-asset
# ─────────────────────────────────────────────────────────────────────────────

class TestQ3MomentumDivergence:

    def test_direction_positive_momentum(self):
        assert momentum_direction(0.05) == 1

    def test_direction_negative_momentum(self):
        assert momentum_direction(-0.03) == -1

    def test_direction_zero_momentum(self):
        assert momentum_direction(0.0) == 0

    def test_direction_none_is_neutral(self):
        assert momentum_direction(None) == 0

    def test_consensus_all_up(self):
        assert consensus_score([1, 1, 1]) == 3

    def test_consensus_all_down(self):
        assert consensus_score([-1, -1, -1]) == -3

    def test_maximum_divergence_score_zero(self):
        # BTC sube, ETH neutral, BNB baja
        assert consensus_score([1, 0, -1]) == 0

    def test_partial_divergence(self):
        assert consensus_score([1, 1, -1]) == 1

    def test_divergence_detected_when_not_full_consensus(self):
        score = consensus_score([1, -1, 1])
        assert abs(score) < 3  # divergencia


# ─────────────────────────────────────────────────────────────────────────────
# Q4 — Anomalías de volumen (Z-score)
# ─────────────────────────────────────────────────────────────────────────────

class TestQ4VolumeAnomalies:

    def test_zscore_mean_value_is_zero(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / (len(values) - 1))
        zscores = [volume_zscore(v, mean, std) for v in values]
        assert abs(sum(zscores)) < 1e-10  # la suma de z-scores es 0

    def test_anomaly_above_threshold(self):
        z = volume_zscore(volume=100.0, mean=10.0, std=5.0)
        assert z > 2.0  # (100-10)/5 = 18

    def test_normal_value_not_anomaly(self):
        z = volume_zscore(volume=11.0, mean=10.0, std=5.0)
        assert abs(z) <= 2.0  # (11-10)/5 = 0.2

    def test_zero_std_returns_zero(self):
        # Si todos los valores son iguales, z = 0 por convención
        assert volume_zscore(5.0, 5.0, 0.0) == 0.0

    def test_rolling_window_uses_correct_range(self):
        values = [1.0, 1.0, 1.0, 1.0, 10.0]
        idx = 4   # último elemento
        mean, std = rolling_mean_std(values, window=5, idx=idx)
        z = volume_zscore(values[idx], mean, std)
        # El 10.0 debe ser claramente anómalo respecto al resto
        assert z > 1.5

    def test_expected_5pct_anomaly_rate_under_normality(self):
        """Bajo distribución normal, ~5% de valores tienen |z| > 2."""
        import random
        random.seed(42)
        n = 10_000
        normal_values = [random.gauss(0, 1) for _ in range(n)]
        anomalies = sum(1 for v in normal_values if abs(v) > 2)
        rate = anomalies / n
        assert 0.04 < rate < 0.06   # ±1% del 5% teórico


# ─────────────────────────────────────────────────────────────────────────────
# Q5 — Latencia end-to-end
# ─────────────────────────────────────────────────────────────────────────────

class TestQ5PipelineLatency:

    def test_p50_is_median(self):
        latencies = [10, 20, 30, 40, 50]
        assert latency_percentile(latencies, 0.50) == 30

    def test_p99_is_near_max(self):
        latencies = list(range(1, 101))   # 1 a 100
        p99 = latency_percentile(latencies, 0.99)
        assert p99 >= 98

    def test_total_latency_is_sum_of_parts(self):
        # total = binance + network
        trade_time   = 1718000000000
        event_time   = 1718000000003   # +3ms binance
        ingestion_ts = 1718000000010   # +7ms network
        binance_lat = event_time   - trade_time    # 3ms
        network_lat = ingestion_ts - event_time    # 7ms
        total_lat   = ingestion_ts - trade_time    # 10ms
        assert total_lat == binance_lat + network_lat

    def test_latency_always_positive(self):
        # ingestion_ts siempre posterior a trade_time
        trade_time = 1718000000000
        for delay in [1, 10, 100, 1000]:
            ingestion_ts = trade_time + delay
            assert ingestion_ts - trade_time > 0


# ─────────────────────────────────────────────────────────────────────────────
# Q6 — Presión compradora acumulada
# ─────────────────────────────────────────────────────────────────────────────

class TestQ6BuyPressure:

    def test_pressure_positive_when_buyers_dominate(self):
        ratios = [1.5, 1.3, 1.8, 2.0, 1.1]   # todos > 1 → compradores
        pressure = cumulative_pressure(ratios, window=5, idx=4)
        assert pressure > 0

    def test_pressure_negative_when_sellers_dominate(self):
        ratios = [0.5, 0.7, 0.3, 0.8, 0.6]   # todos < 1 → vendedores
        pressure = cumulative_pressure(ratios, window=5, idx=4)
        assert pressure < 0

    def test_pressure_zero_at_equilibrium(self):
        # log(1.0) = 0 → sin presión neta
        ratios = [1.0] * 5
        pressure = cumulative_pressure(ratios, window=5, idx=4)
        assert pressure == pytest.approx(0.0)

    def test_rolling_window_respects_size(self):
        # Con window=3, solo debe usar los últimos 3 valores
        ratios = [100.0, 100.0, 1.2, 1.1, 1.3]   # primeros dos son irrelevantes
        pressure_w3  = cumulative_pressure(ratios, window=3, idx=4)
        pressure_w5  = cumulative_pressure(ratios, window=5, idx=4)
        assert pressure_w3 != pytest.approx(pressure_w5)

    def test_log_ratio_used_not_raw_ratio(self):
        """La presión usa log(ratio), no el ratio directo."""
        # Si fuera ratio directo: 2.0 + 0.5 = 2.5 (no simétrico)
        # Con log: log(2) + log(0.5) = log(1) = 0 (simétrico)
        ratios = [2.0, 0.5]
        pressure = cumulative_pressure(ratios, window=2, idx=1)
        assert pressure == pytest.approx(0.0, abs=1e-10)


# ─────────────────────────────────────────────────────────────────────────────
# Q7 — VWAP tracking error
# ─────────────────────────────────────────────────────────────────────────────

class TestQ7VwapTrackingError:

    def test_positive_error_when_close_above_vwap(self):
        error = tracking_error_pct(close=101.0, vwap=100.0)
        assert error == pytest.approx(1.0)

    def test_negative_error_when_close_below_vwap(self):
        error = tracking_error_pct(close=99.0, vwap=100.0)
        assert error == pytest.approx(-1.0)

    def test_zero_error_when_close_equals_vwap(self):
        error = tracking_error_pct(close=100.0, vwap=100.0)
        assert error == pytest.approx(0.0)

    def test_error_is_percentage(self):
        # Un 5% de diferencia debe dar exactamente 5.0
        error = tracking_error_pct(close=105.0, vwap=100.0)
        assert error == pytest.approx(5.0)

    def test_vwap_within_ohlcv_range(self):
        """El VWAP siempre debe estar entre el mínimo y máximo de la ventana."""
        prices = [100.0, 102.0, 98.0, 101.0, 99.0]
        quantities = [1.0, 2.0, 1.5, 0.5, 3.0]
        vwap = sum(p * q for p, q in zip(prices, quantities)) / sum(quantities)
        assert min(prices) <= vwap <= max(prices)

    def test_large_trades_pull_vwap(self):
        """Un trade grande tira el VWAP hacia su precio."""
        prices =    [100.0, 100.0, 200.0]
        quantities = [1.0,   1.0,  100.0]  # trade de 200 es 50x más grande
        vwap = sum(p * q for p, q in zip(prices, quantities)) / sum(quantities)
        # VWAP debe estar mucho más cerca de 200 que de 100
        assert vwap > 190.0

    def test_symmetric_tracking_error(self):
        """Si close alterna entre ±X% sobre VWAP, el error medio tiende a 0."""
        errors = [
            tracking_error_pct(101.0, 100.0),   # +1%
            tracking_error_pct(99.0,  100.0),   # -1%
        ]
        assert sum(errors) == pytest.approx(0.0)
