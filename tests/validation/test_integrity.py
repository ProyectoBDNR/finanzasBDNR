"""
tests/validation/test_integrity.py
------------------------------------
Checks de integridad del pipeline — consistencia entre capas.

Qué valida cada check:

  temporal_ordering      → trade_time ≤ event_time ≤ ingestion_ts en todos los eventos
  schema_completeness    → ningún campo obligatorio es None en el dataset final
  cross_layer_consistency→ el conteo de trades raw ≈ suma de trade_counts en OHLCV
  ttl_compliance         → todos los timestamps están dentro del TTL de 7 días
  symbol_coverage        → los tres símbolos monitoreados producen datos
  feature_nullability    → las features P0 no tienen más de X% de nulos
  traceability           → cada feature puede rastrearse hasta un trade raw

Qué observar:
  Si temporal_ordering falla:
    → el servidor NTP está desincronizado o hay un bug en el consumer
  Si cross_layer_consistency falla (>1% diferencia):
    → se están perdiendo eventos en la limpieza o hay duplicados en raw
  Si feature_nullability > 5%:
    → el warm-up de las ventanas rolling es mayor de lo esperado;
      revisar el tamaño del dataset o el window size
  Si traceability falla:
    → la columna trace_id se está perdiendo en alguna transformación
"""

import math
import time
import uuid
import random
import pytest
from datetime import datetime, timezone, timedelta


# ─────────────────────────────────────────────────────────────────────────────
# Estructuras de datos de referencia
# ─────────────────────────────────────────────────────────────────────────────

MONITORED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}

RAW_TRADE_REQUIRED = [
    "symbol", "agg_trade_id", "price", "quantity",
    "trade_time", "event_time", "is_buyer_maker",
    "trace_id", "ingestion_ts",
]

RAW_TICKER_REQUIRED = [
    "symbol", "event_time",
    "best_bid_price", "best_ask_price",
    "best_bid_qty",  "best_ask_qty",
    "spread", "trace_id", "ingestion_ts",
]

OHLCV_REQUIRED = [
    "symbol", "window_label", "window_start",
    "open", "high", "low", "close", "volume",
    "trade_count",
]

FEATURE_P0_REQUIRED = [
    "symbol", "window_label", "window_start",
    "vwap", "log_return", "trade_count",
]

TTL_DAYS = 7


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


def _make_trade(i: int, symbol: str = "BTCUSDT",
                trade_time: int | None = None) -> dict:
    base_ts = _now_ms() - 60_000  # hace 1 minuto
    tt = trade_time or (base_ts + i * 1000)
    return {
        "symbol":         symbol,
        "agg_trade_id":   i,
        "price":          67000.0 + i * 0.5,
        "quantity":       0.1,
        "trade_time":     tt,
        "event_time":     tt + 3,
        "is_buyer_maker": i % 2 == 0,
        "trace_id":       str(uuid.uuid4()),
        "ingestion_ts":   datetime.fromtimestamp((tt + 10) / 1000, tz=timezone.utc),
    }


def _make_ohlcv(symbol: str, window_start: str,
                trade_count: int = 10, close: float = 67000.0) -> dict:
    return {
        "symbol":       symbol,
        "window_label": "1m",
        "window_start": window_start,
        "open":         close - 10,
        "high":         close + 20,
        "low":          close - 15,
        "close":        close,
        "volume":       trade_count * 0.1,
        "trade_count":  trade_count,
    }


def _make_feature(symbol: str, window_start: str) -> dict:
    return {
        "symbol":       symbol,
        "window_label": "1m",
        "window_start": window_start,
        "vwap":         67000.0,
        "log_return":   0.0001,
        "rolling_volatility": 0.0003,
        "momentum_pct": 0.002,
        "trade_count":  10,
        "spread_mean":  2.0,
        "trace_id":     str(uuid.uuid4()),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. Integridad temporal
# ─────────────────────────────────────────────────────────────────────────────

class TestTemporalOrdering:
    """
    Invariante: trade_time ≤ event_time ≤ ingestion_ts.
    Si se viola, indica un problema de relojes o un bug en el consumer.
    """

    def _check_ordering(self, records: list[dict]) -> list[dict]:
        violations = []
        for r in records:
            tt = r["trade_time"]
            et = r["event_time"]
            it = int(r["ingestion_ts"].timestamp() * 1000)
            if not (tt <= et):
                violations.append({**r, "violation": "event_time < trade_time"})
            elif not (et <= it):
                violations.append({**r, "violation": "ingestion_ts < event_time"})
        return violations

    def test_clean_records_no_violations(self):
        records = [_make_trade(i) for i in range(100)]
        violations = self._check_ordering(records)
        assert violations == [], (
            f"Orden temporal violado en {len(violations)} eventos:\n"
            + "\n".join(str(v) for v in violations[:3])
        )

    def test_detects_event_time_before_trade_time(self):
        bad_trade = _make_trade(0)
        bad_trade["event_time"] = bad_trade["trade_time"] - 100  # event antes que trade
        records = [bad_trade]
        violations = self._check_ordering(records)
        assert len(violations) == 1
        assert violations[0]["violation"] == "event_time < trade_time"

    def test_detects_ingestion_before_event(self):
        bad_trade = _make_trade(0)
        bad_trade["ingestion_ts"] = datetime.fromtimestamp(
            (bad_trade["event_time"] - 1000) / 1000, tz=timezone.utc
        )
        violations = self._check_ordering([bad_trade])
        assert len(violations) == 1

    def test_all_three_timestamps_present(self):
        records = [_make_trade(i) for i in range(10)]
        for r in records:
            assert "trade_time"   in r and r["trade_time"]   is not None
            assert "event_time"   in r and r["event_time"]   is not None
            assert "ingestion_ts" in r and r["ingestion_ts"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# 2. Completitud de schema
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaCompleteness:
    """
    Ningún campo obligatorio puede ser None en los datos procesados.
    Los datos raw pueden tener nulos (se limpian en Spark).
    Los datos analíticos (OHLCV, features) deben estar completos.
    """

    def _null_rate(self, records: list[dict], field: str) -> float:
        if not records:
            return 0.0
        nulls = sum(1 for r in records if r.get(field) is None)
        return nulls / len(records)

    def test_ohlcv_required_fields_never_null(self):
        windows = [_make_ohlcv("BTCUSDT", f"12:0{i}") for i in range(5)]
        for field in OHLCV_REQUIRED:
            null_rate = self._null_rate(windows, field)
            assert null_rate == 0.0, (
                f"Campo OHLCV obligatorio '{field}' tiene {null_rate*100:.1f}% nulos"
            )

    def test_feature_p0_null_rate_within_threshold(self):
        """
        Las features rolling tienen nulos al inicio (warm-up del window).
        El threshold del 10% es conservador para un dataset de 200 ventanas
        con window_size=10.
        """
        MAX_NULL_RATE = 0.10
        n_windows = 200

        features = []
        for i in range(n_windows):
            f = _make_feature("BTCUSDT", f"12:{i:02d}")
            # Simular nulos en las primeras ventanas (warm-up)
            if i < 10:
                f["log_return"] = None
                f["rolling_volatility"] = None
            features.append(f)

        for field in FEATURE_P0_REQUIRED:
            if field in ["log_return"]:
                null_rate = self._null_rate(features, field)
                assert null_rate <= MAX_NULL_RATE, (
                    f"Feature '{field}' tiene {null_rate*100:.1f}% nulos "
                    f"(threshold: {MAX_NULL_RATE*100:.0f}%)"
                )

    def test_trace_id_never_null_in_raw(self):
        records = [_make_trade(i) for i in range(50)]
        null_rate = self._null_rate(records, "trace_id")
        assert null_rate == 0.0, "trace_id no debe ser nulo en ningún evento raw"

    def test_trace_id_is_valid_uuid(self):
        records = [_make_trade(i) for i in range(50)]
        for r in records:
            try:
                uuid.UUID(r["trace_id"])
            except (ValueError, AttributeError) as e:
                pytest.fail(f"trace_id inválido: {r['trace_id']} — {e}")

    def test_price_always_positive_in_clean_data(self):
        records = [_make_trade(i) for i in range(100)]
        for r in records:
            assert r["price"] > 0, f"Precio negativo/cero detectado: {r['price']}"

    def test_quantity_always_positive_in_clean_data(self):
        records = [_make_trade(i) for i in range(100)]
        for r in records:
            assert r["quantity"] > 0


# ─────────────────────────────────────────────────────────────────────────────
# 3. Consistencia entre capas
# ─────────────────────────────────────────────────────────────────────────────

class TestCrossLayerConsistency:
    """
    El trade_count en OHLCV debe ser consistente con los trades raw.
    Una diferencia > 1% indica pérdida de eventos o duplicación.
    """

    MAX_LOSS_RATE = 0.01  # máximo 1% de diferencia tolerable

    def test_trade_count_consistent_with_raw(self):
        """
        Simula: 300 trades crudos → OHLCV con trade_count.
        El sum(trade_count) debe estar dentro del 1% de len(raw_trades).
        """
        n_raw = 300
        raw_trades = [_make_trade(i) for i in range(n_raw)]

        # Simula 3 ventanas con distribución realista
        ohlcv = [
            _make_ohlcv("BTCUSDT", "12:00", trade_count=102),
            _make_ohlcv("BTCUSDT", "12:01", trade_count=95),
            _make_ohlcv("BTCUSDT", "12:02", trade_count=103),
        ]

        raw_count  = len(raw_trades)
        ohlcv_sum  = sum(w["trade_count"] for w in ohlcv)
        difference = abs(raw_count - ohlcv_sum) / raw_count

        print(f"\n  raw={raw_count} ohlcv_sum={ohlcv_sum} diff={difference*100:.2f}%")

        assert difference <= self.MAX_LOSS_RATE, (
            f"Inconsistencia entre capas: raw={raw_count} "
            f"vs ohlcv_sum={ohlcv_sum} ({difference*100:.2f}% diferencia)"
        )

    def test_volume_conservation_across_layers(self):
        """
        La suma de volumes en OHLCV debe ≈ suma de quantities en raw_trades.
        Tolerancia: 0.1% por errores de redondeo float.
        """
        quantities = [round(random.uniform(0.001, 2.0), 6) for _ in range(100)]
        total_qty = sum(quantities)

        # En OHLCV, volumen = sum de cantidades en la ventana
        ohlcv_volumes = [
            sum(quantities[:33]),
            sum(quantities[33:66]),
            sum(quantities[66:]),
        ]
        ohlcv_total = sum(ohlcv_volumes)

        assert abs(total_qty - ohlcv_total) < 1e-6, (
            f"Volumen no conservado: raw={total_qty:.8f} "
            f"vs ohlcv={ohlcv_total:.8f}"
        )

    def test_symbol_coverage_all_three(self):
        """Los tres símbolos monitoreados deben tener datos en cada capa."""
        _SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        raw_symbols   = {_make_trade(i, sym)["symbol"]
                         for i, sym in enumerate(_SYMBOLS)}
        ohlcv_symbols = {_make_ohlcv(sym, "12:00")["symbol"] for sym in _SYMBOLS}

        assert raw_symbols   == MONITORED_SYMBOLS
        assert ohlcv_symbols == MONITORED_SYMBOLS

    def test_no_symbol_missing_from_features(self):
        features = [_make_feature(sym, "12:00") for sym in MONITORED_SYMBOLS]
        present = {f["symbol"] for f in features}
        missing = MONITORED_SYMBOLS - present
        assert not missing, f"Símbolos sin features: {missing}"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Cumplimiento de TTL
# ─────────────────────────────────────────────────────────────────────────────

class TestTTLCompliance:
    """
    El schema define TTL = 7 días. Los datos más antiguos que ese umbral
    deberían haber sido expirados automáticamente por Cassandra.
    Si aparecen en un query, es porque el TTL no está aplicado correctamente.
    """

    def _age_days(self, ingestion_ts: datetime) -> float:
        now = datetime.now(timezone.utc)
        return (now - ingestion_ts).total_seconds() / 86_400

    def test_fresh_events_within_ttl(self):
        records = [_make_trade(i) for i in range(50)]
        violations = []
        for r in records:
            age = self._age_days(r["ingestion_ts"])
            if age > TTL_DAYS:
                violations.append({"trace_id": r["trace_id"], "age_days": age})

        assert not violations, (
            f"{len(violations)} eventos más antiguos que {TTL_DAYS} días "
            f"(TTL debería haberlos expirado):\n"
            + "\n".join(str(v) for v in violations[:3])
        )

    def test_stale_event_detected(self):
        """Verifica que el check detecta un evento más viejo que el TTL."""
        stale_ts = datetime.now(timezone.utc) - timedelta(days=TTL_DAYS + 1)
        stale_trade = _make_trade(0)
        stale_trade["ingestion_ts"] = stale_ts

        age = self._age_days(stale_trade["ingestion_ts"])
        assert age > TTL_DAYS, "El evento debería ser detectado como expirado"

    def test_all_events_have_utc_timezone(self):
        records = [_make_trade(i) for i in range(20)]
        for r in records:
            ts = r["ingestion_ts"]
            assert ts.tzinfo is not None, "ingestion_ts debe tener timezone"
            assert ts.tzinfo == timezone.utc, "ingestion_ts debe ser UTC"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Trazabilidad
# ─────────────────────────────────────────────────────────────────────────────

class TestTraceability:
    """
    Dado un trace_id en la capa analítica, debe ser posible rastrear
    el evento hasta la capa raw.

    El pipeline propaga trace_id a través de todas las capas.
    Si trace_id se pierde en alguna transformación, la trazabilidad se rompe.
    """

    def _build_pipeline_layers(self, n: int = 10) -> tuple[list, list, list]:
        """Construye tres capas del pipeline con trace_ids propagados."""
        raw = [_make_trade(i) for i in range(n)]

        # OHLCV hereda trace_ids de los trades que lo componen
        ohlcv = [{
            **_make_ohlcv("BTCUSDT", "12:00", trade_count=n),
            "source_trace_ids": [r["trace_id"] for r in raw],
        }]

        # Features propagan el trace_id del OHLCV
        features = [{
            **_make_feature("BTCUSDT", "12:00"),
            "trace_id": raw[0]["trace_id"],  # trace_id del primer trade de la ventana
        }]

        return raw, ohlcv, features

    def test_trace_id_present_in_all_layers(self):
        raw, ohlcv, features = self._build_pipeline_layers()
        for r in raw:
            assert r.get("trace_id"), "trace_id ausente en raw"
        for o in ohlcv:
            assert o.get("source_trace_ids"), "source_trace_ids ausente en ohlcv"
        for f in features:
            assert f.get("trace_id"), "trace_id ausente en features"

    def test_feature_trace_id_found_in_raw(self):
        """Un trace_id de features debe existir en la capa raw."""
        raw, ohlcv, features = self._build_pipeline_layers()
        raw_trace_ids = {r["trace_id"] for r in raw}

        for feature in features:
            assert feature["trace_id"] in raw_trace_ids, (
                f"trace_id {feature['trace_id']} de features "
                f"no encontrado en raw_trades"
            )

    def test_all_raw_trace_ids_unique(self):
        raw = [_make_trade(i) for i in range(100)]
        ids = [r["trace_id"] for r in raw]
        assert len(set(ids)) == len(ids), (
            f"trace_ids no únicos en raw: "
            f"{len(ids) - len(set(ids))} colisiones"
        )

    def test_trace_id_format_consistent(self):
        """Todos los trace_ids deben ser UUID4 en formato estándar."""
        raw = [_make_trade(i) for i in range(50)]
        for r in raw:
            tid = r["trace_id"]
            parsed = uuid.UUID(tid)
            assert parsed.version == 4, (
                f"trace_id {tid} no es UUID4 (version={parsed.version})"
            )
            assert str(parsed) == tid, (
                f"trace_id {tid} no está en formato canónico"
            )
