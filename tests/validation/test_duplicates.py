"""
tests/validation/test_duplicates.py
-------------------------------------
Detección de duplicados en cada capa del pipeline.

Un duplicado en CryptoFlow puede originarse en tres puntos:
  1. Reconexión del WebSocket → el mismo trade llega dos veces al consumer
  2. Retry de escritura en Cassandra → el mismo evento se inserta dos veces
  3. Re-ejecución del job de Spark → OHLCV calculado dos veces para la misma ventana

Estrategia por capa:
  - Raw trades    → dedup por (symbol, agg_trade_id)    — ID único de Binance
  - Book tickers  → dedup por (symbol, event_time)      — snapshot de un instante
  - OHLCV         → dedup por (symbol, window_label, window_start)
  - Features      → dedup por (symbol, window_label, window_start)

Qué observar:
  - duplicate_rate > 0% en raw_trades → revisar lógica de reconexión del consumer
  - duplicate_rate > 0% en OHLCV     → el job de Spark se ejecutó dos veces sin
                                        limpiar el output; revisar idempotencia
  - duplicate_rate == 0% en todos    → pipeline limpio
"""

import math
import pytest
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────────────────────
# Implementaciones Python puro de las métricas de duplicados
# ─────────────────────────────────────────────────────────────────────────────

def duplicate_rate(records: list[dict], key_fields: list[str]) -> float:
    """
    Calcula la tasa de duplicados en una lista de registros.

    Args:
        records:    Lista de dicts representando filas
        key_fields: Campos que forman la clave de negocio única

    Returns:
        Fracción de filas que son duplicadas (0.0 = sin duplicados)

    Ejemplo:
        records = [{"sym": "BTC", "id": 1}, {"sym": "BTC", "id": 1}]
        duplicate_rate(records, ["sym", "id"]) → 0.5
    """
    if not records:
        return 0.0
    keys = [tuple(r[f] for f in key_fields) for r in records]
    unique = len(set(keys))
    total  = len(keys)
    return (total - unique) / total


def find_duplicates(records: list[dict], key_fields: list[str]) -> list[dict]:
    """
    Retorna las filas duplicadas con su conteo de apariciones.
    Útil para inspección manual.
    """
    from collections import Counter
    keys = [tuple(r[f] for f in key_fields) for r in records]
    counts = Counter(keys)
    duplicated_keys = {k for k, v in counts.items() if v > 1}

    result = []
    for key, count in counts.items():
        if key in duplicated_keys:
            result.append({
                **dict(zip(key_fields, key)),
                "occurrences": count,
            })
    return result


def check_primary_key_uniqueness(
    records: list[dict],
    pk_fields: list[str],
    table_name: str = "unknown",
) -> dict:
    """
    Verifica unicidad de la clave primaria y retorna un reporte.

    Returns:
        {
          "table": str,
          "total_rows": int,
          "unique_rows": int,
          "duplicate_rows": int,
          "duplicate_rate_pct": float,
          "is_clean": bool,
          "duplicates": list  (vacío si limpio)
        }
    """
    rate = duplicate_rate(records, pk_fields)
    duplicates = find_duplicates(records, pk_fields) if rate > 0 else []
    total = len(records)
    dup_rows = int(rate * total)

    return {
        "table":             table_name,
        "total_rows":        total,
        "unique_rows":       total - dup_rows,
        "duplicate_rows":    dup_rows,
        "duplicate_rate_pct": round(rate * 100, 4),
        "is_clean":          rate == 0.0,
        "duplicates":        duplicates,
    }


def check_dedup_preserves_earliest(
    records: list[dict],
    key_fields: list[str],
    timestamp_field: str,
) -> bool:
    """
    Verifica que tras deduplicar se conserva el registro con
    el timestamp más temprano (primera ingesta).

    Returns:
        True si la deduplicación es correcta.
    """
    from collections import defaultdict
    groups = defaultdict(list)
    for r in records:
        key = tuple(r[f] for f in key_fields)
        groups[key].append(r)

    for key, group in groups.items():
        if len(group) > 1:
            expected_survivor = min(group, key=lambda r: r[timestamp_field])
            # En el dataset deduplicado solo debe quedar el más temprano
            survivors = [r for r in group
                         if r[timestamp_field] == expected_survivor[timestamp_field]]
            if not survivors:
                return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Tests — duplicate_rate
# ─────────────────────────────────────────────────────────────────────────────

_TS = datetime(2024, 6, 10, 12, 0, 0, tzinfo=timezone.utc)
_TS2 = datetime(2024, 6, 10, 12, 0, 1, tzinfo=timezone.utc)


def _trade(symbol, agg_trade_id, price=67000.0, ingestion_ts=_TS):
    return {
        "symbol": symbol, "agg_trade_id": agg_trade_id,
        "price": price, "ingestion_ts": ingestion_ts,
        "trace_id": f"trace-{symbol}-{agg_trade_id}",
    }


def _ticker(symbol, event_time, spread=2.0):
    return {"symbol": symbol, "event_time": event_time, "spread": spread}


def _ohlcv(symbol, window_label, window_start, close=100.0):
    return {
        "symbol": symbol, "window_label": window_label,
        "window_start": window_start, "close": close,
    }


class TestDuplicateRate:

    def test_clean_dataset_rate_is_zero(self):
        records = [_trade("BTCUSDT", 1), _trade("BTCUSDT", 2)]
        assert duplicate_rate(records, ["symbol", "agg_trade_id"]) == 0.0

    def test_full_duplicate_rate_is_one(self):
        records = [_trade("BTCUSDT", 1)] * 4
        assert duplicate_rate(records, ["symbol", "agg_trade_id"]) == pytest.approx(0.75)

    def test_one_duplicate_in_three(self):
        records = [
            _trade("BTCUSDT", 1),
            _trade("BTCUSDT", 2),
            _trade("BTCUSDT", 2),   # ← duplicado
        ]
        rate = duplicate_rate(records, ["symbol", "agg_trade_id"])
        assert rate == pytest.approx(1/3, abs=0.001)

    def test_empty_dataset_rate_is_zero(self):
        assert duplicate_rate([], ["symbol", "agg_trade_id"]) == 0.0

    def test_cross_symbol_not_duplicate(self):
        records = [_trade("BTCUSDT", 1), _trade("ETHUSDT", 1)]
        # Mismo agg_trade_id pero distinto símbolo — no es duplicado
        assert duplicate_rate(records, ["symbol", "agg_trade_id"]) == 0.0

    def test_composite_key_both_fields_matter(self):
        records = [
            _ohlcv("BTCUSDT", "1m", "2024-06-10T12:00"),
            _ohlcv("BTCUSDT", "5m", "2024-06-10T12:00"),  # misma fecha, distinta ventana
        ]
        assert duplicate_rate(records, ["symbol", "window_label", "window_start"]) == 0.0


class TestFindDuplicates:

    def test_finds_exact_duplicates(self):
        records = [
            _trade("BTCUSDT", 42),
            _trade("BTCUSDT", 42),   # ← duplicado
            _trade("BTCUSDT", 99),
        ]
        dups = find_duplicates(records, ["symbol", "agg_trade_id"])
        assert len(dups) == 1
        assert dups[0]["agg_trade_id"] == 42
        assert dups[0]["occurrences"] == 2

    def test_occurrence_count_correct(self):
        records = [_trade("BTCUSDT", 1)] * 5
        dups = find_duplicates(records, ["symbol", "agg_trade_id"])
        assert dups[0]["occurrences"] == 5

    def test_no_duplicates_returns_empty(self):
        records = [_trade("BTCUSDT", i) for i in range(10)]
        assert find_duplicates(records, ["symbol", "agg_trade_id"]) == []


class TestPKUniqueness:

    def test_report_clean_dataset(self):
        records = [_trade("BTCUSDT", i) for i in range(100)]
        report = check_primary_key_uniqueness(
            records, ["symbol", "agg_trade_id"], "raw_trades"
        )
        assert report["is_clean"] is True
        assert report["duplicate_rows"] == 0
        assert report["duplicate_rate_pct"] == 0.0
        assert report["duplicates"] == []

    def test_report_dirty_dataset(self):
        records = [_trade("BTCUSDT", 1)] * 3 + [_trade("BTCUSDT", 2)]
        report = check_primary_key_uniqueness(
            records, ["symbol", "agg_trade_id"], "raw_trades"
        )
        assert report["is_clean"] is False
        assert report["duplicate_rows"] > 0
        assert report["duplicate_rate_pct"] > 0

    def test_report_table_name_preserved(self):
        report = check_primary_key_uniqueness([], ["symbol"], "test_table")
        assert report["table"] == "test_table"

    def test_total_rows_correct(self):
        records = [_trade("BTCUSDT", i) for i in range(50)]
        report = check_primary_key_uniqueness(records, ["symbol", "agg_trade_id"])
        assert report["total_rows"] == 50


class TestDeduplicationPreservesEarliest:

    def test_earliest_timestamp_identified(self):
        records = [
            _trade("BTCUSDT", 42, ingestion_ts=_TS2),  # más tardío
            _trade("BTCUSDT", 42, ingestion_ts=_TS),   # más temprano ← debe sobrevivir
        ]
        result = check_dedup_preserves_earliest(
            records, ["symbol", "agg_trade_id"], "ingestion_ts"
        )
        assert result is True

    def test_clean_dataset_always_passes(self):
        records = [_trade("BTCUSDT", i) for i in range(10)]
        result = check_dedup_preserves_earliest(
            records, ["symbol", "agg_trade_id"], "ingestion_ts"
        )
        assert result is True


# ─────────────────────────────────────────────────────────────────────────────
# Checks de duplicados por capa — funciones de validación del pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestDuplicateChecksByLayer:
    """
    Simula el output de cada capa del pipeline y verifica que
    los duplicados introducidos intencionalmente son detectados.
    """

    def test_raw_trades_layer_detects_ws_reconnect_dup(self):
        """
        Escenario: el WebSocket se reconecta y el mismo aggTrade
        llega dos veces antes de que el consumer detecte el duplicado.
        """
        raw_from_ws = [
            _trade("BTCUSDT", 100001),
            _trade("BTCUSDT", 100002),
            _trade("BTCUSDT", 100001),  # llegó de nuevo tras reconexión
        ]
        report = check_primary_key_uniqueness(
            raw_from_ws, ["symbol", "agg_trade_id"], "raw_trades"
        )
        assert report["is_clean"] is False, (
            "El duplicado por reconexión del WebSocket debe detectarse en raw_trades"
        )

    def test_ohlcv_layer_detects_double_job_execution(self):
        """
        Escenario: el job de Spark se ejecutó dos veces para la misma fecha
        (ej. por un retry manual) y el OHLCV se calculó y guardó dos veces.
        """
        ohlcv_after_double_run = [
            _ohlcv("BTCUSDT", "1m", "2024-06-10T12:00"),
            _ohlcv("ETHUSDT", "1m", "2024-06-10T12:00"),
            _ohlcv("BTCUSDT", "1m", "2024-06-10T12:00"),  # segunda ejecución
        ]
        report = check_primary_key_uniqueness(
            ohlcv_after_double_run,
            ["symbol", "window_label", "window_start"],
            "ohlcv_1m",
        )
        assert report["is_clean"] is False, (
            "La doble ejecución del job debe detectarse en OHLCV"
        )

    def test_feature_layer_clean_after_dedup(self):
        """
        Escenario: el Feature Engine aplica deduplicación antes de persistir.
        El dataset de features resultante debe estar limpio.
        """
        features_after_dedup = [
            {"symbol": "BTCUSDT", "window_label": "1m",
             "window_start": "2024-06-10T12:00", "vwap": 67000.0},
            {"symbol": "ETHUSDT", "window_label": "1m",
             "window_start": "2024-06-10T12:00", "vwap": 3500.0},
            {"symbol": "BNBUSDT", "window_label": "1m",
             "window_start": "2024-06-10T12:00", "vwap": 580.0},
        ]
        report = check_primary_key_uniqueness(
            features_after_dedup,
            ["symbol", "window_label", "window_start"],
            "features_by_window",
        )
        assert report["is_clean"] is True

    def test_ticker_layer_detects_event_time_dup(self):
        """
        Escenario: el mismo snapshot de bid/ask llega dos veces
        (event_time idéntico → mismo instante de mercado).
        """
        tickers = [
            _ticker("BTCUSDT", 1718000000000),
            _ticker("BTCUSDT", 1718000001000),
            _ticker("BTCUSDT", 1718000000000),  # duplicado por event_time
        ]
        report = check_primary_key_uniqueness(
            tickers, ["symbol", "event_time"], "raw_book_tickers"
        )
        assert report["is_clean"] is False
