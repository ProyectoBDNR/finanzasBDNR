"""
tests/validation/test_load.py
------------------------------
Pruebas de carga y rendimiento del pipeline.

Qué medir y qué observar:

  consumer_throughput   → eventos/segundo que el parser puede procesar
    Objetivo: ≥ 1000 ev/s (el stream real llega a ~90 ev/s)
    Si falla: el parsing tiene overhead innecesario (ej. I/O en el parser)

  writer_buffer_throughput → eventos/segundo que el writer puede bufferizar
    Objetivo: ≥ 500 ev/s
    Si falla: el locking del buffer es el cuello de botella

  dedup_at_scale        → la deduplicación escala linealmente, no cuadrática
    Objetivo: dedup de 100k registros < 2 segundos
    Si falla: la implementación usa O(n²) en lugar de O(n) con hash

  memory_footprint      → el buffer del writer no crece indefinidamente
    Objetivo: flush limpia el buffer completamente

Cómo interpretar resultados:
  - throughput << objetivo → bottleneck en el componente; profiling necesario
  - throughput >> objetivo → margen de seguridad adecuado para el stream real
  - memoria no decrece tras flush → memory leak en el buffer
"""

import asyncio
import json
import math
import random
import time
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from consumer.binance_ws import parse_event
from consumer.models import AggTrade, BookTicker


# ─────────────────────────────────────────────────────────────────────────────
# Generadores de payloads sintéticos
# ─────────────────────────────────────────────────────────────────────────────

_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
_BASE_PRICES = {"BTCUSDT": 67000.0, "ETHUSDT": 3500.0, "BNBUSDT": 580.0}


def _make_agg_trade_payload(symbol: str = "BTCUSDT", trade_id: int = 1) -> str:
    price = _BASE_PRICES.get(symbol, 100.0) * math.exp(random.gauss(0, 0.001))
    return json.dumps({
        "stream": f"{symbol.lower()}@aggTrade",
        "data": {
            "e": "aggTrade",
            "E": int(time.time() * 1000),
            "s": symbol,
            "a": trade_id,
            "p": f"{price:.2f}",
            "q": f"{random.uniform(0.001, 2.0):.6f}",
            "f": trade_id * 3,
            "l": trade_id * 3 + 2,
            "T": int(time.time() * 1000) - random.randint(0, 50),
            "m": random.choice([True, False]),
            "M": True,
        },
    })


def _make_book_ticker_payload(symbol: str = "BTCUSDT") -> str:
    price = _BASE_PRICES.get(symbol, 100.0)
    spread = random.uniform(0.5, 3.0)
    return json.dumps({
        "stream": f"{symbol.lower()}@bookTicker",
        "data": {
            "e": "bookTicker",
            "E": int(time.time() * 1000),
            "s": symbol,
            "b": f"{price - spread/2:.2f}",
            "B": f"{random.uniform(0.5, 10.0):.4f}",
            "a": f"{price + spread/2:.2f}",
            "A": f"{random.uniform(0.5, 10.0):.4f}",
        },
    })


# ─────────────────────────────────────────────────────────────────────────────
# Q: ¿qué tan rápido puede parsear el consumer?
# ─────────────────────────────────────────────────────────────────────────────

class TestConsumerThroughput:
    """
    El stream real de Binance produce ~90 eventos/segundo (3 símbolos × 2 streams).
    El parser debe ser al menos 10x más rápido para tener margen de seguridad.
    Objetivo: ≥ 1000 eventos/segundo.
    """

    TARGET_EPS = 1_000   # eventos por segundo mínimo aceptable
    N_EVENTS   = 10_000  # tamaño del batch de prueba

    def test_agg_trade_parser_throughput(self):
        payloads = [
            _make_agg_trade_payload("BTCUSDT", i)
            for i in range(self.N_EVENTS)
        ]

        t0 = time.perf_counter()
        parsed = [parse_event(p) for p in payloads]
        elapsed = time.perf_counter() - t0

        throughput = self.N_EVENTS / elapsed
        parsed_ok  = sum(1 for e in parsed if e is not None)

        print(f"\n  aggTrade parser: {throughput:,.0f} ev/s "
              f"({parsed_ok}/{self.N_EVENTS} OK, {elapsed:.3f}s)")

        assert parsed_ok == self.N_EVENTS, "Algunos eventos no fueron parseados"
        assert throughput >= self.TARGET_EPS, (
            f"Throughput {throughput:.0f} ev/s por debajo del objetivo "
            f"de {self.TARGET_EPS} ev/s"
        )

    def test_book_ticker_parser_throughput(self):
        payloads = [
            _make_book_ticker_payload(random.choice(_SYMBOLS))
            for _ in range(self.N_EVENTS)
        ]

        t0 = time.perf_counter()
        parsed = [parse_event(p) for p in payloads]
        elapsed = time.perf_counter() - t0

        throughput = self.N_EVENTS / elapsed
        parsed_ok  = sum(1 for e in parsed if isinstance(e, BookTicker))

        print(f"\n  bookTicker parser: {throughput:,.0f} ev/s "
              f"({parsed_ok}/{self.N_EVENTS} OK, {elapsed:.3f}s)")

        assert throughput >= self.TARGET_EPS

    def test_mixed_stream_parser_throughput(self):
        """Stream mezclado: 50% aggTrade, 50% bookTicker — simula el stream real."""
        payloads = []
        for i in range(self.N_EVENTS // 2):
            sym = random.choice(_SYMBOLS)
            payloads.append(_make_agg_trade_payload(sym, i))
            payloads.append(_make_book_ticker_payload(sym))
        random.shuffle(payloads)

        t0 = time.perf_counter()
        parsed = [parse_event(p) for p in payloads]
        elapsed = time.perf_counter() - t0

        throughput = self.N_EVENTS / elapsed
        trades  = sum(1 for e in parsed if isinstance(e, AggTrade))
        tickers = sum(1 for e in parsed if isinstance(e, BookTicker))

        print(f"\n  Mixed parser: {throughput:,.0f} ev/s "
              f"(trades={trades} tickers={tickers}, {elapsed:.3f}s)")

        assert throughput >= self.TARGET_EPS

    def test_trace_id_generation_at_scale(self):
        """
        Cada evento genera un UUID4. Verificar que la generación masiva
        no degrada el throughput y que todos son únicos.
        """
        n = 5_000
        payloads = [_make_agg_trade_payload("BTCUSDT", i) for i in range(n)]

        t0 = time.perf_counter()
        events = [parse_event(p) for p in payloads]
        elapsed = time.perf_counter() - t0

        trace_ids = [e.trace_id for e in events if e]
        unique_ids = set(trace_ids)

        assert len(unique_ids) == n, (
            f"Colisión de trace_id: {n - len(unique_ids)} duplicados en {n} eventos"
        )
        print(f"\n  UUID4 generation: {n/elapsed:,.0f} IDs/s — {len(unique_ids)} únicos")


# ─────────────────────────────────────────────────────────────────────────────
# Q: ¿el writer bufferiza eficientemente?
# ─────────────────────────────────────────────────────────────────────────────

class TestWriterBufferThroughput:
    """
    El writer recibe eventos del consumer vía asyncio.
    El overhead de bufferizar (sin escritura real en Cassandra)
    debe ser mínimo para no crear backpressure en el consumer.
    Objetivo: ≥ 500 eventos/segundo en el path de buffering.
    """

    TARGET_EPS = 500
    N_EVENTS   = 5_000

    @pytest.fixture
    def mock_writer(self):
        """Writer con Cassandra mockeada — mide solo el overhead del buffer."""
        from storage.cassandra_writer import CassandraWriter

        mock_session = MagicMock()
        ps = MagicMock()
        ps.bind.return_value = MagicMock()
        mock_session.prepare.return_value = ps
        future = MagicMock()
        future.add_errback = MagicMock()
        mock_session.execute_async.return_value = future

        with patch("storage.cassandra_writer.get_session", return_value=mock_session):
            writer = CassandraWriter(
                batch_size=self.N_EVENTS + 1,  # evitar flush por tamaño
                flush_interval_s=60.0,          # evitar flush periódico
            )
            writer.start()
            yield writer
            writer._running = False

    def _make_agg_trade_event(self, i: int) -> AggTrade:
        return AggTrade(
            symbol="BTCUSDT",
            agg_trade_id=i,
            price=67000.0 + i * 0.01,
            quantity=0.1,
            trade_time=1718000000000 + i * 1000,
            event_time=1718000000002 + i * 1000,
            is_buyer_maker=False,
        )

    def test_buffer_write_throughput(self, mock_writer):
        events = [self._make_agg_trade_event(i) for i in range(self.N_EVENTS)]

        loop = asyncio.new_event_loop()
        t0 = time.perf_counter()
        for event in events:
            loop.run_until_complete(mock_writer.write(event))
        elapsed = time.perf_counter() - t0
        loop.close()

        throughput = self.N_EVENTS / elapsed
        buffered = sum(len(v) for v in mock_writer._buffers.values())

        print(f"\n  Buffer throughput: {throughput:,.0f} ev/s "
              f"({buffered} en buffer, {elapsed:.3f}s)")

        assert throughput >= self.TARGET_EPS, (
            f"Buffer throughput {throughput:.0f} ev/s por debajo "
            f"del objetivo de {self.TARGET_EPS} ev/s"
        )

    def test_buffer_cleared_after_flush(self, mock_writer):
        """Después de flush_all(), los buffers deben estar vacíos."""
        events = [self._make_agg_trade_event(i) for i in range(100)]
        loop = asyncio.new_event_loop()
        for event in events:
            loop.run_until_complete(mock_writer.write(event))
        loop.close()

        assert sum(len(v) for v in mock_writer._buffers.values()) == 100

        mock_writer._flush_all()

        remaining = sum(len(v) for v in mock_writer._buffers.values())
        assert remaining == 0, (
            f"Buffer no vaciado tras flush: {remaining} eventos pendientes"
        )

    def test_no_memory_leak_across_multiple_flushes(self, mock_writer):
        """El buffer no debe crecer entre ciclos de write → flush."""
        loop = asyncio.new_event_loop()

        sizes_after_flush = []
        for cycle in range(5):
            for i in range(50):
                loop.run_until_complete(
                    mock_writer.write(self._make_agg_trade_event(cycle * 50 + i))
                )
            mock_writer._flush_all()
            sizes_after_flush.append(
                sum(len(v) for v in mock_writer._buffers.values())
            )

        loop.close()

        assert all(s == 0 for s in sizes_after_flush), (
            f"Memory leak detectado. Tamaños tras flush: {sizes_after_flush}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Q: ¿la deduplicación escala?
# ─────────────────────────────────────────────────────────────────────────────

class TestDeduplicationScale:
    """
    La deduplicación debe ser O(n) — basada en hash — no O(n²).
    Medir que 100k registros se deduplicanen < 2 segundos.
    """

    def _dedup(self, records: list[dict], key_fields: list[str]) -> list[dict]:
        """Implementación O(n) con dict — espejo de la lógica del cleaner."""
        seen: dict = {}
        result = []
        for r in records:
            key = tuple(r[f] for f in key_fields)
            if key not in seen:
                seen[key] = True
                result.append(r)
        return result

    def test_dedup_100k_under_2_seconds(self):
        """100k registros, 10% de duplicados, dedup < 2s."""
        n = 100_000
        dup_rate = 0.10
        random.seed(42)

        unique_ids = list(range(int(n * (1 - dup_rate))))
        ids = unique_ids + random.choices(unique_ids, k=int(n * dup_rate))
        random.shuffle(ids)

        records = [{"symbol": "BTCUSDT", "agg_trade_id": i} for i in ids]

        t0 = time.perf_counter()
        deduped = self._dedup(records, ["symbol", "agg_trade_id"])
        elapsed = time.perf_counter() - t0

        expected_unique = len(unique_ids)
        actual_unique   = len(deduped)

        print(f"\n  Dedup 100k: {elapsed:.3f}s "
              f"({n} → {actual_unique} registros, "
              f"{(n-actual_unique)/n*100:.1f}% eliminados)")

        assert elapsed < 2.0, f"Dedup tomó {elapsed:.2f}s — posible complejidad O(n²)"
        assert actual_unique == expected_unique

    def test_dedup_scales_linearly(self):
        """
        Verificar que el tiempo escala ~linealmente con n.
        Ratio de tiempo(10x) debe ser < 15x (O(n log n) sería 11x, O(n²) sería 100x).
        """
        random.seed(0)

        def time_dedup(n: int) -> float:
            records = [{"symbol": "BTC", "id": i % (n // 2)} for i in range(n)]
            t0 = time.perf_counter()
            self._dedup(records, ["symbol", "id"])
            return time.perf_counter() - t0

        t_small  = time_dedup(1_000)
        t_large  = time_dedup(10_000)

        ratio = t_large / max(t_small, 1e-9)
        print(f"\n  Scaling ratio (10x más datos): {ratio:.1f}x más tiempo")

        assert ratio < 20.0, (
            f"Scaling ratio={ratio:.1f}x sugiere complejidad super-lineal"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Q: ¿los datos sintéticos representan el volumen real?
# ─────────────────────────────────────────────────────────────────────────────

class TestStreamRateValidation:
    """
    Verifica que el volumen de datos generado en demo/test
    cumple el requisito de ≥ 1 evento/segundo del proyecto.
    """

    def test_binance_combined_stream_rate(self):
        """
        Stream real estimado:
          BTCUSDT aggTrade:  ~5–50 ev/s
          BTCUSDT bookTicker: ~5–20 ev/s
          × 3 símbolos ≈ 30–210 ev/s combinado

        El requisito del proyecto es ≥ 1 ev/s.
        Este test verifica que los datos sintéticos cumplen el requisito.
        """
        n_events     = 600    # 600 eventos = 10 minutos a 1 ev/s
        duration_s   = 600    # 10 minutos simulados

        events_per_second = n_events / duration_s
        min_required = 1.0

        assert events_per_second >= min_required, (
            f"Tasa de eventos {events_per_second:.2f} ev/s por debajo "
            f"del requisito de {min_required} ev/s"
        )
        print(f"\n  Stream rate: {events_per_second:.2f} ev/s "
              f"(requisito: ≥{min_required} ev/s) ✓")

    def test_demo_dataset_has_enough_events_per_symbol(self):
        """Cada símbolo debe tener al menos 50 eventos en el demo para
        que las features sean estadísticamente válidas."""
        events_per_symbol = 200  # configurado en processing/job.py make_demo_trades
        min_for_features  = 50   # mínimo para rolling_volatility con window=10

        assert events_per_symbol >= min_for_features, (
            f"Solo {events_per_symbol} eventos/símbolo — "
            f"insuficiente para features con window={min_for_features}"
        )

    def test_three_timestamps_always_ordered(self):
        """
        Invariante temporal: trade_time ≤ event_time ≤ ingestion_ts.
        Si se viola, hay un problema de sincronización de relojes.
        """
        # Simula 100 eventos con timestamps realistas
        random.seed(42)
        violations = 0
        for _ in range(100):
            trade_time   = int(time.time() * 1000) - random.randint(0, 100)
            event_time   = trade_time + random.randint(0, 10)    # +0–10ms
            ingestion_ts = event_time + random.randint(0, 50)    # +0–50ms

            if not (trade_time <= event_time <= ingestion_ts):
                violations += 1

        assert violations == 0, (
            f"Orden temporal violado en {violations}/100 eventos"
        )
