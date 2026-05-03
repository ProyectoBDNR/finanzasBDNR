"""
tests/test_storage.py
---------------------
Prueba el writer y el schema manager sin Cassandra real.
Todos los componentes de Cassandra están mockeados.

Ejecutar: python -m pytest tests/test_storage.py -v
"""

import asyncio
import threading
import time
import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call
from collections import defaultdict

import pytest

from consumer.models import AggTrade, BookTicker
from storage.cassandra_writer import CassandraWriter, _ms_to_date, BATCH_SIZE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_trade(
    symbol="BTCUSDT",
    trade_time=1718000000000,
    price=67850.0,
    quantity=0.01,
    is_buyer_maker=False,
) -> AggTrade:
    return AggTrade(
        symbol=symbol,
        agg_trade_id=123456,
        price=price,
        quantity=quantity,
        trade_time=trade_time,
        event_time=trade_time + 5,
        is_buyer_maker=is_buyer_maker,
    )


def make_ticker(
    symbol="BTCUSDT",
    event_time=1718000001000,
    bid=67849.0,
    ask=67851.0,
) -> BookTicker:
    return BookTicker(
        symbol=symbol,
        best_bid_price=bid,
        best_bid_qty=1.0,
        best_ask_price=ask,
        best_ask_qty=0.5,
        event_time=event_time,
    )


def run_async(coro):
    """Ejecuta una coroutine en el event loop de test."""
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Tests — _ms_to_date helper
# ---------------------------------------------------------------------------

class TestMsToDate:

    def test_known_timestamp(self):
        # 1718000000000 ms = 2024-06-10 UTC (aproximado)
        result = _ms_to_date(1718000000000)
        assert result == "2024-06-10"

    def test_returns_string(self):
        assert isinstance(_ms_to_date(1718000000000), str)

    def test_format_yyyy_mm_dd(self):
        result = _ms_to_date(1718000000000)
        parts = result.split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4   # yyyy
        assert len(parts[1]) == 2   # mm
        assert len(parts[2]) == 2   # dd


# ---------------------------------------------------------------------------
# Fixture: writer con Cassandra mockeada
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    """Sesión de Cassandra completamente mockeada."""
    session = MagicMock()

    # prepare() devuelve un PreparedStatement mock
    ps = MagicMock()
    ps.bind.return_value = MagicMock()      # BoundStatement mock
    session.prepare.return_value = ps

    # execute_async() devuelve un future mock con add_errback
    future = MagicMock()
    future.add_errback = MagicMock()
    session.execute_async.return_value = future

    return session


@pytest.fixture
def writer(mock_session):
    """CassandraWriter con sesión mockeada, listo para usar."""
    with patch("storage.cassandra_writer.get_session", return_value=mock_session):
        w = CassandraWriter(batch_size=BATCH_SIZE, flush_interval_s=60.0)
        w.start()
        yield w
        w._running = False   # detener flush thread sin esperar


# ---------------------------------------------------------------------------
# Tests — binding y buffering
# ---------------------------------------------------------------------------

class TestWriterBinding:

    def test_write_trade_adds_to_buffer(self, writer):
        trade = make_trade()
        run_async(writer.write(trade))
        assert len(writer._buffers) == 1

    def test_write_ticker_adds_to_buffer(self, writer):
        ticker = make_ticker()
        run_async(writer.write(ticker))
        assert len(writer._buffers) == 1

    def test_same_symbol_same_partition(self, writer):
        """Eventos del mismo símbolo y día van al mismo buffer."""
        t1 = make_trade(symbol="BTCUSDT", trade_time=1718000000000)
        t2 = make_trade(symbol="BTCUSDT", trade_time=1718000100000)
        run_async(writer.write(t1))
        run_async(writer.write(t2))
        assert len(writer._buffers) == 1
        partition = list(writer._buffers.keys())[0]
        assert len(writer._buffers[partition]) == 2

    def test_different_symbols_different_partitions(self, writer):
        btc = make_trade(symbol="BTCUSDT")
        eth = make_trade(symbol="ETHUSDT")
        run_async(writer.write(btc))
        run_async(writer.write(eth))
        assert len(writer._buffers) == 2

    def test_ps_bind_called_with_correct_symbol(self, writer, mock_session):
        trade = make_trade(symbol="ETHUSDT", price=3400.0)
        run_async(writer.write(trade))
        bound_args = mock_session.prepare.return_value.bind.call_args[0][0]
        assert bound_args[0] == "ETHUSDT"

    def test_trade_price_passed_to_bind(self, writer, mock_session):
        trade = make_trade(price=99999.99)
        run_async(writer.write(trade))
        bound_args = mock_session.prepare.return_value.bind.call_args[0][0]
        assert bound_args[5] == pytest.approx(99999.99)

    def test_trace_id_is_uuid_object(self, writer, mock_session):
        trade = make_trade()
        run_async(writer.write(trade))
        bound_args = mock_session.prepare.return_value.bind.call_args[0][0]
        trace_id_value = bound_args[8]
        assert isinstance(trace_id_value, uuid.UUID)

    def test_spread_included_in_ticker_binding(self, writer, mock_session):
        ticker = make_ticker(bid=100.0, ask=100.5)
        run_async(writer.write(ticker))
        bound_args = mock_session.prepare.return_value.bind.call_args[0][0]
        spread_value = bound_args[7]
        assert spread_value == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Tests — flush por tamaño
# ---------------------------------------------------------------------------

class TestFlushBySize:

    def test_flush_triggered_at_batch_size(self, writer, mock_session):
        """Al alcanzar BATCH_SIZE eventos en la misma partición, se hace flush."""
        for i in range(BATCH_SIZE):
            run_async(writer.write(make_trade(trade_time=1718000000000 + i * 1000)))

        # El buffer debe estar vacío tras el flush automático
        assert len(writer._buffers) == 0

    def test_execute_async_called_on_flush(self, writer, mock_session):
        for i in range(BATCH_SIZE):
            run_async(writer.write(make_trade(trade_time=1718000000000 + i * 1000)))

        assert mock_session.execute_async.called

    def test_errback_registered_on_future(self, writer, mock_session):
        for i in range(BATCH_SIZE):
            run_async(writer.write(make_trade(trade_time=1718000000000 + i * 1000)))

        future = mock_session.execute_async.return_value
        assert future.add_errback.called

    def test_stats_batch_count_increments(self, writer, mock_session):
        for i in range(BATCH_SIZE):
            run_async(writer.write(make_trade(trade_time=1718000000000 + i * 1000)))

        assert writer.get_stats()["batches_flushed"] == 1

    def test_buffer_below_batch_size_not_flushed(self, writer, mock_session):
        """Menos de BATCH_SIZE eventos: no se hace flush hasta el período."""
        for i in range(BATCH_SIZE - 1):
            run_async(writer.write(make_trade(trade_time=1718000000000 + i * 1000)))

        # Sin flush periódico activo (interval=60s), el buffer debe estar lleno
        assert mock_session.execute_async.call_count == 0


# ---------------------------------------------------------------------------
# Tests — flush_all
# ---------------------------------------------------------------------------

class TestFlushAll:

    def test_flush_all_clears_buffers(self, writer, mock_session):
        run_async(writer.write(make_trade()))
        run_async(writer.write(make_ticker()))
        assert len(writer._buffers) > 0

        writer._flush_all()
        assert len(writer._buffers) == 0

    def test_flush_all_sends_all_partitions(self, writer, mock_session):
        for sym in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
            run_async(writer.write(make_trade(symbol=sym)))

        writer._flush_all()
        assert mock_session.execute_async.call_count == 3


# ---------------------------------------------------------------------------
# Tests — schema manager (sin Cassandra)
# ---------------------------------------------------------------------------

class TestSchemaManager:

    def test_load_statements_from_cql(self):
        """Verifica que el parser de CQL extrae los statements correctamente."""
        from storage.schema_manager import _load_statements
        from pathlib import Path

        path = Path("schemas/cassandra.cql")
        if not path.exists():
            pytest.skip("schemas/cassandra.cql no encontrado")

        stmts = _load_statements(path)
        # Debe tener al menos: CREATE KEYSPACE + CREATE TABLE (×2)
        assert len(stmts) >= 3
        # Todos deben terminar en ";"
        for stmt in stmts:
            assert stmt.strip().endswith(";")
        # Ninguno debe ser solo comentario
        for stmt in stmts:
            non_comment = [
                l for l in stmt.splitlines()
                if l.strip() and not l.strip().startswith("--")
            ]
            assert len(non_comment) > 0

    def test_apply_schema_calls_execute(self):
        """apply_schema() ejecuta todos los statements en la sesión."""
        from storage.schema_manager import apply_schema

        mock_cluster_instance = MagicMock()
        mock_session = MagicMock()
        mock_cluster_instance.connect.return_value = mock_session

        with patch("storage.schema_manager.Cluster", return_value=mock_cluster_instance):
            apply_schema()

        assert mock_session.execute.call_count >= 3
        mock_cluster_instance.shutdown.assert_called_once()
