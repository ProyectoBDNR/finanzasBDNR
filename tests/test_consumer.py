"""
tests/test_consumer.py
----------------------
Prueba el parser y los modelos sin conexión a Binance.
Ejecutar: python -m pytest tests/test_consumer.py -v
"""

import json
import pytest
from consumer.models import AggTrade, BookTicker
from consumer.binance_ws import parse_event


# ---------------------------------------------------------------------------
# Fixtures — payloads reales del WebSocket combinado de Binance
# ---------------------------------------------------------------------------

AGG_TRADE_RAW = {
    "stream": "btcusdt@aggTrade",
    "data": {
        "e": "aggTrade",
        "E": 1718000000123,
        "s": "BTCUSDT",
        "a": 3456789,
        "p": "67850.50",
        "q": "0.01234",
        "f": 9000001,
        "l": 9000003,
        "T": 1718000000100,
        "m": False,
        "M": True,
    },
}

BOOK_TICKER_RAW = {
    "stream": "btcusdt@bookTicker",
    "data": {
        "e": "bookTicker",
        "E": 1718000001000,
        "s": "BTCUSDT",
        "b": "67849.00",
        "B": "1.234",
        "a": "67851.00",
        "A": "0.567",
    },
}

UNKNOWN_EVENT_RAW = {
    "stream": "btcusdt@something",
    "data": {"e": "unknownEvent"},
}


# ---------------------------------------------------------------------------
# Tests — AggTrade
# ---------------------------------------------------------------------------

class TestAggTradeParser:

    def test_parse_returns_agg_trade(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert isinstance(event, AggTrade)

    def test_symbol_uppercase(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert event.symbol == "BTCUSDT"

    def test_price_is_float(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert isinstance(event.price, float)
        assert event.price == pytest.approx(67850.50)

    def test_quantity_is_float(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert isinstance(event.quantity, float)
        assert event.quantity == pytest.approx(0.01234)

    def test_is_buyer_maker_false(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert event.is_buyer_maker is False

    def test_trace_id_is_uuid_string(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert isinstance(event.trace_id, str)
        assert len(event.trace_id) == 36  # formato UUID4

    def test_trace_ids_are_unique(self):
        e1 = parse_event(json.dumps(AGG_TRADE_RAW))
        e2 = parse_event(json.dumps(AGG_TRADE_RAW))
        assert e1.trace_id != e2.trace_id

    def test_ingestion_ts_is_set(self):
        from datetime import datetime
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        assert isinstance(event.ingestion_ts, datetime)

    def test_to_dict_has_required_keys(self):
        event = parse_event(json.dumps(AGG_TRADE_RAW))
        d = event.to_dict()
        required = {
            "symbol", "price", "quantity", "trade_time",
            "event_time", "is_buyer_maker", "trace_id", "ingestion_ts",
        }
        assert required.issubset(d.keys())


# ---------------------------------------------------------------------------
# Tests — BookTicker
# ---------------------------------------------------------------------------

class TestBookTickerParser:

    def test_parse_returns_book_ticker(self):
        event = parse_event(json.dumps(BOOK_TICKER_RAW))
        assert isinstance(event, BookTicker)

    def test_bid_ask_are_floats(self):
        event = parse_event(json.dumps(BOOK_TICKER_RAW))
        assert event.best_bid_price == pytest.approx(67849.00)
        assert event.best_ask_price == pytest.approx(67851.00)

    def test_spread_is_positive(self):
        event = parse_event(json.dumps(BOOK_TICKER_RAW))
        assert event.spread > 0
        assert event.spread == pytest.approx(2.00)

    def test_to_dict_includes_spread(self):
        event = parse_event(json.dumps(BOOK_TICKER_RAW))
        assert "spread" in event.to_dict()

    def test_trace_id_present(self):
        event = parse_event(json.dumps(BOOK_TICKER_RAW))
        assert event.trace_id is not None


# ---------------------------------------------------------------------------
# Tests — Casos borde
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_unknown_event_returns_none(self):
        result = parse_event(json.dumps(UNKNOWN_EVENT_RAW))
        assert result is None

    def test_malformed_json_returns_none(self):
        result = parse_event("this is not json {{{")
        assert result is None

    def test_empty_string_returns_none(self):
        result = parse_event("{}")
        assert result is None
