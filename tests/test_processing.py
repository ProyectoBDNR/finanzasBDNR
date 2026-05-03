"""
tests/test_processing.py
-------------------------
Tests del pipeline de Spark: cleaner, aggregator y enrichment.

Usa una SparkSession local (local[1]) sin Cassandra.
Los datos se crean directamente con spark.createDataFrame().

Ejecutar: python -m pytest tests/test_processing.py -v
"""

import pytest
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, BooleanType, TimestampType
)

from processing.cleaner import (
    drop_nulls, validate_trades, validate_tickers,
    cast_trade_types, add_trade_derived_cols,
    deduplicate_trades, clean_trades,
    add_ticker_derived_cols, clean_tickers,
)
from processing.aggregator import compute_ohlcv, compute_spread_timeseries
from enrichment.coingecko import get_metadata, get_all_metadata, MOCK_METADATA


# ---------------------------------------------------------------------------
# SparkSession compartida para todos los tests (scope=session)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("cryptoflow-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")   # menos particiones = más rápido en tests
        .config("spark.ui.enabled", "false")            # deshabilitar UI web
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

TRADE_SCHEMA = StructType([
    StructField("symbol",         StringType(),   False),
    StructField("date",           StringType(),   False),
    StructField("trade_time",     LongType(),     False),
    StructField("event_time",     LongType(),     False),
    StructField("agg_trade_id",   LongType(),     False),
    StructField("price",          DoubleType(),   False),
    StructField("quantity",       DoubleType(),   False),
    StructField("is_buyer_maker", BooleanType(),  False),
    StructField("trace_id",       StringType(),   False),
    StructField("ingestion_ts",   TimestampType(), False),
])

TICKER_SCHEMA = StructType([
    StructField("symbol",          StringType(),  False),
    StructField("date",            StringType(),  False),
    StructField("event_time",      LongType(),    False),
    StructField("best_bid_price",  DoubleType(),  False),
    StructField("best_bid_qty",    DoubleType(),  False),
    StructField("best_ask_price",  DoubleType(),  False),
    StructField("best_ask_qty",    DoubleType(),  False),
    StructField("spread",          DoubleType(),  False),
    StructField("trace_id",        StringType(),  False),
    StructField("ingestion_ts",    TimestampType(), False),
])

_TS = datetime(2024, 6, 10, 12, 0, 0, tzinfo=timezone.utc)


def _trade(
    symbol="BTCUSDT",
    trade_time=1718017200000,
    price=67000.0,
    quantity=0.1,
    agg_trade_id=1,
    is_buyer_maker=False,
    ingestion_ts=_TS,
):
    return (symbol, "2024-06-10", trade_time, trade_time + 2,
            agg_trade_id, price, quantity, is_buyer_maker, f"trace-{agg_trade_id}", ingestion_ts)


def _ticker(
    symbol="BTCUSDT",
    event_time=1718017200000,
    bid=67000.0,
    ask=67002.0,
):
    spread = ask - bid
    return (symbol, "2024-06-10", event_time,
            bid, 1.0, ask, 0.5, spread, f"trace-t-{event_time}", _TS)


# ---------------------------------------------------------------------------
# Tests — cleaner: drop_nulls
# ---------------------------------------------------------------------------

class TestDropNulls:

    def test_removes_rows_with_null_price(self, spark):
        rows = [("BTCUSDT", "2024-06-10", 1718000000000, 1718000000002,
                 1, None, 0.1, False, "t1", _TS)]
        schema = StructType([
            StructField("symbol",         StringType(),   True),
            StructField("date",           StringType(),   True),
            StructField("trade_time",     LongType(),     True),
            StructField("event_time",     LongType(),     True),
            StructField("agg_trade_id",   LongType(),     True),
            StructField("price",          DoubleType(),   True),
            StructField("quantity",       DoubleType(),   True),
            StructField("is_buyer_maker", BooleanType(),  True),
            StructField("trace_id",       StringType(),   True),
            StructField("ingestion_ts",   TimestampType(), True),
        ])
        df = spark.createDataFrame(rows, schema=schema)
        result = drop_nulls(df, ["price"])
        assert result.count() == 0

    def test_keeps_complete_rows(self, spark):
        df = spark.createDataFrame([_trade()], schema=TRADE_SCHEMA)
        result = drop_nulls(df, ["symbol", "price", "quantity"])
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Tests — cleaner: validate_trades
# ---------------------------------------------------------------------------

class TestValidateTrades:

    def test_removes_negative_price(self, spark):
        df = spark.createDataFrame([_trade(price=-1.0)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        result = validate_trades(df)
        assert result.count() == 0

    def test_removes_zero_quantity(self, spark):
        df = spark.createDataFrame([_trade(quantity=0.0)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        result = validate_trades(df)
        assert result.count() == 0

    def test_keeps_valid_trade(self, spark):
        df = spark.createDataFrame([_trade(price=67000.0, quantity=0.5)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        result = validate_trades(df)
        assert result.count() == 1

    def test_removes_future_timestamp(self, spark):
        future_ts = 2_000_000_000_000   # año 2033 — fuera de rango
        df = spark.createDataFrame([_trade(trade_time=future_ts)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        result = validate_trades(df)
        assert result.count() == 0


# ---------------------------------------------------------------------------
# Tests — cleaner: validate_tickers
# ---------------------------------------------------------------------------

class TestValidateTickers:

    def test_removes_negative_spread(self, spark):
        # ask < bid → spread negativo
        df = spark.createDataFrame([_ticker(bid=100.0, ask=99.0)], schema=TICKER_SCHEMA)
        result = validate_tickers(df)
        assert result.count() == 0

    def test_keeps_valid_ticker(self, spark):
        df = spark.createDataFrame([_ticker(bid=100.0, ask=100.5)], schema=TICKER_SCHEMA)
        result = validate_tickers(df)
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Tests — cleaner: derived columns
# ---------------------------------------------------------------------------

class TestDerivedColumns:

    def test_trade_datetime_is_timestamp(self, spark):
        df = spark.createDataFrame([_trade()], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        dtype = dict(df.dtypes)["trade_datetime"]
        assert "timestamp" in dtype

    def test_price_qty_product(self, spark):
        df = spark.createDataFrame([_trade(price=100.0, quantity=2.0)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        row = df.select("price_qty").first()
        assert row["price_qty"] == pytest.approx(200.0)

    def test_side_sell_when_buyer_maker(self, spark):
        # is_buyer_maker=True → el agressor es el vendedor → side='sell'
        df = spark.createDataFrame([_trade(is_buyer_maker=True)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        row = df.select("side").first()
        assert row["side"] == "sell"

    def test_side_buy_when_not_buyer_maker(self, spark):
        df = spark.createDataFrame([_trade(is_buyer_maker=False)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        row = df.select("side").first()
        assert row["side"] == "buy"

    def test_log_price_is_positive(self, spark):
        df = spark.createDataFrame([_trade(price=67000.0)], schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        row = df.select("log_price").first()
        assert row["log_price"] > 0

    def test_ticker_spread_recalculated(self, spark):
        df = spark.createDataFrame([_ticker(bid=100.0, ask=100.8)], schema=TICKER_SCHEMA)
        df = add_ticker_derived_cols(df)
        row = df.select("spread").first()
        assert row["spread"] == pytest.approx(0.8)

    def test_ticker_mid_price(self, spark):
        df = spark.createDataFrame([_ticker(bid=100.0, ask=102.0)], schema=TICKER_SCHEMA)
        df = add_ticker_derived_cols(df)
        row = df.select("mid_price").first()
        assert row["mid_price"] == pytest.approx(101.0)


# ---------------------------------------------------------------------------
# Tests — cleaner: deduplicación
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_removes_duplicate_agg_trade_id(self, spark):
        rows = [
            _trade(agg_trade_id=42, ingestion_ts=datetime(2024, 6, 10, 12, 0, 1, tzinfo=timezone.utc)),
            _trade(agg_trade_id=42, ingestion_ts=datetime(2024, 6, 10, 12, 0, 2, tzinfo=timezone.utc)),
        ]
        df = spark.createDataFrame(rows, schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        result = deduplicate_trades(df)
        assert result.count() == 1

    def test_keeps_different_trade_ids(self, spark):
        rows = [_trade(agg_trade_id=1), _trade(agg_trade_id=2)]
        df = spark.createDataFrame(rows, schema=TRADE_SCHEMA)
        df = cast_trade_types(df)
        df = add_trade_derived_cols(df)
        result = deduplicate_trades(df)
        assert result.count() == 2


# ---------------------------------------------------------------------------
# Tests — pipeline completo clean_trades
# ---------------------------------------------------------------------------

class TestCleanTradesPipeline:

    def test_clean_trades_returns_dataframe(self, spark):
        df = spark.createDataFrame([_trade()], schema=TRADE_SCHEMA)
        result = clean_trades(df)
        assert result.count() == 1

    def test_clean_trades_adds_derived_columns(self, spark):
        df = spark.createDataFrame([_trade()], schema=TRADE_SCHEMA)
        result = clean_trades(df)
        cols = result.columns
        assert "trade_datetime" in cols
        assert "price_qty" in cols
        assert "side" in cols
        assert "log_price" in cols

    def test_clean_trades_filters_invalid(self, spark):
        valid   = _trade(price=67000.0, quantity=0.1)
        invalid = _trade(price=-1.0, quantity=0.1, agg_trade_id=2)
        df = spark.createDataFrame([valid, invalid], schema=TRADE_SCHEMA)
        result = clean_trades(df)
        assert result.count() == 1


# ---------------------------------------------------------------------------
# Tests — aggregator: OHLCV
# ---------------------------------------------------------------------------

class TestOHLCV:

    @pytest.fixture
    def clean_df(self, spark):
        """5 trades del mismo símbolo en el mismo minuto."""
        prices = [100.0, 105.0, 98.0, 103.0, 101.0]
        base_ts = 1718017200000   # 2024-06-10 12:00:00 UTC
        rows = [
            _trade(price=p, quantity=1.0, trade_time=base_ts + i * 5_000, agg_trade_id=i)
            for i, p in enumerate(prices)
        ]
        df = spark.createDataFrame(rows, schema=TRADE_SCHEMA)
        return clean_trades(df)

    def test_ohlcv_high_is_max_price(self, clean_df):
        result = compute_ohlcv(clean_df, "1m")
        row = result.filter(F.col("symbol") == "BTCUSDT").first()
        assert row["high"] == pytest.approx(105.0)

    def test_ohlcv_low_is_min_price(self, clean_df):
        result = compute_ohlcv(clean_df, "1m")
        row = result.filter(F.col("symbol") == "BTCUSDT").first()
        assert row["low"] == pytest.approx(98.0)

    def test_ohlcv_volume_is_sum_quantity(self, clean_df):
        result = compute_ohlcv(clean_df, "1m")
        row = result.filter(F.col("symbol") == "BTCUSDT").first()
        assert row["volume"] == pytest.approx(5.0)   # 5 trades × 1.0

    def test_ohlcv_trade_count(self, clean_df):
        result = compute_ohlcv(clean_df, "1m")
        row = result.filter(F.col("symbol") == "BTCUSDT").first()
        assert row["trade_count"] == 5

    def test_ohlcv_has_window_label(self, clean_df):
        result = compute_ohlcv(clean_df, "5m")
        row = result.first()
        assert row["window_label"] == "5m"

    def test_ohlcv_price_qty_sum_present(self, clean_df):
        result = compute_ohlcv(clean_df, "1m")
        row = result.filter(F.col("symbol") == "BTCUSDT").first()
        # price_qty_sum = sum(price * qty) = sum(prices) × 1.0
        expected = sum([100.0, 105.0, 98.0, 103.0, 101.0])
        assert row["price_qty_sum"] == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Tests — aggregator: spread timeseries
# ---------------------------------------------------------------------------

class TestSpreadTimeseries:

    @pytest.fixture
    def ticker_df(self, spark):
        base_ts = 1718017200000
        rows = [
            _ticker(bid=100.0, ask=100.5, event_time=base_ts + i * 5_000)
            for i in range(10)
        ]
        df = spark.createDataFrame(rows, schema=TICKER_SCHEMA)
        return clean_tickers(df)

    def test_spread_mean_computed(self, ticker_df):
        result = compute_spread_timeseries(ticker_df, "1m")
        row = result.first()
        assert row["spread_mean"] == pytest.approx(0.5, abs=0.01)

    def test_tick_count_correct(self, ticker_df):
        result = compute_spread_timeseries(ticker_df, "1m")
        total_ticks = result.agg(F.sum("tick_count")).first()[0]
        assert total_ticks == 10


# ---------------------------------------------------------------------------
# Tests — enrichment: CoinGecko mock
# ---------------------------------------------------------------------------

class TestCoinGeckoMock:

    def test_get_metadata_btc_mock(self):
        meta = get_metadata("BTCUSDT", use_mock=True)
        assert meta["symbol"] == "BTCUSDT"
        assert meta["market_cap_rank"] == 1
        assert meta["market_cap_usd"] > 0

    def test_get_all_metadata_returns_three_symbols(self):
        results = get_all_metadata(use_mock=True)
        assert len(results) == 3
        symbols = {r["symbol"] for r in results}
        assert symbols == {"BTCUSDT", "ETHUSDT", "BNBUSDT"}

    def test_unknown_symbol_returns_empty(self):
        meta = get_metadata("XYZUSDT", use_mock=True)
        assert meta["market_cap_usd"] is None

    def test_eth_has_no_total_supply(self):
        # ETH no tiene supply máximo fijo
        meta = get_metadata("ETHUSDT", use_mock=True)
        assert meta["total_supply"] is None
