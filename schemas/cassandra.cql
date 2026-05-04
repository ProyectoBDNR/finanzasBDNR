-- =============================================================================
-- CryptoFlow Analytics — Cassandra DDL
-- Keyspace: cryptoflow
-- =============================================================================
-- Estrategia de particionado:
--   raw_trades       → (symbol, date)  clustering: trade_time DESC
--   raw_book_tickers → (symbol, date)  clustering: event_time DESC
--
-- Justificación:
--   - symbol filtra por activo (BTCUSDT / ETHUSDT / BNBUSDT)
--   - date (yyyy-mm-dd) limita el tamaño de partición (~5-50 ev/s × 86 400s
--     = máx ~4.3M filas/día por símbolo — dentro del límite recomendado)
--   - clustering DESC coloca los eventos más recientes primero (LIMIT N rápido)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Keyspace
-- ---------------------------------------------------------------------------
CREATE KEYSPACE IF NOT EXISTS cryptoflow
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    }
    AND durable_writes = true;

-- ---------------------------------------------------------------------------
-- raw_trades  (aggTrade stream)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.raw_trades (
    symbol          text,
    date            text,          -- yyyy-mm-dd  (parte de la partition key)
    trade_time      bigint,        -- ms epoch — tiempo real de ejecución
    event_time      bigint,        -- ms epoch — emisión del evento
    agg_trade_id    bigint,
    price           double,
    quantity        double,
    is_buyer_maker  boolean,
    trace_id        uuid,
    ingestion_ts    timestamp,
    PRIMARY KEY ((symbol, date), trade_time, agg_trade_id)
) WITH CLUSTERING ORDER BY (trade_time DESC, agg_trade_id DESC)
  AND default_time_to_live = 604800  -- 7 días TTL automático
  AND comment = 'Raw aggTrade events from Binance WebSocket';

-- ---------------------------------------------------------------------------
-- raw_book_tickers  (bookTicker stream)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.raw_book_tickers (
    symbol          text,
    date            text,          -- yyyy-mm-dd
    event_time      bigint,        -- ms epoch
    best_bid_price  double,
    best_bid_qty    double,
    best_ask_price  double,
    best_ask_qty    double,
    spread          double,        -- best_ask - best_bid (desnormalizado para queries)
    trace_id        uuid,
    ingestion_ts    timestamp,
    PRIMARY KEY ((symbol, date), event_time, trace_id)
) WITH CLUSTERING ORDER BY (event_time DESC, trace_id DESC)
  AND default_time_to_live = 604800
  AND comment = 'Raw bookTicker events from Binance WebSocket';

-- ---------------------------------------------------------------------------
-- ohlcv_1m  (OHLCV de 1 minuto)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.ohlcv_1m (
    symbol          text,
    window_label    text,
    window_start    timestamp,
    window_end      timestamp,
    open            double,
    high            double,
    low             double,
    close           double,
    volume          double,
    trade_count     bigint,
    buy_volume      double,
    sell_volume     double,
    price_qty_sum   double,
    PRIMARY KEY ((symbol, window_label), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'OHLCV agregado por ventana de 1 minuto';

-- ---------------------------------------------------------------------------
-- ohlcv_5m  (OHLCV de 5 minutos)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.ohlcv_5m (
    symbol          text,
    window_label    text,
    window_start    timestamp,
    window_end      timestamp,
    open            double,
    high            double,
    low             double,
    close           double,
    volume          double,
    trade_count     bigint,
    buy_volume      double,
    sell_volume     double,
    price_qty_sum   double,
    PRIMARY KEY ((symbol, window_label), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'OHLCV agregado por ventana de 5 minutos';

-- ---------------------------------------------------------------------------
-- ohlcv_1h  (OHLCV de 1 hora)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.ohlcv_1h (
    symbol          text,
    window_label    text,
    window_start    timestamp,
    window_end      timestamp,
    open            double,
    high            double,
    low             double,
    close           double,
    volume          double,
    trade_count     bigint,
    buy_volume      double,
    sell_volume     double,
    price_qty_sum   double,
    PRIMARY KEY ((symbol, window_label), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'OHLCV agregado por ventana de 1 hora';

-- ---------------------------------------------------------------------------
-- features_by_window  (output del Feature Engine)
-- Particionado por (symbol, window_label) para queries por activo y ventana
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.features_by_window (
    symbol              text,
    window_label        text,
    window_start        timestamp,
    window_end          timestamp,
    open                double,
    high                double,
    low                 double,
    close               double,
    volume              double,
    trade_count         bigint,
    buy_volume          double,
    sell_volume         double,
    price_qty_sum       double,
    vwap                double,
    log_return          double,
    rolling_volatility  double,
    momentum            double,
    momentum_pct        double,
    buy_sell_ratio      double,
    spread_mean         double,
    spread_mean_pct     double,
    spread_min          double,
    spread_max          double,
    spread_std          double,
    mid_price_mean      double,
    tick_count          bigint,
    PRIMARY KEY ((symbol, window_label), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'Features calculadas por el Feature Engine por ventana temporal';

-- ---------------------------------------------------------------------------
-- spread_timeseries  (serie temporal del spread bid/ask)
-- Particionado por (symbol, date) para queries por activo y día
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cryptoflow.spread_timeseries (
    symbol          text,
    date            text,          -- yyyy-mm-dd
    window_start    timestamp,
    spread_mean     double,
    spread_min      double,
    spread_max      double,
    spread_std      double,
    mid_price_mean  double,
    tick_count      bigint,
    PRIMARY KEY ((symbol, date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'Serie temporal del spread bid/ask por minuto';
