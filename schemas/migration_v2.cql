-- migration_v2.cql
-- Ejecutar con: docker exec cryptoflow-cassandra-1 cqlsh -u cassandra -p BDNR -f /migration_v2.cql

-- ══════════════════════════════════════════════════════════════
-- 1. Recrear features_by_window con schema completo
-- ══════════════════════════════════════════════════════════════
DROP TABLE IF EXISTS cryptoflow.features_by_window;

CREATE TABLE cryptoflow.features_by_window (
    symbol              text,
    window_label        text,
    window_start        timestamp,
    window_end          timestamp,
    -- OHLCV
    open                double,
    high                double,
    low                 double,
    close               double,
    volume              double,
    trade_count         bigint,
    buy_volume          double,
    sell_volume         double,
    -- Features
    vwap                double,
    log_return          double,
    rolling_volatility  double,
    realized_volatility double,
    momentum            double,
    momentum_pct        double,
    buy_sell_ratio      double,
    return_autocorr     double,
    -- Spread
    spread_mean         double,
    spread_mean_pct     double,
    spread_min          double,
    spread_max          double,
    spread_std          double,
    mid_price_mean      double,
    tick_count          bigint,
    obi                 double,
    PRIMARY KEY ((symbol, window_label), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'Features cuantitativas por símbolo y ventana temporal';

-- ══════════════════════════════════════════════════════════════
-- 2. Otorgar permisos en tablas nuevas a cf_analyst
-- ══════════════════════════════════════════════════════════════
GRANT SELECT ON cryptoflow.features_by_window TO cf_analyst;
GRANT MODIFY ON cryptoflow.features_by_window TO cf_analyst;
GRANT SELECT ON cryptoflow.spread_timeseries   TO cf_analyst;
GRANT MODIFY ON cryptoflow.spread_timeseries   TO cf_analyst;
