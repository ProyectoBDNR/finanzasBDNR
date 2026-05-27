-- =============================================================================
-- migration_v3_microprice.cql
-- -----------------------------------------------------------------------------
-- Agrega la tabla `microprice_by_window` para guardar el Micro-Price
-- agregado por ventana (referencia: hftbacktest — obi_mm).
--
-- Ejecutar con:
--   docker exec cryptoflow-cassandra-1 cqlsh -u cassandra -p BDNR \
--       -f /migration_v3_microprice.cql
--
-- Idempotente: usa IF NOT EXISTS, se puede re-ejecutar sin efectos.
-- =============================================================================

CREATE TABLE IF NOT EXISTS cryptoflow.microprice_by_window (
    symbol                       text,
    window_label                 text,
    window_start                 timestamp,
    window_end                   timestamp,
    micro_price_mean             double,
    micro_price_close            double,
    micro_price_div_mean_bps     double,
    micro_price_div_std_bps      double,
    micro_price_div_skewness     double,
    PRIMARY KEY ((symbol, window_label), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'Micro-Price agregado por ventana (hftbacktest)';

-- ---------------------------------------------------------------------------
-- Permisos RBAC para cf_analyst
-- ---------------------------------------------------------------------------
GRANT SELECT ON cryptoflow.microprice_by_window TO cf_analyst;
GRANT MODIFY ON cryptoflow.microprice_by_window TO cf_analyst;
