-- =============================================================================
-- migration_v4_cointegration.cql
-- -----------------------------------------------------------------------------
-- Agrega la tabla `pairs_zscore_1m` para persistir el z-score de cointegración
-- rolling entre pares cripto (BTC-ETH, BTC-BNB, ETH-BNB). Es la primera tabla
-- cross-asset del esquema: la partition key incluye ambos símbolos.
--
-- Particionado
--   PK ((sym_dom, sym_hedge, lookback), window_start)
--     - (sym_dom, sym_hedge, lookback) limita las particiones a un par fijo
--       y un horizonte de cointegración concreto. Cada combinación produce
--       como máximo ~525,600 filas/año a granularidad 1m — muy debajo del
--       límite recomendado por Cassandra (~100MB/partición).
--     - window_start como clustering DESC permite leer las observaciones
--       más recientes con LIMIT N de forma eficiente.
-- =============================================================================

CREATE TABLE IF NOT EXISTS cryptoflow.pairs_zscore_1m (
    sym_dom      text,
    sym_hedge    text,
    lookback     int,
    window_start timestamp,
    alpha        double,
    beta         double,
    spread_pct   double,
    z_score      double,
    PRIMARY KEY ((sym_dom, sym_hedge, lookback), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND comment = 'Z-score de cointegracion rolling entre pares cripto';

-- ---------------------------------------------------------------------------
-- RBAC: alineado con la convención existente (cf_analyst lee y escribe
-- features y agregados). Ver `schemas/rbac.cql` para los demás grants.
-- ---------------------------------------------------------------------------
GRANT SELECT ON cryptoflow.pairs_zscore_1m TO cf_analyst;
GRANT MODIFY ON cryptoflow.pairs_zscore_1m TO cf_analyst;
