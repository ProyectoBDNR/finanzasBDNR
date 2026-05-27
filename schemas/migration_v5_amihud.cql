-- =============================================================================
-- migration_v5_amihud.cql
-- -----------------------------------------------------------------------------
-- Agrega la columna `amihud_illiq` a la tabla `features_by_window`.
--
-- Referencia:
--   Amihud, Y. (2002). "Illiquidity and stock returns: cross-section and
--   time-series effects". Journal of Financial Markets 5(1), 31-56.
--   DOI: 10.1016/S1386-4181(01)00024-6.
--
-- Unidad: bp por millón USD operado (ver feature_engine/amihud.py).
--
-- Idempotencia: Cassandra acepta ALTER TABLE ADD sólo si la columna no
-- existe aún; si ya existe, retorna error que el setup.py captura como
-- "ya aplicada — continuando".
-- =============================================================================

ALTER TABLE cryptoflow.features_by_window ADD amihud_illiq double;
