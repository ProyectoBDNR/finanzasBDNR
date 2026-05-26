-- =============================================================================
-- CryptoFlow Analytics — RBAC
-- Ejecutar como superusuario: cqlsh -u cassandra -p cassandra -f rbac.cql
-- Prerequisito: PasswordAuthenticator + CassandraAuthorizer en cassandra.yaml
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Cambiar la contraseña del superusuario por defecto (hacer PRIMERO)
-- ---------------------------------------------------------------------------
ALTER ROLE cassandra WITH PASSWORD = 'BDNR';

-- ---------------------------------------------------------------------------
-- 2. cf_writer — escritura exclusiva de eventos raw desde el consumer Python
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS cf_writer
    WITH PASSWORD = 'writer_pwd_BDNR'
    AND LOGIN = true
    AND SUPERUSER = false;

GRANT MODIFY ON TABLE cryptoflow.raw_trades        TO cf_writer;
GRANT MODIFY ON TABLE cryptoflow.raw_book_tickers  TO cf_writer;

-- ---------------------------------------------------------------------------
-- 3. cf_analyst — lectura/escritura de datos procesados por Spark
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS cf_analyst
    WITH PASSWORD = 'analyst_pwd_BDNR'
    AND LOGIN = true
    AND SUPERUSER = false;

-- Lectura de raw (Spark lee para limpiar y agregar)
GRANT SELECT ON TABLE cryptoflow.raw_trades        TO cf_analyst;
GRANT SELECT ON TABLE cryptoflow.raw_book_tickers  TO cf_analyst;

-- Lectura + escritura en tablas procesadas
GRANT SELECT ON TABLE cryptoflow.ohlcv_1m          TO cf_analyst;
GRANT MODIFY ON TABLE cryptoflow.ohlcv_1m          TO cf_analyst;
GRANT SELECT ON TABLE cryptoflow.ohlcv_5m          TO cf_analyst;
GRANT MODIFY ON TABLE cryptoflow.ohlcv_5m          TO cf_analyst;
GRANT SELECT ON TABLE cryptoflow.ohlcv_1h          TO cf_analyst;
GRANT MODIFY ON TABLE cryptoflow.ohlcv_1h          TO cf_analyst;
GRANT SELECT ON TABLE cryptoflow.features_by_window TO cf_analyst;
GRANT MODIFY ON TABLE cryptoflow.features_by_window TO cf_analyst;

-- ---------------------------------------------------------------------------
-- 4. cf_admin — administración completa del keyspace
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS cf_admin
    WITH PASSWORD = 'admin_pwd_BDNR'
    AND LOGIN = true
    AND SUPERUSER = false;

GRANT ALL PERMISSIONS ON KEYSPACE cryptoflow TO cf_admin;
GRANT CREATE    ON ALL ROLES TO cf_admin;
GRANT ALTER     ON ALL ROLES TO cf_admin;
GRANT DROP      ON ALL ROLES TO cf_admin;
GRANT DESCRIBE  ON ALL ROLES TO cf_admin;
GRANT AUTHORIZE ON KEYSPACE cryptoflow TO cf_admin;

-- ---------------------------------------------------------------------------
-- 5. Verificación
-- ---------------------------------------------------------------------------
LIST ALL PERMISSIONS OF cf_writer;
LIST ALL PERMISSIONS OF cf_analyst;
LIST ALL PERMISSIONS OF cf_admin;
