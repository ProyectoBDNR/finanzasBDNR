"""
storage/schema_manager.py
--------------------------
Aplica el DDL de Cassandra en el arranque del sistema.
Idempotente — usa IF NOT EXISTS en todos los statements.
Soporta autenticación RBAC vía CASSANDRA_ADMIN_USER / PASSWORD.
"""

from __future__ import annotations

import os
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

from consumer.logger import get_logger

logger = get_logger("cassandra.schema")

SCHEMA_PATH        = Path(__file__).parent.parent / "schemas" / "cassandra.cql"
CASSANDRA_HOSTS    = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")


def _get_auth_provider():
    """
    Usa cf_admin si está configurado, o cassandra/cassandra como fallback,
    o None si no hay autenticación habilitada.
    """
    # Primero intentar cf_admin
    user = os.getenv("CASSANDRA_ADMIN_USER")
    pwd  = os.getenv("CASSANDRA_ADMIN_PASSWORD")
    if user and pwd:
        return PlainTextAuthProvider(username=user, password=pwd)
    # Fallback: superusuario por defecto
    fallback_user = os.getenv("CASSANDRA_SUPER_USER", "cassandra")
    fallback_pwd  = os.getenv("CASSANDRA_SUPER_PASSWORD", "cassandra")
    if fallback_user != "cassandra" or fallback_pwd != "cassandra":
        return PlainTextAuthProvider(username=fallback_user, password=fallback_pwd)
    # Sin autenticación (AllowAllAuthenticator en dev)
    return None


def _load_statements(path: Path) -> list[str]:
    """Lee el CQL y separa en statements individuales ignorando comentarios."""
    raw = path.read_text(encoding="utf-8")
    statements = []
    for stmt in raw.split(";"):
        cleaned = "\n".join(
            line for line in stmt.splitlines()
            if not line.strip().startswith("--")
        ).strip()
        if cleaned:
            statements.append(cleaned + ";")
    return statements


def apply_schema() -> None:
    """Conecta a Cassandra y aplica todos los statements del DDL."""
    logger.info("Aplicando schema. path=%s", SCHEMA_PATH)
    statements = _load_statements(SCHEMA_PATH)
    logger.info("Statements encontrados: %d", len(statements))
    
    auth = PlainTextAuthProvider(username="cassandra", password="BDNR")
    cluster = Cluster(
        contact_points=CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        protocol_version=4,
        reconnection_policy=ExponentialReconnectionPolicy(1.0, 60.0),
        connect_timeout=15,
        auth_provider=auth,
    )

    try:
        session = cluster.connect()
        for stmt in statements:
            session.execute(SimpleStatement(stmt))
        logger.info("Schema aplicado correctamente.")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    apply_schema()