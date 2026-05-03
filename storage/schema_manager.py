"""
storage/schema_manager.py
--------------------------
Aplica el DDL de Cassandra en el arranque del sistema.

Diseño:
  - Lee schemas/cassandra.cql y ejecuta cada statement de forma idempotente
    (todos usan IF NOT EXISTS).
  - El keyspace se crea conectando sin keyspace primero, luego se usa
    la sesión de la aplicación.
  - Separado de session.py para poder ejecutarlo independientemente
    (útil en scripts de CI o scripts de inicialización).

Uso:
    python -m storage.schema_manager
"""

from __future__ import annotations

import os
from pathlib import Path

from cassandra.cluster import Cluster
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.query import SimpleStatement

from consumer.logger import get_logger
from storage import session as session_module

logger = get_logger("cassandra.schema")

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "cassandra.cql"
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")


def _load_statements(path: Path) -> list[str]:
    """
    Lee el archivo CQL y separa en statements individuales.
    Ignora líneas de comentario y statements vacíos.
    """
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
    """
    Conecta a Cassandra (sin keyspace) y aplica todos los statements del DDL.
    Idempotente — seguro de ejecutar múltiples veces.
    """
    logger.info("Aplicando schema. path=%s", SCHEMA_PATH)

    statements = _load_statements(SCHEMA_PATH)
    logger.info("Statements encontrados: %d", len(statements))

    # Conectar sin keyspace para poder crear el keyspace
    cluster = Cluster(
        contact_points=CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        protocol_version=4,
        reconnection_policy=ExponentialReconnectionPolicy(1.0, 60.0),
        connect_timeout=15,
    )

    try:
        session = cluster.connect()          # sin keyspace
        for i, stmt in enumerate(statements, 1):
            preview = stmt[:60].replace("\n", " ")
            logger.info("Ejecutando statement %d/%d: %s...", i, len(statements), preview)
            session.execute(SimpleStatement(stmt))
        logger.info("Schema aplicado correctamente.")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    apply_schema()
