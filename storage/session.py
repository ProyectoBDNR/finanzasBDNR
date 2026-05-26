"""
storage/session.py
------------------
Singleton de conexión a Cassandra con soporte RBAC.

Roles soportados (configurables vía .env):
  cf_writer  → consumer/main.py          CASSANDRA_WRITER_USER / PASSWORD
  cf_analyst → Spark jobs                CASSANDRA_ANALYST_USER / PASSWORD
  cf_admin   → mantenimiento manual      CASSANDRA_ADMIN_USER / PASSWORD

Si las variables de entorno de RBAC no están definidas, conecta sin
autenticación (compatible con AllowAllAuthenticator en desarrollo local).
"""

from __future__ import annotations

import os
import threading
from typing import Optional

from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    ExponentialReconnectionPolicy,
    TokenAwarePolicy,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import ConsistencyLevel

from consumer.logger import get_logger

logger = get_logger("cassandra.session")

# Multi-nodo: el driver descubre todos los nodos via gossip protocol.
# Solo es necesario especificar el seed node (cassandra-1).
# TokenAwarePolicy enruta las queries al nodo que posee el token
# de la partition key — distribuyendo la carga entre los 3 nodos.
CASSANDRA_HOSTS    = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT     = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")

_lock    = threading.Lock()
_cluster: Optional[Cluster]  = None
_session: Optional[Session]  = None


def _build_auth_provider() -> Optional[PlainTextAuthProvider]:
    """
    Construye PlainTextAuthProvider para el rol cf_writer (usado por el consumer).
    Si las variables no están definidas, retorna None (sin autenticación).
    """
    user = os.getenv("CASSANDRA_WRITER_USER")
    pwd  = os.getenv("CASSANDRA_WRITER_PASSWORD")
    if user and pwd:
        logger.info("RBAC activo — conectando como cf_writer (user=%s)", user)
        return PlainTextAuthProvider(username=user, password=pwd)
    logger.info("RBAC no configurado — conectando sin autenticación.")
    return None


def get_session() -> Session:
    """Retorna la sesión Cassandra compartida (thread-safe, singleton)."""
    global _cluster, _session

    if _session is not None:
        return _session

    with _lock:
        if _session is not None:
            return _session

        logger.info("Iniciando conexión a Cassandra. hosts=%s port=%d",
                    CASSANDRA_HOSTS, CASSANDRA_PORT)

        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=10.0,
        )

        _cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=4,
            connect_timeout=10,
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 60.0),
            auth_provider=_build_auth_provider(),
        )

        _session = _cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Sesión establecida. keyspace=%s", CASSANDRA_KEYSPACE)

    return _session


def close() -> None:
    """Cierra el cluster limpiamente."""
    global _cluster, _session
    with _lock:
        if _cluster:
            _cluster.shutdown()
            _cluster = None
            _session = None
            logger.info("Conexión a Cassandra cerrada.")