"""
storage/session.py
------------------
Singleton de conexión a Cassandra.

Proporciona una única instancia de `cassandra.cluster.Session` compartida
por toda la aplicación. El cluster se crea una vez y se reutiliza para
evitar el overhead de múltiples handshakes TCP.

Configuración relevante para rendimiento:
  - protocol_version=4        → compatible con Cassandra 3.x y 4.x
  - execution_profiles         → perfil default con consistencia LOCAL_QUORUM
  - connect_timeout            → falla rápido si Cassandra no está disponible
  - reconnection_policy        → reconexión exponencial automática del driver
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
from cassandra.query import ConsistencyLevel

from consumer.logger import get_logger

logger = get_logger("cassandra.session")

# ---------------------------------------------------------------------------
# Configuración — sobreescribible por variables de entorno
# ---------------------------------------------------------------------------

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "cryptoflow")

# ---------------------------------------------------------------------------
# Singleton thread-safe
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_cluster: Optional[Cluster] = None
_session: Optional[Session] = None


def get_session() -> Session:
    """
    Retorna la sesión Cassandra compartida, creándola si no existe.
    Thread-safe mediante double-checked locking.
    """
    global _cluster, _session

    if _session is not None:
        return _session

    with _lock:
        if _session is not None:   # segundo check dentro del lock
            return _session

        logger.info(
            "Iniciando conexión a Cassandra. hosts=%s port=%d",
            CASSANDRA_HOSTS, CASSANDRA_PORT,
        )

        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy()
            ),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=10.0,
        )

        _cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=4,
            connect_timeout=10,
            reconnection_policy=ExponentialReconnectionPolicy(
                base_delay=1.0,
                max_delay=60.0,
            ),
        )

        _session = _cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Sesión establecida. keyspace=%s", CASSANDRA_KEYSPACE)

    return _session


def close() -> None:
    """Cierra el cluster limpiamente. Llamar en shutdown."""
    global _cluster, _session
    with _lock:
        if _cluster:
            _cluster.shutdown()
            _cluster = None
            _session = None
            logger.info("Conexión a Cassandra cerrada.")
