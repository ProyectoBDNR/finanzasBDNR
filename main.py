"""
main.py
-------
Entrypoint del pipeline CryptoFlow.

Arranque:
    0. Espera a que Cassandra esté disponible (con timeout).
    1. Aplica schema en Cassandra (idempotente).
    2. Inicia Spark scheduler (batch cada SPARK_BATCH_INTERVAL_MINUTES).
    3. Inicializa CassandraWriter.
    4. Arranca BinanceConsumer (WebSocket en tiempo real).
    5. Shutdown limpio en Ctrl+C.

Variables de entorno:
    CASSANDRA_HOSTS                (default: 127.0.0.1)
    CASSANDRA_PORT                 (default: 9042)
    CASSANDRA_KEYSPACE             (default: cryptoflow)
    CASSANDRA_WRITER_USER          (opcional, RBAC)
    CASSANDRA_WRITER_PASSWORD      (opcional, RBAC)
    SPARK_BATCH_INTERVAL_MINUTES   (default: 5)
"""

import asyncio
import os
import signal
import socket
import time

# Cargar variables de entorno desde .env antes de cualquier otro import
from dotenv import load_dotenv
load_dotenv()

from consumer.binance_ws import BinanceConsumer
from consumer.logger import get_logger
from storage.cassandra_writer import CassandraWriter
from storage.schema_manager import apply_schema
from storage import session as cassandra_session
from processing.scheduler import start_scheduler

logger = get_logger("main")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")[0]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))


def wait_for_cassandra(
    host: str = CASSANDRA_HOST,
    port: int = CASSANDRA_PORT,
    timeout_s: int = 120,
    interval_s: float = 3.0,
) -> None:
    """Espera hasta que Cassandra acepte conexiones TCP."""
    logger.info("Esperando Cassandra en %s:%d (timeout=%ds)...", host, port, timeout_s)
    deadline = time.monotonic() + timeout_s
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info("Cassandra disponible. host=%s port=%d intentos=%d",
                            host, port, attempt)
                time.sleep(2.0)
                return
        except OSError:
            remaining = round(deadline - time.monotonic())
            logger.info("Cassandra no lista. Reintento en %.0fs (intento=%d, restante=%ds)...",
                        interval_s, attempt, remaining)
            time.sleep(interval_s)

    raise RuntimeError(
        f"Cassandra no respondió en {timeout_s}s ({host}:{port}).\n"
        "Verifica con: docker ps && docker logs cryptoflow-cassandra"
    )


async def main() -> None:
    # 0. Esperar Cassandra
    wait_for_cassandra()

    # 1. Schema
    logger.info("Paso 1/4 — Aplicando schema Cassandra...")
    apply_schema()

    # 2. Spark scheduler automático
    logger.info("Paso 2/4 — Iniciando Spark scheduler (intervalo: %s min)...",
                os.getenv("SPARK_BATCH_INTERVAL_MINUTES", "5"))
    scheduler_thread, scheduler_stop = start_scheduler()

    # 3. Writer
    logger.info("Paso 3/4 — Inicializando CassandraWriter...")
    writer = CassandraWriter()
    writer.start()

    # 4. Consumer
    logger.info("Paso 4/4 — Arrancando BinanceConsumer...")
    consumer = BinanceConsumer(handler=writer.write)

    def _shutdown(signum, frame):
        logger.info("Señal recibida (%d). Deteniendo pipeline...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        await consumer.run()
    finally:
        logger.info("Shutdown: deteniendo scheduler y flusheando escrituras...")
        scheduler_stop.set()
        scheduler_thread.join(timeout=5)
        writer.stop()
        cassandra_session.close()
        logger.info("Shutdown completo.")


if __name__ == "__main__":
    asyncio.run(main())