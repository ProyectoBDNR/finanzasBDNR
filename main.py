"""
main.py
-------
Entrypoint del pipeline de ingesta CryptoFlow.

Arranque:
    0. Espera a que Cassandra esté disponible (con timeout).
    1. Aplica schema en Cassandra (idempotente).
    2. Inicializa CassandraWriter.
    3. Arranca BinanceConsumer con el writer como handler.
    4. Shutdown limpio en Ctrl+C (Windows + Unix).

Uso:
    python main.py

Variables de entorno:
    CASSANDRA_HOSTS    (default: 127.0.0.1)
    CASSANDRA_PORT     (default: 9042)
    CASSANDRA_KEYSPACE (default: cryptoflow)
"""

import asyncio
import os
import signal
import socket
import time

from consumer.binance_ws import BinanceConsumer
from consumer.logger import get_logger
from storage.cassandra_writer import CassandraWriter
from storage.schema_manager import apply_schema
from storage import session as cassandra_session

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
    logger.info(
        "Esperando Cassandra en %s:%d (timeout=%ds)...", host, port, timeout_s
    )
    deadline = time.monotonic() + timeout_s
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        try:
            with socket.create_connection((host, port), timeout=2):
                logger.info(
                    "Cassandra disponible. host=%s port=%d intentos=%d",
                    host, port, attempt,
                )
                time.sleep(2.0)
                return
        except OSError:
            remaining = round(deadline - time.monotonic())
            logger.info(
                "Cassandra no lista. Reintento en %.0fs (intento=%d, restante=%ds)...",
                interval_s, attempt, remaining,
            )
            time.sleep(interval_s)

    raise RuntimeError(
        f"Cassandra no respondio en {timeout_s}s ({host}:{port}).\n"
        "Verifica con: docker ps && docker logs cryptoflow-cassandra"
    )


async def main() -> None:
    # 0. Esperar Cassandra
    wait_for_cassandra()

    # 1. Schema
    logger.info("Paso 1/3 — Aplicando schema Cassandra...")
    apply_schema()

    # 2. Writer
    logger.info("Paso 2/3 — Inicializando CassandraWriter...")
    writer = CassandraWriter()
    writer.start()

    # 3. Consumer
    logger.info("Paso 3/3 — Arrancando BinanceConsumer...")
    consumer = BinanceConsumer(handler=writer.write)

    # Manejo de Ctrl+C compatible con Windows y Unix.
    # add_signal_handler() solo existe en Unix (ProactorEventLoop de Windows
    # no lo implementa). signal.signal() funciona en ambos sistemas.
    def _shutdown(signum, frame):
        logger.info("Senal recibida (%d). Deteniendo consumer...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        await consumer.run()
    finally:
        logger.info("Shutdown: flusheando escrituras pendientes...")
        writer.stop()
        cassandra_session.close()
        logger.info("Shutdown completo.")


if __name__ == "__main__":
    asyncio.run(main())