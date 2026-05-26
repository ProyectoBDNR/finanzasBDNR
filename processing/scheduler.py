"""
processing/scheduler.py
------------------------
Scheduler que ejecuta el Spark processing job automáticamente cada N minutos.

Se ejecuta como thread paralelo al consumer WebSocket en main.py.
Garantiza que los datos OHLCV en Cassandra siempre estén actualizados
sin intervención manual.

Diseño:
  - No usa Kafka ni Spark Streaming — el proyecto es batch por diseño.
  - Ejecuta el job via subprocess para aislar el proceso de Spark del
    proceso principal del consumer (evita conflictos de JVM/ClassLoader).
  - Si un batch falla, loguea el error y espera al siguiente ciclo
    sin matar el pipeline de ingesta.

Variables de entorno:
  SPARK_BATCH_INTERVAL_MINUTES  (default: 5)
  CASSANDRA_HOSTS               (default: 127.0.0.1)
  CASSANDRA_ANALYST_USER        (optional, para RBAC)
  CASSANDRA_ANALYST_PASSWORD    (optional, para RBAC)
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from consumer.logger import get_logger

logger = get_logger("spark.scheduler")

INTERVAL_MINUTES = int(os.getenv("SPARK_BATCH_INTERVAL_MINUTES", "5"))
PROJECT_ROOT     = Path(__file__).parent.parent
RUN_SPARK_SH     = PROJECT_ROOT / "run_spark.sh"
JOB_SCRIPT       = PROJECT_ROOT / "processing" / "job.py"


def _run_batch(date: str) -> bool:
    """
    Ejecuta el Spark job para la fecha dada.
    Retorna True si el job completó exitosamente.
    """
    logger.info("Iniciando batch de Spark para fecha=%s", date)

    env = os.environ.copy()
    # Python (Windows) usa 127.0.0.1 para conectar a Cassandra via puerto mapeado.
    # Spark (dentro de Docker) necesita el hostname interno de Docker: cassandra-1.
    # CASSANDRA_SPARK_HOSTS permite configurar ambos de forma independiente.
    spark_host = os.getenv("CASSANDRA_SPARK_HOSTS", "cassandra-1")
    env["CASSANDRA_HOSTS"] = spark_host

    # Pasar credenciales de cf_analyst a Spark si están configuradas
    analyst_user = os.getenv("CASSANDRA_ANALYST_USER")
    analyst_pwd  = os.getenv("CASSANDRA_ANALYST_PASSWORD")
    if analyst_user:
        env["CASSANDRA_ANALYST_USER"]     = analyst_user
        env["CASSANDRA_ANALYST_PASSWORD"] = analyst_pwd or ""

    try:
        # Detectar si estamos dentro de Docker leyendo /.dockerenv
        in_docker = Path("/.dockerenv").exists()

        if in_docker:
            # Dentro de Docker: usar run_spark.sh directamente
            cmd = [str(RUN_SPARK_SH), str(JOB_SCRIPT), "--date", date]
        else:
            # Windows / macOS: lanzar el job dentro del contenedor Spark.
            # Las credenciales de Cassandra se pasan como --conf de spark-submit
            # para que el conector las recoja directamente, sin depender de
            # variables de entorno dentro del contenedor.
            analyst_user = os.getenv("CASSANDRA_ANALYST_USER", "")
            analyst_pwd  = os.getenv("CASSANDRA_ANALYST_PASSWORD", "")

            cmd = [
                "docker", "exec",
                "-e", "PYTHONPATH=/app",
                "cryptoflow-spark",
                "/opt/spark/bin/spark-submit",
                "--master", "local[*]",
                "--packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0",
                "--conf", "spark.sql.shuffle.partitions=3",
                "--conf", "spark.cassandra.connection.host=cassandra",
                "--conf", "spark.cassandra.connection.port=9042",
            ]
            if analyst_user:
                cmd += [
                    "--conf", f"spark.cassandra.auth.username={analyst_user}",
                    "--conf", f"spark.cassandra.auth.password={analyst_pwd}",
                ]
            cmd += [
                "--conf", "spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions",
                "/app/processing/job.py",
                "--date", date,
            ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,   # 5 minutos máximo por batch
            env=env,
            cwd=str(PROJECT_ROOT),
        )

        if result.returncode == 0:
            logger.info("Batch completado exitosamente para fecha=%s", date)
            return True
        else:
            logger.error(
                "Batch falló para fecha=%s. returncode=%d stderr=%s",
                date, result.returncode,
                result.stderr[-500:] if result.stderr else "sin stderr"
            )
            return False

    except subprocess.TimeoutExpired:
        logger.error("Batch excedió timeout de 300s para fecha=%s", date)
        return False
    except Exception as e:
        logger.error("Error inesperado en batch: %s", e)
        return False


def _scheduler_loop(stop_event: threading.Event) -> None:
    """
    Loop principal del scheduler. Corre indefinidamente hasta que
    stop_event sea señalizado (en shutdown del sistema).
    """
    logger.info(
        "Scheduler iniciado. Intervalo: %d minutos. "
        "Primer batch en %d minutos.",
        INTERVAL_MINUTES, INTERVAL_MINUTES
    )

    # Esperar el primer intervalo antes de correr (dar tiempo al consumer
    # de acumular datos suficientes para el primer batch)
    next_run = time.monotonic() + INTERVAL_MINUTES * 60

    while not stop_event.is_set():
        now = time.monotonic()

        if now >= next_run:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            success = _run_batch(date)

            if success:
                logger.info(
                    "Próximo batch en %d minutos.", INTERVAL_MINUTES
                )
            else:
                logger.warning(
                    "Batch falló — se reintentará en %d minutos.", INTERVAL_MINUTES
                )

            next_run = time.monotonic() + INTERVAL_MINUTES * 60

        # Sleep corto para responder rápido al stop_event
        stop_event.wait(timeout=10)


def start_scheduler() -> tuple[threading.Thread, threading.Event]:
    """
    Inicia el scheduler como daemon thread.

    Returns:
        (thread, stop_event)
        Llamar stop_event.set() para detener el scheduler limpiamente.
    """
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_scheduler_loop,
        args=(stop_event,),
        daemon=True,
        name="spark-scheduler",
    )
    thread.start()
    logger.info("Thread spark-scheduler iniciado.")
    return thread, stop_event