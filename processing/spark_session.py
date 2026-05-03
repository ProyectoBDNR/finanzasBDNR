"""
processing/spark_session.py
----------------------------
Factoría de SparkSession configurada para leer/escribir en Cassandra
usando spark-cassandra-connector 3.4.

Decisiones:
  - Versión fijada del conector (3.4) para evitar incompatibilidades con Spark 3.4.x
  - LOCAL[*] en modo local; sobreescribible por SPARK_MASTER env
  - Parámetros de throughput: fetchSizeInRows + readTimeoutMS calibrados
    para lecturas batch, no streaming
  - spark.sql.extensions registra el CassandraSQLContext que habilita
    la sintaxis df.write.cassandraFormat(...)
"""

import os
from pyspark.sql import SparkSession

CASSANDRA_HOST = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")[0]
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")

# Coordenadas Maven del conector — NUNCA cambiar sin revisar compatibilidad
_CONNECTOR_JAR = (
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0"
)


def get_spark(app_name: str = "cryptoflow") -> SparkSession:
    """
    Retorna una SparkSession lista para usar con Cassandra.
    Si ya existe una sesión activa, la reutiliza (patrón Spark estándar).
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        # ── Conector Cassandra ──────────────────────────────────────────
        .config("spark.jars.packages", _CONNECTOR_JAR)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .config(
            "spark.sql.extensions",
            "com.datastax.spark.connector.CassandraSparkExtensions",
        )
        # ── Lectura: fetches grandes reducen round-trips ────────────────
        .config("spark.cassandra.input.fetch.sizeInRows", "10000")
        # ── Escritura: throughput sin saturar el coordinador ───────────
        .config("spark.cassandra.output.batch.size.rows", "auto")
        .config("spark.cassandra.output.concurrent.writes", "5")
        .config("spark.cassandra.output.batch.grouping.key", "partition")
        # ── SQL: evitar warnings de fecha/hora en Spark 3.x ───────────
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )