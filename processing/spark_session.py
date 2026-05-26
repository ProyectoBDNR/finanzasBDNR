"""
processing/spark_session.py
----------------------------
SparkSession configurada para Cassandra con soporte RBAC.

Autenticación:
  Spark usa el rol cf_analyst (solo lectura en raw, lectura+escritura en OHLCV).
  Configurado via CASSANDRA_ANALYST_USER / CASSANDRA_ANALYST_PASSWORD.
  Si no están definidas, conecta sin autenticación (dev local).
"""

import os
from pyspark.sql import SparkSession

CASSANDRA_HOST  = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")[0]
CASSANDRA_PORT  = os.getenv("CASSANDRA_PORT", "9042")
SPARK_MASTER    = os.getenv("SPARK_MASTER", "local[*]")

# Credenciales para cf_analyst (Spark)
CASSANDRA_USER  = os.getenv("CASSANDRA_ANALYST_USER", "")
CASSANDRA_PWD   = os.getenv("CASSANDRA_ANALYST_PASSWORD", "")

_CONNECTOR_JAR = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0"


def get_spark(app_name: str = "cryptoflow") -> SparkSession:
    """Retorna SparkSession lista para Cassandra. Reutiliza sesión existente."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars.packages",            _CONNECTOR_JAR)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .config("spark.sql.extensions",
                "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.cassandra.input.fetch.sizeInRows",      "10000")
        .config("spark.cassandra.output.batch.size.rows",      "auto")
        .config("spark.cassandra.output.concurrent.writes",    "5")
        .config("spark.cassandra.output.batch.grouping.key",   "partition")
        .config("spark.sql.legacy.timeParserPolicy",           "LEGACY")
    )

    # Agregar credenciales RBAC si están configuradas
    if CASSANDRA_USER and CASSANDRA_PWD:
        builder = (
            builder
            .config("spark.cassandra.auth.username", CASSANDRA_USER)
            .config("spark.cassandra.auth.password", CASSANDRA_PWD)
        )

    return builder.getOrCreate()