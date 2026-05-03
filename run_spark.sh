#!/bin/bash
# Script de arranque para Spark dentro del contenedor Docker.
# Agrega /app al PYTHONPATH antes de llamar spark-submit,
# lo que hace que todos los módulos del proyecto sean importables.

export PYTHONPATH=/app:$PYTHONPATH

/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
  --conf spark.sql.shuffle.partitions=3 \
  "$@"
