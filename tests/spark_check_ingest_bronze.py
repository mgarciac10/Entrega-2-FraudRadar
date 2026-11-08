"""Verifica que Bronze tiene exactamente lo que hay en Kafka: ni pérdidas ni duplicados.

Se corre con el compose arriba y el productor DETENIDO (si sigue escribiendo, los conteos no cuadran).
Sirve también para la prueba de matar ingest-bronze y reiniciarlo (RNF-03):
  docker compose --profile spark exec -T spark-master /opt/spark/bin/spark-submit
    --master local[2] /opt/fraudradar/tests/spark_check_ingest_bronze.py [--session local]
"""
import argparse
import os
import sys

from pyspark.sql import functions as F

from spark_session import get_spark, lake_path

p = argparse.ArgumentParser()
p.add_argument("--session", default=os.environ.get("STREAM_SESSION", "local"))
args = p.parse_args()

spark = get_spark("check_ingest_bronze")
spark.sparkContext.setLogLevel("ERROR")

kafka = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    .option("kafka.security.protocol", os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    .option("subscribe", os.environ.get("KAFKA_TOPIC", "transactions"))
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .select("partition", "offset")
)
bronze = spark.read.format("delta").load(lake_path("bronze", "transactions")) \
    .filter(F.col("_session") == args.session).select("partition", "offset")

n_kafka, n_bronze = kafka.count(), bronze.count()
n_distinct = bronze.distinct().count()
missing = kafka.subtract(bronze).count()

print(f"Kafka: {n_kafka} mensajes · Bronze (sesión {args.session}): {n_bronze} filas, {n_distinct} offsets distintos")
ok = n_bronze == n_distinct and missing == 0 and n_bronze == n_kafka
print("OK    sin duplicados ni pérdidas" if ok else f"FALLA duplicados={n_bronze - n_distinct}, faltantes={missing}")
spark.stop()
sys.exit(0 if ok else 1)
