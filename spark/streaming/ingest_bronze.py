"""ingest_bronze: topic transactions (Kafka/MSK) -> <lake>/bronze/transactions.

Job de streaming continuo. Guarda el mensaje tal cual llega (el JSON como texto) con su topic,
partición, offset y hora de ingesta. El parseo y la validación se hacen en bronze_to_silver.

Uso:
  spark-submit spark/streaming/ingest_bronze.py [--session 2026-11-07] [--trigger "15 seconds"]
"""
import argparse
import os

from pyspark.sql import functions as F

from spark_session import checkpoint_uri, get_spark, lake_path


def read_kafka(spark, starting_offsets):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .option("kafka.security.protocol", os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
        .option("subscribe", os.environ.get("KAFKA_TOPIC", "transactions"))
        .option("startingOffsets", starting_offsets)
        .load()
    )


def to_bronze(kafka_df, session):
    # Los offsets vuelven a 0 con cada MSK nuevo: (_session, partition, offset) es la llave única
    return kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.lit(session).alias("_session"),
        F.current_timestamp().alias("_ingested_at"),
    ).withColumn("ingest_date", F.to_date("_ingested_at"))


def main():
    p = argparse.ArgumentParser()
    # MSK se recrea en cada sesión: un checkpoint viejo apunta a offsets que ya no existen
    p.add_argument("--session", default=os.environ.get("STREAM_SESSION", "local"))
    p.add_argument("--trigger", default=os.environ.get("BRONZE_TRIGGER", "15 seconds"))
    p.add_argument("--starting-offsets", default="earliest")
    args = p.parse_args()

    spark = get_spark("ingest_bronze")
    spark.sparkContext.setLogLevel("WARN")

    target = lake_path("bronze", "transactions")
    checkpoint = f"{checkpoint_uri()}/ingest_bronze/{args.session}"
    print(f"ingest_bronze -> {target} (checkpoint {checkpoint}, trigger {args.trigger})")

    query = (
        to_bronze(read_kafka(spark, args.starting_offsets), args.session)
        .writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=args.trigger)
        .start(target)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
