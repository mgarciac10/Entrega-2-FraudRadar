"""bronze_to_silver: bronze/transactions -> silver/transactions y silver/transactions_rejected.

Batch incremental que dispara Airflow cada 15 min: lee solo lo nuevo de Bronze (availableNow) y
termina. Valida contra el contrato del evento, deduplica con MERGE por transaction_id y enriquece
con los catálogos (usuarios, comercios y ciudades), que también deja tipados en Silver.

Uso:
  spark-submit spark/batch/bronze_to_silver.py [--run-id <run_id de Airflow>]
"""
import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from schemas import ATTACK_PATTERNS, CHANNELS, REQUIRED_FIELDS, TX_TYPES, transaction_struct
from spark_session import checkpoint_uri, get_spark, lake_path


def latest_catalog(spark, name):
    df = spark.read.format("delta").load(lake_path("bronze", f"catalogs/{name}"))
    last_run = df.agg(F.max("_run_id")).first()[0]
    return df.filter(F.col("_run_id") == last_run)


def refresh_catalogs(spark):
    """Pasa la última corrida de cada catálogo a Silver con tipos. Son tablas pequeñas: se reescriben."""
    users = latest_catalog(spark, "usuarios").select(
        "user_id", "kyc_level", "segment", "home_city_code", F.to_date("registered_at").alias("registered_at"))
    merchants = latest_catalog(spark, "comercios").select(
        "merchant_id",
        F.col("name").alias("merchant_name"),
        F.col("category").alias("merchant_category"),
        F.col("city_code").alias("merchant_city_code"))
    cities = latest_catalog(spark, "ciudades").select(
        "city_code", "city", "department",
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        F.col("population_weight").cast("double").alias("population_weight"))

    for name, df in (("users", users), ("merchants", merchants), ("cities", cities)):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(lake_path("silver", name))


def reject_reason(e, raw):
    """Primer problema que tenga el evento, o nulo si es válido."""
    # from_json no falla con texto roto: devuelve campos nulos. json_object_keys sí da nulo
    checks = [(e.isNull() | F.json_object_keys(raw).isNull(), "json_invalido")]
    checks += [(e[f].isNull(), f"falta_{f}") for f in REQUIRED_FIELDS]
    checks += [
        (~e["tx_type"].isin(*TX_TYPES), "tx_type_invalido"),
        (~e["channel"].isin(*CHANNELS), "channel_invalido"),
        (e["amount_cop"] <= 0, "monto_no_positivo"),
        ((e["tx_type"] == "PAGO") & e["merchant_id"].isNull(), "pago_sin_comercio"),
        ((e["tx_type"] == "P2P") & e["counterparty_user_id"].isNull(), "p2p_sin_destinatario"),
        (e["attack_pattern"].isNotNull() & ~e["attack_pattern"].isin(*ATTACK_PATTERNS), "attack_pattern_invalido"),
    ]
    reason = F.lit(None).cast("string")
    for cond, label in reversed(checks):
        reason = F.when(cond, label).otherwise(reason)
    return reason


def merge_new(spark, df, path, condition, partition_by=None):
    """Inserta solo las filas cuya llave no existe todavía (reprocesar no duplica)."""
    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)
        return
    DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), condition).whenNotMatchedInsertAll().execute()


def process_batch(batch, batch_id, run_id):
    spark = batch.sparkSession
    users, merchants, cities = (
        spark.read.format("delta").load(lake_path("silver", name)) for name in ("users", "merchants", "cities"))
    parsed = (
        batch.withColumn("e", F.from_json("value", transaction_struct()))
        .withColumn("reject_reason", reject_reason(F.col("e"), F.col("value")))
        .persist()
    )

    rejected = parsed.filter(F.col("reject_reason").isNotNull()).select(
        "value", "reject_reason", "topic", "partition", "offset", "_session", "_ingested_at",
        F.current_timestamp().alias("_rejected_at"),
        F.lit(run_id).alias("_run_id"),
    )
    merge_new(spark, rejected, lake_path("silver", "transactions_rejected"),
              "t._session = s._session AND t.`partition` = s.`partition` AND t.`offset` = s.`offset`")

    tx = parsed.filter(F.col("reject_reason").isNull()).select(
        "e.*",
        F.col("_session").alias("kafka_session"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        "_ingested_at",
    )
    # El mismo evento puede llegar dos veces (reintento del productor): se queda el primero
    first = Window.partitionBy("transaction_id").orderBy("_ingested_at", "kafka_partition", "kafka_offset")
    tx = tx.withColumn("_rn", F.row_number().over(first)).filter("_rn = 1").drop("_rn")

    silver = (
        tx.join(F.broadcast(users), "user_id", "left")
        .join(F.broadcast(merchants), "merchant_id", "left")
        .join(F.broadcast(cities.select("city_code", "city", "department")), "city_code", "left")
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )
    merge_new(spark, silver, lake_path("silver", "transactions"),
              "t.event_date = s.event_date AND t.transaction_id = s.transaction_id", partition_by="event_date")

    n_rejected, n_valid = rejected.count(), tx.count()
    parsed.unpersist()
    print(f"lote {batch_id}: {n_valid} válidas, {n_rejected} rechazadas")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H:%M:%S"))
    args = p.parse_args()

    spark = get_spark("bronze_to_silver")
    spark.sparkContext.setLogLevel("WARN")

    refresh_catalogs(spark)
    query = (
        spark.readStream.format("delta").load(lake_path("bronze", "transactions"))
        .writeStream.foreachBatch(lambda df, batch_id: process_batch(df, batch_id, args.run_id))
        .option("checkpointLocation", f"{checkpoint_uri()}/bronze_to_silver")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
