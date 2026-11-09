"""Construye dimensiones y hechos Gold desde Silver y el estado de alertas.

Las dimensiones son tipo 1. Los hechos se fusionan por sus llaves de negocio, de modo que
reintentar el mismo DAG actualiza las filas sin duplicarlas.

Uso:
  spark-submit spark/batch/silver_to_gold.py [--run-id <run_id>]
  spark-submit spark/batch/silver_to_gold.py --dimensions-only
  spark-submit spark/batch/silver_to_gold.py --facts-only
"""
import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from schemas import ATTACK_PATTERNS, CHANNELS
from spark_session import get_spark, lake_path


def read_silver(spark, name):
    path = lake_path("silver", name)
    if not DeltaTable.isDeltaTable(spark, path):
        raise FileNotFoundError(f"Falta la tabla Silver requerida: {path}")
    return spark.read.format("delta").load(path)


def _hashed_key(column):
    return F.sha2(column.cast("string"), 256)


def upsert(spark, name, dataframe, key, run_id, partition_by=None):
    path = lake_path("gold", name)
    source = (
        dataframe.dropDuplicates([key])
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_updated_at", F.current_timestamp())
    )
    if not DeltaTable.isDeltaTable(spark, path):
        writer = source.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)
        return
    (
        DeltaTable.forPath(spark, path).alias("target")
        .merge(source.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _first_non_null(name):
    return F.first(F.col(name), ignorenulls=True).alias(name)


def build_dimensions(spark, transactions, alerts, run_id):
    users = read_silver(spark, "users").select(
        "user_id", "kyc_level", "segment", "registered_at", "home_city_code"
    )
    user_rows = (
        users.unionByName(
            transactions.select("user_id").distinct()
            .withColumn("kyc_level", F.lit(None).cast("string"))
            .withColumn("segment", F.lit(None).cast("string"))
            .withColumn("registered_at", F.lit(None).cast("date"))
            .withColumn("home_city_code", F.lit(None).cast("string"))
        )
        .unionByName(
            alerts.select("user_id").distinct()
            .withColumn("kyc_level", F.lit(None).cast("string"))
            .withColumn("segment", F.lit(None).cast("string"))
            .withColumn("registered_at", F.lit(None).cast("date"))
            .withColumn("home_city_code", F.lit(None).cast("string"))
        )
        .groupBy("user_id")
        .agg(*[_first_non_null(name) for name in ("kyc_level", "segment", "registered_at", "home_city_code")])
        .withColumn("user_key", _hashed_key(F.col("user_id")))
        .select("user_key", "user_id", "kyc_level", "segment", "registered_at", "home_city_code")
    )
    upsert(spark, "dim_user", user_rows, "user_key", run_id)

    merchants = read_silver(spark, "merchants").select(
        "merchant_id", "merchant_name", "merchant_category", "merchant_city_code"
    )
    merchant_rows = (
        merchants.unionByName(
            transactions.filter(F.col("merchant_id").isNotNull()).select("merchant_id").distinct()
            .withColumn("merchant_name", F.lit(None).cast("string"))
            .withColumn("merchant_category", F.lit(None).cast("string"))
            .withColumn("merchant_city_code", F.lit(None).cast("string"))
        )
        .groupBy("merchant_id")
        .agg(*[_first_non_null(name) for name in ("merchant_name", "merchant_category", "merchant_city_code")])
        .withColumn("merchant_key", _hashed_key(F.col("merchant_id")))
        .select("merchant_key", "merchant_id", "merchant_name", "merchant_category", "merchant_city_code")
    )
    upsert(spark, "dim_merchant", merchant_rows, "merchant_key", run_id)

    cities = read_silver(spark, "cities").select("city_code", "city", "department", "lat", "lon")
    location_rows = (
        cities.unionByName(
            transactions.select("city_code", "city", "department")
            .withColumn("lat", F.lit(None).cast("double"))
            .withColumn("lon", F.lit(None).cast("double"))
        )
        .unionByName(
            spark.createDataFrame(
                [("UNKNOWN", "Desconocida", "Desconocido", None, None)],
                "city_code string, city string, department string, lat double, lon double",
            )
        )
        .groupBy("city_code")
        .agg(*[_first_non_null(name) for name in ("city", "department", "lat", "lon")])
        .withColumn("location_key", _hashed_key(F.col("city_code")))
        .select("location_key", "city_code", "city", "department", "lat", "lon")
    )
    upsert(spark, "dim_location", location_rows, "location_key", run_id)

    channel_rows = (
        spark.createDataFrame([(value,) for value in CHANNELS], "channel string")
        .unionByName(transactions.select("channel").distinct())
        .distinct()
        .withColumn("channel_key", _hashed_key(F.col("channel")))
        .select("channel_key", "channel")
    )
    upsert(spark, "dim_channel", channel_rows, "channel_key", run_id)

    rule_rows = (
        spark.createDataFrame([(value,) for value in ATTACK_PATTERNS], "rule string")
        .unionByName(alerts.select("rule").distinct())
        .distinct()
        .withColumn("rule_key", _hashed_key(F.col("rule")))
        .select("rule_key", "rule")
    )
    upsert(spark, "dim_rule", rule_rows, "rule_key", run_id)

    event_times = transactions.select(F.col("event_ts").alias("event_ts")).unionByName(
        alerts.select(F.col("trigger_event_ts").alias("event_ts"))
    ).filter(F.col("event_ts").isNotNull())
    date_rows = (
        event_times.select(F.to_date("event_ts").alias("date"))
        .distinct()
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .select("date_key", "date", "year", "quarter", "month", "day", "day_of_week", "day_name", "is_weekend")
    )
    upsert(spark, "dim_date", date_rows, "date_key", run_id)

    hour = F.hour("event_ts")
    minute = F.minute("event_ts")
    time_rows = (
        event_times.select((F.hour("event_ts") * 100 + F.minute("event_ts")).alias("time_key"), hour.alias("hour"), minute.alias("minute"))
        .distinct()
        .withColumn(
            "time_band",
            F.when(F.col("hour") < 6, "madrugada")
            .when(F.col("hour") < 12, "mañana")
            .when(F.col("hour") < 18, "tarde")
            .otherwise("noche"),
        )
        .select("time_key", "hour", "minute", "time_band")
    )
    upsert(spark, "dim_time", time_rows, "time_key", run_id)


def build_fact_transaction(spark, transactions, alerts, run_id):
    flagged = (
        alerts.select(F.explode("transaction_ids").alias("transaction_id"))
        .filter(F.col("transaction_id").isNotNull())
        .distinct()
        .withColumn("_is_flagged", F.lit(True))
    )
    rows = (
        transactions
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("date_key", F.date_format("event_ts", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour("event_ts") * 100 + F.minute("event_ts"))
        .join(flagged, "transaction_id", "left")
        .withColumn("is_flagged", F.coalesce(F.col("_is_flagged"), F.lit(False)))
        .join(spark.read.format("delta").load(lake_path("gold", "dim_user")).select("user_id", "user_key"),
              "user_id", "left")
        .join(spark.read.format("delta").load(lake_path("gold", "dim_merchant")).select("merchant_id", "merchant_key"),
              "merchant_id", "left")
        .join(spark.read.format("delta").load(lake_path("gold", "dim_location")).select("city_code", "location_key"),
              "city_code", "left")
        .join(spark.read.format("delta").load(lake_path("gold", "dim_channel")).select("channel", "channel_key"),
              "channel", "left")
        .select(
            "transaction_id", "event_date", "event_ts", "date_key", "time_key",
            "user_key", "merchant_key", "location_key", "channel_key",
            "tx_type", "amount_cop", "is_flagged", "is_fraud_injected", "attack_id", "attack_pattern",
            "kafka_session", "kafka_partition", "kafka_offset",
        )
    )
    upsert(spark, "fact_transaction", rows, "transaction_id", run_id, "event_date")


def build_fact_alert(spark, transactions, alerts, run_id):
    exploded = alerts.select(
        "alert_id", "trigger_event_ts", F.explode_outer("transaction_ids").alias("transaction_id")
    )
    candidates = (
        exploded.join(
            transactions.select("transaction_id", "event_ts", "city_code"),
            "transaction_id",
            "left",
        )
        .withColumn(
            "_distance",
            F.abs(F.unix_timestamp("event_ts") - F.unix_timestamp("trigger_event_ts")),
        )
    )
    nearest = Window.partitionBy("alert_id").orderBy(F.col("_distance").asc_nulls_last(), F.col("event_ts").desc_nulls_last())
    alert_locations = (
        candidates.withColumn("_rn", F.row_number().over(nearest))
        .filter(F.col("_rn") == 1)
        .select("alert_id", F.coalesce("city_code", F.lit("UNKNOWN")).alias("city_code"))
    )
    rows = (
        alerts.join(alert_locations, "alert_id", "left")
        .withColumn("event_date", F.to_date("trigger_event_ts"))
        .withColumn("date_key", F.date_format("trigger_event_ts", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.hour("trigger_event_ts") * 100 + F.minute("trigger_event_ts"))
        .withColumn("detection_latency_s", F.unix_timestamp("created_at") - F.unix_timestamp("trigger_event_ts"))
        .withColumn("n_transactions", F.coalesce(F.size("transaction_ids"), F.lit(0)))
        .withColumn("user_key", _hashed_key(F.col("user_id")))
        .withColumn("rule_key", _hashed_key(F.col("rule")))
        .withColumn("location_key", _hashed_key(F.coalesce(F.col("city_code"), F.lit("UNKNOWN"))))
        .select(
            "alert_id", "event_date", "trigger_event_ts", "window_start", "date_key", "time_key",
            "user_key", "rule_key", "location_key", "status", "amount_at_risk_cop",
            "n_transactions", "detection_latency_s", "transaction_ids",
        )
    )
    upsert(spark, "fact_alert", rows, "alert_id", run_id, "event_date")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dimensions-only", action="store_true")
    mode.add_argument("--facts-only", action="store_true")
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H:%M:%S"))
    args = parser.parse_args()

    spark = get_spark("silver_to_gold")
    spark.sparkContext.setLogLevel("WARN")
    transactions = read_silver(spark, "transactions")
    alerts = read_silver(spark, "alert_status")

    if not args.facts_only:
        build_dimensions(spark, transactions, alerts, args.run_id)
    if not args.dimensions_only:
        build_fact_transaction(spark, transactions, alerts, args.run_id)
        build_fact_alert(spark, transactions, alerts, args.run_id)
    for name in ("dim_date", "dim_time", "dim_user", "dim_merchant", "dim_location", "dim_channel", "dim_rule",
                 "fact_transaction", "fact_alert"):
        path = lake_path("gold", name)
        if DeltaTable.isDeltaTable(spark, path):
            print(f"gold/{name}: {spark.read.format('delta').load(path).count()} filas")
    spark.stop()


if __name__ == "__main__":
    main()
