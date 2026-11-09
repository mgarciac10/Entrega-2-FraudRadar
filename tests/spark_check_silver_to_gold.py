"""Verifica Gold, el estado de alertas y los chequeos de calidad en Delta local."""
import os
import subprocess
import sys
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

from extract_alert_status import ALERT_SCHEMA, normalize_alert
from spark_session import get_spark

LAKE = os.environ["LAKE_URI"]
SPARK_DIR = "/opt/fraudradar/spark"
failures = []


def check(condition, message):
    print(("OK    " if condition else "FALLA ") + message)
    if not condition:
        failures.append(message)


def submit(script, *args):
    result = subprocess.run(
        ["/opt/spark/bin/spark-submit", "--master", "local[2]", f"{SPARK_DIR}/{script}", *args],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(result.stdout[-3000:], result.stderr[-3000:])
        sys.exit(f"{script} falló")
    return result.stdout


spark = get_spark("check_silver_to_gold")
spark.sparkContext.setLogLevel("ERROR")

users = spark.createDataFrame(
    [
        ("u1", "nivel_2", "frecuente", datetime(2025, 1, 1).date(), "C1"),
        ("u2", "nivel_1", "nuevo", datetime(2026, 1, 1).date(), "C2"),
    ],
    "user_id string, kyc_level string, segment string, registered_at date, home_city_code string",
)
merchants = spark.createDataFrame(
    [("M1", "Tienda", "supermercado", "C2")],
    "merchant_id string, merchant_name string, merchant_category string, merchant_city_code string",
)
cities = spark.createDataFrame(
    [
        ("C1", "Medellin", "Antioquia", 6.25, -75.56),
        ("C2", "Bogota", "Cundinamarca", 4.71, -74.07),
    ],
    "city_code string, city string, department string, lat double, lon double",
)
for name, dataframe in (("users", users), ("merchants", merchants), ("cities", cities)):
    dataframe.write.format("delta").mode("overwrite").save(f"{LAKE}/silver/{name}")

tx_schema = T.StructType([
    T.StructField("transaction_id", T.StringType(), False),
    T.StructField("event_ts", T.TimestampType(), False),
    T.StructField("user_id", T.StringType(), False),
    T.StructField("tx_type", T.StringType(), False),
    T.StructField("channel", T.StringType(), False),
    T.StructField("amount_cop", T.LongType(), False),
    T.StructField("merchant_id", T.StringType(), True),
    T.StructField("city_code", T.StringType(), False),
    T.StructField("city", T.StringType(), True),
    T.StructField("department", T.StringType(), True),
    T.StructField("is_fraud_injected", T.BooleanType(), False),
    T.StructField("attack_id", T.StringType(), True),
    T.StructField("attack_pattern", T.StringType(), True),
    T.StructField("kafka_session", T.StringType(), False),
    T.StructField("kafka_partition", T.IntegerType(), False),
    T.StructField("kafka_offset", T.LongType(), False),
])
transactions = spark.createDataFrame(
    [
        ("tx1", datetime(2026, 11, 8, 10, 0), "u1", "P2P", "app", 1000, None,
         "C1", "Medellin", "Antioquia", True, "atk1", "rafaga", "local", 0, 1),
        ("tx2", datetime(2026, 11, 8, 10, 1), "u1", "P2P", "app", 2000, None,
         "C1", "Medellin", "Antioquia", True, "atk1", "rafaga", "local", 0, 2),
        ("tx3", datetime(2026, 11, 8, 10, 2), "u2", "PAGO", "qr", 50000, "M1",
         "C2", "Bogota", "Cundinamarca", False, None, None, "local", 1, 1),
    ],
    tx_schema,
)
transactions.withColumn("event_date", F.to_date("event_ts")).write.format("delta").mode("overwrite").save(
    f"{LAKE}/silver/transactions"
)

alerts = [
    {
        "alert_id": "alert1", "user_id": "u1", "rule": "rafaga",
        "window_start": "2026-11-08T10:00:00Z", "status": "confirmada",
        "amount_at_risk_cop": 1000, "transaction_ids": ["tx1"],
        "trigger_event_ts": "2026-11-08T10:00:00Z", "created_at": "2026-11-08T10:00:01Z",
        "evidence": {"test": "true"},
    },
    {
        "alert_id": "alert2", "user_id": "u2", "rule": "monto_atipico",
        "window_start": "2026-11-08T10:02:00Z", "status": "descartada",
        "amount_at_risk_cop": 50000, "transaction_ids": [],
        "trigger_event_ts": "2026-11-08T10:02:00Z", "created_at": "2026-11-08T10:02:01Z",
        "evidence": {},
    },
]
alert_rows = [normalize_alert(item) for item in alerts]
alert_df = spark.createDataFrame(alert_rows, ALERT_SCHEMA)
(
    alert_df.withColumn("_run_id", F.lit("test"))
    .withColumn("_extracted_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").save(f"{LAKE}/silver/alert_status")
)

submit("batch/silver_to_gold.py", "--run-id", "gold-test")
fact_tx = spark.read.format("delta").load(f"{LAKE}/gold/fact_transaction")
fact_alert = spark.read.format("delta").load(f"{LAKE}/gold/fact_alert")
dim_location = spark.read.format("delta").load(f"{LAKE}/gold/dim_location")
check(fact_tx.count() == 3, "fact_transaction contiene las 3 transacciones")
check(
    fact_tx.filter("transaction_id = 'tx1'").first()["kafka_offset"] == 1,
    "fact_transaction conserva el offset de Kafka para reconstruir linaje",
)
check(fact_alert.count() == 2, "fact_alert contiene las dos alertas y sus estados")
check(fact_tx.filter("transaction_id = 'tx1'").first()["is_flagged"], "la transacción incluida en una alerta queda marcada")
check(not fact_tx.filter("transaction_id = 'tx2'").first()["is_flagged"], "una transacción del ataque no alertada no queda marcada")
check(
    fact_alert.filter("alert_id = 'alert1'").first()["location_key"]
    == dim_location.filter("city_code = 'C1'").first()["location_key"],
    "la alerta se vincula a la ubicación de su transacción disparadora",
)
check(
    spark.read.format("delta").load(f"{LAKE}/gold/dim_rule").count() == 4,
    "dim_rule conserva las cuatro reglas aunque alguna no tenga alertas",
)
submit("batch/data_quality_checks.py", "--run-id", "gold-test")

submit("batch/silver_to_gold.py", "--run-id", "gold-test-retry")
check(
    spark.read.format("delta").load(f"{LAKE}/gold/fact_transaction").count() == 3
    and spark.read.format("delta").load(f"{LAKE}/gold/fact_alert").count() == 2,
    "reintentar silver_to_gold no duplica filas",
)
check(
    DeltaTable.isDeltaTable(spark, f"{LAKE}/gold/fact_transaction"),
    "fact_transaction es una tabla Delta",
)

spark.stop()
print("\nTodo bien." if not failures else f"\n{len(failures)} verificaciones fallidas.")
sys.exit(1 if failures else 0)
