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
    T.StructField("_processed_at", T.TimestampType(), False),  # lo pone bronze_to_silver en cada lote
])
P0, P1, P2 = datetime(2026, 11, 8, 10, 15), datetime(2026, 11, 8, 10, 30), datetime(2026, 11, 9, 3, 15)
transactions = spark.createDataFrame(
    [
        ("tx1", datetime(2026, 11, 8, 10, 0), "u1", "P2P", "app", 1000, None,
         "C1", "Medellin", "Antioquia", True, "atk1", "rafaga", "local", 0, 1, P0),
        ("tx2", datetime(2026, 11, 8, 10, 1), "u1", "P2P", "app", 2000, None,
         "C1", "Medellin", "Antioquia", True, "atk1", "rafaga", "local", 0, 2, P0),
        ("tx3", datetime(2026, 11, 8, 10, 2), "u2", "PAGO", "qr", 50000, "M1",
         "C2", "Bogota", "Cundinamarca", False, None, None, "local", 1, 1, P1),
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
def write_alerts(items):
    rows = [normalize_alert(item) for item in items]
    (
        spark.createDataFrame(rows, ALERT_SCHEMA).withColumn("_run_id", F.lit("test"))
        .withColumn("_extracted_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").save(f"{LAKE}/silver/alert_status")
    )


write_alerts(alerts)

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

# Hora de Colombia: tx1 es 10:00 UTC = 05:00 en Bogotá (madrugada), no 10:00 (mañana)
tx1 = fact_tx.filter("transaction_id = 'tx1'").first()
dim_time = spark.read.format("delta").load(f"{LAKE}/gold/dim_time")
check(tx1["time_key"] == 500 and dim_time.filter("time_key = 500").first()["time_band"] == "madrugada",
      f"time_key y franja en hora de Colombia (time_key={tx1['time_key']})")

out = submit("batch/silver_to_gold.py", "--run-id", "gold-test-retry")
check(
    spark.read.format("delta").load(f"{LAKE}/gold/fact_transaction").count() == 3
    and spark.read.format("delta").load(f"{LAKE}/gold/fact_alert").count() == 2,
    "reintentar silver_to_gold no duplica filas",
)
check("1 transacciones pendientes" in out, "el reintento solo relee el último lote de Silver, no toda la historia")

# Corrida incremental: llega tx4 (otro lote de Silver) y una alerta nueva que referencia tx2, ya cargada.
# tx4 es 03:00 UTC del 9 = 22:00 del 8 en Bogotá: su fecha local es el 8 y su franja, noche.
spark.createDataFrame(
    [("tx4", datetime(2026, 11, 9, 3, 0), "u2", "P2P", "app", 3000, None,
      "C2", "Bogota", "Cundinamarca", False, None, None, "local", 1, 2, P2)],
    tx_schema,
).withColumn("event_date", F.to_date("event_ts")).write.format("delta").mode("append").save(
    f"{LAKE}/silver/transactions")
write_alerts(alerts + [{
    "alert_id": "alert3", "user_id": "u1", "rule": "rafaga",
    "window_start": "2026-11-08T10:01:00Z", "status": "abierta",
    "amount_at_risk_cop": 2000, "transaction_ids": ["tx2"],
    "trigger_event_ts": "2026-11-08T10:01:00Z", "created_at": "2026-11-08T10:01:30Z", "evidence": {},
}])
out = submit("batch/silver_to_gold.py", "--run-id", "gold-test-incremental")
fact_tx = spark.read.format("delta").load(f"{LAKE}/gold/fact_transaction")
check("2 transacciones pendientes" in out, "la corrida incremental toma tx3 (último lote) y tx4, no tx1 ni tx2")
check(fact_tx.count() == 4, f"fact_transaction suma la transacción nueva ({fact_tx.count()})")
check(fact_tx.filter("transaction_id = 'tx2'").first()["is_flagged"],
      "una alerta posterior marca una transacción ya cargada en Gold")
check(fact_tx.filter("transaction_id = 'tx1'").first()["_run_id"] == "gold-test",
      "tx1 no se reescribió en corridas posteriores")
tx4 = fact_tx.filter("transaction_id = 'tx4'").first()
check(str(tx4["event_date"]) == "2026-11-08" and tx4["date_key"] == 20261108 and tx4["time_key"] == 2200,
      f"fecha local de Colombia: {tx4['event_date']}, {tx4['date_key']}, {tx4['time_key']}")
check(spark.read.format("delta").load(f"{LAKE}/gold/fact_alert").count() == 3, "fact_alert suma la alerta nueva")
submit("batch/data_quality_checks.py", "--run-id", "gold-test-incremental")
check(
    DeltaTable.isDeltaTable(spark, f"{LAKE}/gold/fact_transaction"),
    "fact_transaction es una tabla Delta",
)

spark.stop()
print("\nTodo bien." if not failures else f"\n{len(failures)} verificaciones fallidas.")
sys.exit(1 if failures else 0)
