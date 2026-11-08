"""Verifica bronze_to_silver sobre un lakehouse local, sin Kafka. Corre DENTRO del contenedor de Spark.

Arma a mano una tabla Bronze con eventos válidos, duplicados e inválidos, corre el job y revisa
Silver. También borra el checkpoint para simular una caída: el MERGE no debe duplicar nada.
Lo lanza tests/run_spark_checks.sh.
"""
import csv
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from spark_session import get_spark

LANDING = os.environ["LANDING_URI"]
LAKE = os.environ["LAKE_URI"]
CHECKPOINTS = os.environ["CHECKPOINT_URI"]
SPARK_DIR = "/opt/fraudradar/spark"

failures = []


def check(cond, msg):
    print(("OK    " if cond else "FALLA ") + msg)
    if not cond:
        failures.append(msg)


def submit(script, *args):
    cmd = ["/opt/spark/bin/spark-submit", "--master", "local[2]", f"{SPARK_DIR}/{script}", *args]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        print(out.stdout[-3000:], out.stderr[-3000:])
        sys.exit(f"{script} falló")
    return out.stdout


def first_row(name):
    with open(f"{LANDING}/{name}.csv", newline="", encoding="utf-8") as f:
        return next(csv.DictReader(f))


def event(tx_id, tx_type="P2P", **extra):
    e = {
        "transaction_id": tx_id, "event_ts": "2026-11-07T22:40:02.123Z", "user_id": USER,
        "tx_type": tx_type, "channel": "app", "amount_cop": 50000, "merchant_id": None,
        "counterparty_user_id": "u-000002", "city_code": CITY, "lat": 6.25, "lon": -75.56,
        "is_fraud_injected": False, "attack_id": None, "attack_pattern": None,
    }
    e.update(extra)
    return json.dumps(e)


def append_bronze(spark, values, first_offset):
    now = datetime.now(timezone.utc)
    rows = [(USER, v, "transactions", 0, first_offset + i, now, "test", now) for i, v in enumerate(values)]
    df = spark.createDataFrame(
        rows, "key string, value string, topic string, partition int, offset long, kafka_ts timestamp, "
              "_session string, _ingested_at timestamp")
    df.withColumn("ingest_date", F.to_date("_ingested_at")).write.format("delta").mode("append") \
        .partitionBy("ingest_date").save(f"{LAKE}/bronze/transactions")


def table(spark, name):
    return spark.read.format("delta").load(f"{LAKE}/silver/{name}")


submit("batch/ingest_catalogs.py", "--run-id", "2026-11-07")
USER = first_row("usuarios")["user_id"]
CITY = first_row("ciudades")["city_code"]
MERCHANT = first_row("comercios")["merchant_id"]

spark = get_spark("check_bronze_to_silver")
spark.sparkContext.setLogLevel("ERROR")

# Lote 1: 3 válidas, 1 duplicada y 4 inválidas
append_bronze(spark, [
    event("tx-1"),
    event("tx-2", "PAGO", merchant_id=MERCHANT, counterparty_user_id=None),
    event("tx-3", is_fraud_injected=True, attack_id="atk-1", attack_pattern="rafaga"),
    event("tx-1"),  # reenvío
    "{esto no es json",
    event("tx-4", amount_cop=None),
    event("tx-5", tx_type="CHEQUE"),
    event("tx-6", "PAGO", merchant_id=None),
], first_offset=0)

submit("batch/bronze_to_silver.py", "--run-id", "run-1")
tx, rej = table(spark, "transactions"), table(spark, "transactions_rejected")
check(tx.count() == 3, f"3 transacciones válidas en Silver ({tx.count()})")
check(tx.select("transaction_id").distinct().count() == tx.count(), "sin transaction_id repetidos")
check(rej.count() == 4, f"4 rechazadas ({rej.count()})")
reasons = {r[0] for r in rej.select("reject_reason").collect()}
check(reasons == {"json_invalido", "falta_amount_cop", "tx_type_invalido", "pago_sin_comercio"},
      f"motivos de rechazo: {sorted(reasons)}")
check(rej.filter(F.col("value") == "{esto no es json").count() == 1, "el JSON roto queda guardado tal cual")

check(dict(tx.dtypes)["event_ts"] == "timestamp" and dict(tx.dtypes)["amount_cop"] == "bigint", "tipos del contrato")
pago = tx.filter("transaction_id = 'tx-2'").first()
check(pago["merchant_category"] is not None and pago["city"] is not None and pago["kyc_level"] is not None,
      "enriquecida con comercio, ciudad y usuario")
atk = tx.filter("transaction_id = 'tx-3'").first()
check(atk["is_fraud_injected"] and atk["attack_id"] == "atk-1" and atk["attack_pattern"] == "rafaga",
      "conserva las etiquetas del productor")
check(tx.filter("transaction_id = 'tx-1'").first()["kafka_offset"] == 0, "del duplicado se queda el primer offset")
check(tx.filter(F.col("_run_id") != "run-1").count() == 0, "todas las filas llevan el run_id")
check(table(spark, "users").count() > 0 and table(spark, "merchants").count() > 0, "catálogos tipados en Silver")

# Correr otra vez sin datos nuevos no cambia nada
submit("batch/bronze_to_silver.py", "--run-id", "run-2")
check(table(spark, "transactions").count() == 3, "sin datos nuevos, Silver no cambia")

# Lote 2: una nueva y un reenvío de tx-2 que llega más tarde
append_bronze(spark, [event("tx-7"), event("tx-2", "PAGO", merchant_id=MERCHANT, counterparty_user_id=None)],
              first_offset=8)
out = submit("batch/bronze_to_silver.py", "--run-id", "run-3")
check(table(spark, "transactions").count() == 4, "el reenvío en otro lote no se duplica (MERGE)")
check("2 válidas" in out, "solo procesó lo nuevo de Bronze")

# Caída: se pierde el checkpoint y el job relee todo Bronze
shutil.rmtree(f"{CHECKPOINTS}/bronze_to_silver")
submit("batch/bronze_to_silver.py", "--run-id", "run-4")
tx, rej = table(spark, "transactions"), table(spark, "transactions_rejected")
check(tx.count() == 4 and rej.count() == 4, f"releer todo Bronze no duplica: {tx.count()} y {rej.count()}")

spark.stop()
print("\nTodo bien." if not failures else f"\n{len(failures)} verificaciones fallidas.")
sys.exit(1 if failures else 0)
