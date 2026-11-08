"""detect_fraud: topic transactions (Kafka/MSK) -> alertas en DynamoDB (fraud_alerts).

Job de streaming continuo (camino caliente). No lee Bronze: lee directo de MSK para no sumar la
latencia de Bronze a la de la alerta (ADR-02).

Flujo:
  1. Parsea el JSON con el contrato del evento y descarta los inválidos (reject_reason, el mismo
     criterio de bronze_to_silver; los inválidos igual quedan en Bronze y de ahí a transactions_rejected).
  2. Deduplica por transaction_id con watermark de 10 min.
  3. Join stream-estático con silver.user_profile: agrega el promedio de 30 días a cada transacción.
  4. Una "libreta" por usuario (applyInPandasWithState, lógica en fraud_rules.py) evalúa las 4 reglas.
  5. foreachBatch: escribe cada alerta en DynamoDB con escritura condicional por alert_id.

Garantía: alert_id = hash(user_id, regla, inicio de ventana) es determinístico y la escritura es
condicional (attribute_not_exists), así que una alerta repetida (por un reintento del micro-batch o un
reinicio) se ignora y no se duplica ni se vuelve a avisar por correo (Lambda solo reacciona a INSERT).
Si DynamoDB falla, el micro-batch falla, Spark vuelve al checkpoint y lo repite completo.

No lee las etiquetas del productor (is_fraud_injected, attack_id, attack_pattern).

Uso:
  spark-submit spark/streaming/detect_fraud.py [--session 2026-11-08] [--sink dynamodb|console]
Requiere haber corrido build_user_profile (silver/user_profile) al menos una vez.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

import fraud_rules
import reglas
from bronze_to_silver import reject_reason
from ingest_bronze import read_kafka
from schemas import transaction_struct
from spark_session import checkpoint_uri, get_spark, lake_path

WATERMARK = "10 minutes"

# Lo que entra a la libreta de cada usuario
INPUT_COLS = ["transaction_id", "user_id", "event_ts", "ts_ms", "amount_cop", "city_code", "avg_amount_30d"]

STATE_SCHEMA = "state_json string"
ALERT_SCHEMA = (
    "user_id string, rule string, window_start_ms long, trigger_ts_ms long, trigger_tx_id string, "
    "transaction_ids array<string>, amount_at_risk_cop long, evidence string"
)
ALERT_COLS = [f.split(" ")[0] for f in ALERT_SCHEMA.split(", ")]


def update_user(key, pdf_iter, state):
    """Se llama una vez por usuario y micro-batch: abre su libreta, evalúa lo nuevo y la guarda."""
    out = []
    if state.hasTimedOut:
        state.remove()  # el usuario lleva más de 30 min sin movimiento: ya no hace falta su libreta
    else:
        book = json.loads(state.get[0]) if state.exists else fraud_rules.new_state()
        events = []
        for pdf in pdf_iter:
            pdf = pdf.astype(object).where(pdf.notna(), None)  # NaN -> None (usuario sin perfil)
            events.extend(pdf.to_dict("records"))
        book, fires = fraud_rules.evaluate(book, events)
        for f in fires:
            if f["first_of_episode"]:  # el resto de disparos del episodio es la misma alerta
                out.append((key[0], f["rule"], f["window_start_ms"], f["trigger_ts_ms"], f["trigger_tx_id"],
                            f["transaction_ids"], f["amount_at_risk_cop"], json.dumps(f["evidence"])))
        state.update((json.dumps(book),))
        last_ts = max((r[fraud_rules.TS] for r in book["events"]), default=0)
        # El timeout debe quedar por delante del watermark actual
        state.setTimeoutTimestamp(max(last_ts + fraud_rules.RETENTION_MS, state.getCurrentWatermarkMs() + 1))
    yield pd.DataFrame(out, columns=ALERT_COLS)


def build_alerts(events, profile):
    """DataFrame (streaming) de eventos crudos ya parseados -> DataFrame (streaming) de alertas.

    events:  columnas del contrato del evento (transaction_id, user_id, event_ts, amount_cop, city_code, ...)
    profile: user_id, avg_amount_30d (estático)
    """
    ts_ms = (F.col("event_ts").cast("double") * 1000).cast("long")
    prepared = (
        events.withWatermark("event_ts", WATERMARK)
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .join(F.broadcast(profile.select("user_id", F.col("avg_amount_30d").cast("double"))), "user_id", "left")
        .select("transaction_id", "user_id", "event_ts", ts_ms.alias("ts_ms"), "amount_cop", "city_code", "avg_amount_30d")
    )
    alerts = prepared.groupBy("user_id").applyInPandasWithState(
        update_user, ALERT_SCHEMA, STATE_SCHEMA, "Update", GroupStateTimeout.EventTimeTimeout)
    return alerts.select(
        "*",
        (F.col("window_start_ms") / 1000).cast("timestamp").alias("window_start"),
        (F.col("trigger_ts_ms") / 1000).cast("timestamp").alias("trigger_event_ts"),
    )


def parse_events(raw):
    """Mensajes de Kafka (value como texto) -> eventos válidos con el tipo del contrato."""
    parsed = raw.select(F.col("value").cast("string").alias("value")).withColumn(
        "e", F.from_json("value", transaction_struct()))
    valid = parsed.filter(reject_reason(F.col("e"), F.col("value")).isNull())
    return valid.select("e.*")


# --- Escritura de alertas ---------------------------------------------------

def alert_item(row, now=None):
    """Fila de alerta -> ítem de DynamoDB (mismos atributos que infra/scripts/fixtures/alert_sample.json)."""
    now = now or datetime.now(timezone.utc)
    return {
        "alert_id": fraud_rules.alert_id(row["user_id"], row["rule"], row["window_start_ms"]),
        "user_id": row["user_id"],
        "rule": row["rule"],
        "window_start": fraud_rules.iso_utc(row["window_start_ms"]),
        "status": "abierta",
        "amount_at_risk_cop": int(row["amount_at_risk_cop"]),
        "transaction_ids": list(row["transaction_ids"]),
        "trigger_event_ts": fraud_rules.iso_utc(row["trigger_ts_ms"]),
        "created_at": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        # boto3 no acepta float: los decimales viajan como Decimal
        "evidence": json.loads(row["evidence"], parse_float=Decimal),
    }


def dynamodb_writer():
    import boto3
    from botocore.exceptions import ClientError

    table = boto3.resource(
        "dynamodb",
        region_name=os.environ.get("AWS_REGION", "us-east-2"),
        endpoint_url=os.environ.get("DYNAMODB_ENDPOINT_URL") or None,  # solo para DynamoDB local
    ).Table(os.environ.get("ALERTS_TABLE", "fraud_alerts"))

    def write(batch, batch_id):
        rows = [r.asDict() for r in batch.collect()]  # ~2 % de los eventos: caben en el driver
        created = duplicated = 0
        for row in rows:
            try:
                table.put_item(Item=alert_item(row), ConditionExpression="attribute_not_exists(alert_id)")
                created += 1
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise  # otro error: el micro-batch falla y Spark lo repite desde el checkpoint
                duplicated += 1  # la alerta ya existía: es una reemisión, se ignora
        print(f"lote {batch_id}: {created} alertas nuevas, {duplicated} ya existían")

    return write


def console_writer():
    def write(batch, batch_id):
        for r in batch.collect():
            print(f"lote {batch_id}: {json.dumps(alert_item(r.asDict()), default=str, ensure_ascii=False)}")

    return write


def main():
    p = argparse.ArgumentParser()
    # MSK se recrea en cada sesión: un checkpoint viejo apunta a offsets que ya no existen
    p.add_argument("--session", default=os.environ.get("STREAM_SESSION", "local"))
    p.add_argument("--trigger", default=os.environ.get("DETECT_TRIGGER", "15 seconds"))
    p.add_argument("--starting-offsets", default="earliest")
    p.add_argument("--sink", choices=["dynamodb", "console"], default=os.environ.get("ALERTS_SINK", "dynamodb"))
    args = p.parse_args()

    spark = get_spark("detect_fraud")
    spark.sparkContext.setLogLevel("WARN")

    profile_path = lake_path("silver", "user_profile")
    try:
        profile = spark.read.format("delta").load(profile_path)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"No existe {profile_path}: corre build_user_profile antes de detect_fraud") from exc

    checkpoint = f"{checkpoint_uri()}/detect_fraud/{args.session}"
    print(f"detect_fraud -> {args.sink} (checkpoint {checkpoint}, trigger {args.trigger}, "
          f"umbrales {json.dumps({k: v for k, v in vars(reglas).items() if k.isupper()})})")

    alerts = build_alerts(parse_events(read_kafka(spark, args.starting_offsets)), profile)
    writer = dynamodb_writer() if args.sink == "dynamodb" else console_writer()
    query = (
        alerts.writeStream.outputMode("update")
        .foreachBatch(writer)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
