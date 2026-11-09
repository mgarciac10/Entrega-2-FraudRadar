"""Copia las alertas de DynamoDB a Silver para que Gold tenga un estado consultable.

Uso:
  spark-submit spark/batch/extract_alert_status.py
  spark-submit spark/batch/extract_alert_status.py --source local-json --input-json /data/alerts.json
"""
import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

from schemas import ATTACK_PATTERNS
from spark_session import get_spark, lake_path

ALERT_SCHEMA = T.StructType([
    T.StructField("alert_id", T.StringType(), False),
    T.StructField("user_id", T.StringType(), False),
    T.StructField("rule", T.StringType(), False),
    T.StructField("window_start", T.TimestampType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("amount_at_risk_cop", T.LongType(), False),
    T.StructField("transaction_ids", T.ArrayType(T.StringType(), False), False),
    T.StructField("trigger_event_ts", T.TimestampType(), True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("evidence", T.StringType(), True),
])
VALID_STATUSES = {"abierta", "confirmada", "descartada"}


def _timestamp(value, field, alert_id):
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} inválido en alerta {alert_id}: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def normalize_alert(item):
    alert_id = str(item.get("alert_id", ""))
    required = (
        "alert_id", "user_id", "rule", "window_start", "status",
        "amount_at_risk_cop", "trigger_event_ts", "created_at",
    )
    missing = [field for field in required if item.get(field) is None]
    if missing:
        raise ValueError(f"Alerta sin campos requeridos {missing}: {alert_id or '<sin alert_id>'}")
    if not alert_id:
        raise ValueError("alert_id no puede estar vacío")
    if item["rule"] not in ATTACK_PATTERNS:
        raise ValueError(f"Regla inválida en alerta {alert_id}: {item['rule']}")
    if item["status"] not in VALID_STATUSES:
        raise ValueError(f"Estado inválido en alerta {alert_id}: {item['status']}")
    amount_at_risk = int(item["amount_at_risk_cop"])
    if amount_at_risk != item["amount_at_risk_cop"] or amount_at_risk <= 0:
        raise ValueError(f"amount_at_risk_cop debe ser positivo en alerta {alert_id}")
    if "transaction_ids" not in item:
        raise ValueError(f"Falta transaction_ids en alerta {alert_id}")
    transaction_ids = item["transaction_ids"] or []
    if not isinstance(transaction_ids, (list, tuple)) or any(not isinstance(value, str) for value in transaction_ids):
        raise ValueError(f"transaction_ids inválido en alerta {alert_id}")
    if len(transaction_ids) != len(set(transaction_ids)):
        raise ValueError(f"transaction_ids repetidos en alerta {alert_id}")
    evidence = item.get("evidence")
    return (
        alert_id,
        str(item["user_id"]),
        str(item["rule"]),
        _timestamp(item.get("window_start"), "window_start", alert_id),
        str(item["status"]),
        amount_at_risk,
        list(transaction_ids),
        _timestamp(item.get("trigger_event_ts"), "trigger_event_ts", alert_id),
        _timestamp(item.get("created_at"), "created_at", alert_id),
        json.dumps(evidence, ensure_ascii=False, default=str) if evidence is not None else None,
    )


def read_json_alerts(path):
    with open(path, encoding="utf-8") as source:
        text = source.read()
    if not text.strip():
        raise ValueError(f"El archivo local de alertas está vacío: {path}")
    try:
        decoded = json.loads(text)
        items = decoded if isinstance(decoded, list) else [decoded]
    except json.JSONDecodeError:
        items = [json.loads(line) for line in text.splitlines() if line.strip()]
    if any(not isinstance(item, dict) for item in items):
        raise ValueError(f"El JSON de alertas debe contener objetos: {path}")
    return items


def scan_alerts(table_name, region, endpoint_url):
    import boto3

    # DynamoDB Local no valida credenciales, pero boto3 exige que existan. En AWS: rol de la EC2.
    local_creds = {"aws_access_key_id": "local", "aws_secret_access_key": "local"} if endpoint_url else {}
    table = boto3.resource(
        "dynamodb", region_name=region, endpoint_url=endpoint_url or None, **local_creds
    ).Table(table_name)
    items = []
    request = {}
    while True:
        response = table.scan(**request)
        items.extend(response["Items"])
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        request = {"ExclusiveStartKey": last_key}
    return items


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--source",
        choices=("local-json", "dynamodb"),
        default=os.environ.get("ALERTS_SOURCE", "dynamodb"),
    )
    parser.add_argument("--input-json", default=os.environ.get("ALERTS_JSON"))
    parser.add_argument("--table", default=os.environ.get("ALERTS_TABLE", "fraud_alerts"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-2"))
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H:%M:%S"))
    args = parser.parse_args()

    if args.source == "local-json":
        if not args.input_json:
            parser.error("ALERTS_SOURCE=local-json requiere ALERTS_JSON o --input-json")
        items = read_json_alerts(args.input_json)
    else:
        items = scan_alerts(args.table, args.region, os.environ.get("DYNAMODB_ENDPOINT_URL"))
    rows = [normalize_alert(item) for item in items]
    ids = [row[0] for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("La fuente contiene alert_id repetidos")

    spark = get_spark("extract_alert_status")
    spark.sparkContext.setLogLevel("WARN")
    alerts = spark.createDataFrame(rows, ALERT_SCHEMA)
    (
        alerts.withColumn("_run_id", F.lit(args.run_id))
        .withColumn("_extracted_at", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .save(lake_path("silver", "alert_status"))
    )
    print(f"silver/alert_status: {len(rows)} alertas (run_id={args.run_id})")
    spark.stop()


if __name__ == "__main__":
    main()
