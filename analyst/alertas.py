"""alertas: herramienta del analista de riesgo para trabajar las alertas de DynamoDB (RF-08).

Cubre lo que el analista necesita después de recibir el correo de una alerta:
  listar    alertas de un usuario (opcionalmente solo las abiertas)
  abiertas  alertas abiertas de todos los usuarios, las más recientes primero
  marcar    cambiar el estado de una alerta a confirmada o descartada
  resumen   cuántas alertas hay por estado y por regla

Estados: abierta (la deja detect_fraud) -> confirmada (era fraude) | descartada (falso positivo).
Esa decisión es la que `silver_to_gold` lee cada 15 min para fact_alert y la tasa de falsos positivos (Q7).
Marcar una alerta es un cambio (MODIFY) y no un INSERT, así que no dispara otro correo (ADR-04).

Uso (con las credenciales del proyecto: perfil `fraudradar`, región us-east-2):
  export AWS_PROFILE=fraudradar AWS_REGION=us-east-2
  python analyst/alertas.py listar --user u-000123 [--estado abierta]
  python analyst/alertas.py abiertas [--limite 20]
  python analyst/alertas.py marcar <alert_id> confirmada --analista juanjo
  python analyst/alertas.py resumen

Lo mismo sin el script, desde la consola de DynamoDB o con PartiQL:
  SELECT * FROM "fraud_alerts"."user_id-status-index" WHERE user_id = 'u-000123' AND status = 'abierta'
  UPDATE "fraud_alerts" SET status = 'confirmada' WHERE alert_id = '<alert_id>'
"""
import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

TABLE = os.environ.get("ALERTS_TABLE", "fraud_alerts")
INDEX = "user_id-status-index"
ESTADOS = ("abierta", "confirmada", "descartada")


def get_table():
    endpoint = os.environ.get("DYNAMODB_ENDPOINT_URL") or None  # solo para DynamoDB local
    # Perfil del proyecto por defecto (CLAUDE.md): nunca el `default`, que puede ser de otro proyecto
    profile = None if endpoint else os.environ.get("AWS_PROFILE", "fraudradar")
    session = boto3.Session(profile_name=profile, region_name=os.environ.get("AWS_REGION", "us-east-2"))
    return session.resource("dynamodb", endpoint_url=endpoint).Table(TABLE)


def _by_recent(items):
    return sorted(items, key=lambda a: a.get("created_at", ""), reverse=True)


def list_alerts(table, user_id, estado=None):
    """Alertas de un usuario por el índice user_id + status (la consulta principal del analista)."""
    cond = Key("user_id").eq(user_id)
    if estado:
        cond = cond & Key("status").eq(estado)
    items, kwargs = [], {"IndexName": INDEX, "KeyConditionExpression": cond}
    while True:
        page = table.query(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return _by_recent(items)
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def scan_alerts(table, **kwargs):
    items = []
    while True:
        page = table.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def open_alerts(table, limit=20):
    """Alertas abiertas de todos los usuarios. Es un scan: está bien para el volumen del proyecto (~2 % de los eventos)."""
    return _by_recent(scan_alerts(table, FilterExpression=Attr("status").eq("abierta")))[:limit]


def mark(table, alert_id, estado, analista=None):
    """Cambia el estado de UNA alerta que ya existe. Devuelve el estado anterior.

    update_item crea el ítem si no existe; por eso la condición attribute_exists: un alert_id mal
    escrito falla en vez de dejar una alerta fantasma en la tabla.
    """
    if estado not in ESTADOS:
        raise ValueError(f"estado inválido: {estado} (usa {', '.join(ESTADOS)})")
    now = datetime.now(timezone.utc)
    try:
        resp = table.update_item(
            Key={"alert_id": alert_id},
            UpdateExpression="SET #s = :s, reviewed_at = :t, reviewed_by = :a",
            ExpressionAttributeNames={"#s": "status"},  # status es palabra reservada de DynamoDB
            ExpressionAttributeValues={
                ":s": estado,
                ":t": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
                ":a": analista or "n/d",
            },
            ConditionExpression="attribute_exists(alert_id)",
            ReturnValues="UPDATED_OLD",
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise LookupError(f"no existe la alerta {alert_id}") from exc
        raise
    return resp["Attributes"].get("status")


def summary(table):
    items = scan_alerts(table)
    return {
        "total": len(items),
        "por_estado": dict(Counter(a.get("status", "?") for a in items)),
        "por_regla": dict(Counter(a.get("rule", "?") for a in items)),
        "monto_en_riesgo_abiertas_cop": int(sum(a.get("amount_at_risk_cop", 0) for a in items if a.get("status") == "abierta")),
    }


def fmt(a):
    ev = json.dumps(a.get("evidence", {}), default=str, ensure_ascii=False)
    return (f"{a['alert_id']}  {a.get('status', '?'):<10}  {a.get('rule', '?'):<16}  {a.get('user_id', '?'):<10}  "
            f"${int(a.get('amount_at_risk_cop', 0)):,}  {a.get('trigger_event_ts', '')}\n    tx: "
            f"{', '.join(a.get('transaction_ids', []))}\n    evidencia: {ev}")


def main(argv=None):
    p = argparse.ArgumentParser(description="Herramienta del analista para las alertas de FraudRadar")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("listar", help="alertas de un usuario")
    s.add_argument("--user", required=True)
    s.add_argument("--estado", choices=ESTADOS)
    s = sub.add_parser("abiertas", help="alertas abiertas de todos los usuarios")
    s.add_argument("--limite", type=int, default=20)
    s = sub.add_parser("marcar", help="confirmar o descartar una alerta")
    s.add_argument("alert_id")
    s.add_argument("estado", choices=("confirmada", "descartada"))
    s.add_argument("--analista", help="quién decide (queda en reviewed_by)")
    sub.add_parser("resumen", help="conteo por estado y por regla")
    args = p.parse_args(argv)

    table = get_table()
    try:
        if args.cmd == "listar":
            items = list_alerts(table, args.user, args.estado)
            print("\n".join(fmt(a) for a in items) or "sin alertas")
        elif args.cmd == "abiertas":
            items = open_alerts(table, args.limite)
            print("\n".join(fmt(a) for a in items) or "sin alertas abiertas")
        elif args.cmd == "marcar":
            before = mark(table, args.alert_id, args.estado, args.analista)
            print(f"{args.alert_id}: {before} -> {args.estado}")
        else:
            print(json.dumps(summary(table), indent=2, ensure_ascii=False))
    except (LookupError, ValueError) as exc:
        sys.exit(str(exc))


if __name__ == "__main__":
    main()
