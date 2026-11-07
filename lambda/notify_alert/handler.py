"""notify_alert: avisa al analista de cada alerta nueva de fraud_alerts.

Disparada por DynamoDB Streams (solo eventos INSERT; el filtro está en el
event source mapping). Publica un mensaje en el topic SNS fraud-alerts.

Garantía: Lambda procesa los registros de DynamoDB Streams al menos una vez,
así que en un caso raro el analista puede recibir un correo duplicado. Se
acepta: un aviso duplicado es inofensivo y uno perdido no (ADR-04). Se usa
respuesta de lote parcial para reintentar solo los registros que fallaron.
"""
import json
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer

TOPIC_ARN = os.environ["TOPIC_ARN"]

sns = boto3.client("sns")
_deserializer = TypeDeserializer()


def _plain(image):
    """Convierte un ítem en formato DynamoDB JSON a un dict de Python."""
    return {k: _deserializer.deserialize(v) for k, v in image.items()}


def _ascii(text, limit=100):
    # El asunto de SNS debe ser ASCII imprimible y de máximo 100 caracteres.
    return text.encode("ascii", "replace").decode("ascii")[:limit]


def build_message(alert):
    """Devuelve (asunto, cuerpo) para una alerta."""
    rule = alert.get("rule", "desconocida")
    user = alert.get("user_id", "desconocido")
    subject = _ascii(f"FraudRadar: alerta {rule} - usuario {user}")
    body = "\n".join(
        [
            f"Regla: {rule}",
            f"Usuario: {user}",
            f"Monto en riesgo (COP): {alert.get('amount_at_risk_cop', 'n/d')}",
            f"Transacciones: {', '.join(alert.get('transaction_ids', [])) or 'n/d'}",
            f"Inicio de la ventana: {alert.get('window_start', 'n/d')}",
            f"Detectada en el evento de: {alert.get('trigger_event_ts', 'n/d')}",
            f"Evidencia: {json.dumps(alert.get('evidence', {}), default=str, ensure_ascii=False)}",
            f"Estado: {alert.get('status', 'n/d')}",
            f"ID de alerta: {alert.get('alert_id', 'n/d')}",
        ]
    )
    return subject, body


def lambda_handler(event, context):
    failures = []
    for record in event.get("Records", []):
        sequence = record["dynamodb"]["SequenceNumber"]
        try:
            # El event source mapping ya filtra INSERT; esto es una defensa extra.
            if record.get("eventName") != "INSERT":
                continue
            alert = _plain(record["dynamodb"]["NewImage"])
            subject, body = build_message(alert)
            sns.publish(TopicArn=TOPIC_ARN, Subject=subject, Message=body)
            print(json.dumps({"evento": "alerta_enviada", "alert_id": alert.get("alert_id")}))
        except Exception as exc:  # noqa: BLE001 - se reintenta solo este registro
            print(json.dumps({"evento": "error", "sequence": sequence, "error": str(exc)}))
            failures.append({"itemIdentifier": sequence})
    return {"batchItemFailures": failures}
