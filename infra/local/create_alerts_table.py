"""Crea la tabla fraud_alerts en DynamoDB Local, con la misma forma que en AWS.

Es el equivalente local de FraudAlertsTable en infra/cloudformation/persistent.yaml: llave alert_id
y el índice user_id-status-index que usa el analista. Si cambia la plantilla, cambiar aquí también.
Sin Streams ni Lambda: en local no hay correo; las alertas se ven con analyst/alertas.py.

Lo corre el servicio dynamodb-init (perfil local) del docker-compose. Es idempotente.
"""
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

ENDPOINT = os.environ.get("DYNAMODB_ENDPOINT_URL", "http://dynamodb-local:8000")
TABLE = os.environ.get("ALERTS_TABLE", "fraud_alerts")

# DynamoDB Local no valida credenciales, pero boto3 exige que existan
client = boto3.client("dynamodb", endpoint_url=ENDPOINT, region_name=os.environ.get("AWS_REGION", "us-east-2"),
                      aws_access_key_id="local", aws_secret_access_key="local")

for attempt in range(30):
    try:
        client.list_tables()
        break
    except EndpointConnectionError:
        time.sleep(1)
else:
    sys.exit(f"DynamoDB Local no responde en {ENDPOINT}")

try:
    client.create_table(
        TableName=TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "alert_id", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[{
            "IndexName": "user_id-status-index",
            "KeySchema": [{"AttributeName": "user_id", "KeyType": "HASH"},
                          {"AttributeName": "status", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
        }],
    )
    print(f"tabla {TABLE} creada en {ENDPOINT}")
except ClientError as exc:
    if exc.response["Error"]["Code"] != "ResourceInUseException":
        raise
    print(f"tabla {TABLE} ya existía en {ENDPOINT}")
