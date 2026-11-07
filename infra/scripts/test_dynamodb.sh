#!/usr/bin/env bash
# Prueba de humo de la tabla fraud_alerts: escritura condicional (idempotencia),
# consulta por el índice user_id + status y Streams activado.
# Uso: bash infra/scripts/test_dynamodb.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
TABLE="fraud_alerts"
AWS=(aws --profile "$PROFILE" --region "$REGION")

cd "$ROOT/infra/scripts"
FAIL=0
ok()  { echo "OK   $1"; }
bad() { echo "FALLA $1"; FAIL=1; }

cleanup() { "${AWS[@]}" dynamodb delete-item --table-name "$TABLE" --key file://fixtures/alert_key.json >/dev/null 2>&1; }
trap cleanup EXIT
cleanup

# 1. La primera escritura crea la alerta.
if "${AWS[@]}" dynamodb put-item --table-name "$TABLE" --item file://fixtures/alert_sample.json \
     --condition-expression "attribute_not_exists(alert_id)" >/dev/null 2>&1; then
  ok "primera escritura condicional crea la alerta"
else
  bad "la primera escritura condicional debía funcionar"
fi

# 2. Reescribir la misma alerta debe ser rechazado (así se evitan duplicados).
OUT="$("${AWS[@]}" dynamodb put-item --table-name "$TABLE" --item file://fixtures/alert_sample.json \
     --condition-expression "attribute_not_exists(alert_id)" 2>&1)"
if echo "$OUT" | grep -q "ConditionalCheckFailedException"; then
  ok "la reescritura de la misma alerta es rechazada (idempotencia)"
else
  bad "la reescritura debía fallar con ConditionalCheckFailedException"
fi

# 3. Consulta por el índice global user_id + status.
N="$("${AWS[@]}" dynamodb query --table-name "$TABLE" --index-name user_id-status-index \
     --key-condition-expression "user_id = :u AND #s = :s" \
     --expression-attribute-names '{"#s":"status"}' \
     --expression-attribute-values file://fixtures/alert_query_values.json \
     --query Count --output text 2>&1)"
if [ "$N" = "1" ]; then ok "el índice user_id + status devuelve la alerta"; else bad "el índice devolvió '$N' en lugar de 1 (los índices son eventualmente consistentes: reintenta)"; fi

# 4. Streams activado.
VIEW="$("${AWS[@]}" dynamodb describe-table --table-name "$TABLE" \
     --query "Table.StreamSpecification.StreamViewType" --output text 2>&1)"
if [ "$VIEW" = "NEW_IMAGE" ]; then ok "DynamoDB Streams activado (NEW_IMAGE)"; else bad "Streams no está en NEW_IMAGE: '$VIEW'"; fi

[ "$FAIL" = "0" ] && echo "Todo bien." || { echo "Hay pruebas fallidas."; exit 1; }
