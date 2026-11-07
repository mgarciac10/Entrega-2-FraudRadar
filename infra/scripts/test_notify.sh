#!/usr/bin/env bash
# Prueba de extremo a extremo del aviso: tabla -> Streams -> Lambda -> SNS.
#  1. Una alerta nueva (INSERT) dispara la Lambda, que publica en SNS.
#  2. Cambiar el estado de la alerta (MODIFY) NO dispara un segundo aviso.
# Debes confirmar la suscripción de correo de SNS antes de recibir el mensaje.
# Uso: bash infra/scripts/test_notify.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
TABLE="fraud_alerts"
LOG_GROUP="/aws/lambda/fraudradar-notify-alert"
AWS=(aws --profile "$PROFILE" --region "$REGION")

cd "$ROOT/infra/scripts"
FAIL=0
ok()  { echo "OK   $1"; }
bad() { echo "FALLA $1"; FAIL=1; }

cleanup() { "${AWS[@]}" dynamodb delete-item --table-name "$TABLE" --key file://fixtures/alert_key.json >/dev/null 2>&1; }
trap cleanup EXIT
cleanup

# Cuenta cuántos avisos registró la Lambda desde el instante $1 (milisegundos).
# - MSYS_NO_PATHCONV evita que Git Bash convierta "/aws/lambda/..." en una ruta de Windows.
# - Se cuentan ocurrencias en el texto y no con length(events): la CLI pagina y
#   calcularía un número por página. Los errores de la CLI no se ocultan.
sent_since() {
  MSYS_NO_PATHCONV=1 "${AWS[@]}" logs filter-log-events --log-group-name "$LOG_GROUP" --start-time "$1" \
    --filter-pattern alerta_enviada --query "events[].message" --output text | grep -o alerta_enviada | wc -l | tr -d ' '
}

# Espera hasta 45 s a que el conteo llegue a $2 desde $1.
wait_for() {
  local start="$1" want="$2" n=0
  for _ in $(seq 1 15); do
    n="$(sent_since "$start")"
    [ "$n" -ge "$want" ] 2>/dev/null && { echo "$n"; return; }
    sleep 3
  done
  echo "$n"
}

# Estado de la suscripción de correo.
SUBS="$("${AWS[@]}" sns list-subscriptions-by-topic \
  --topic-arn "$("${AWS[@]}" sns list-topics --query "Topics[?contains(TopicArn,'fraud-alerts')].TopicArn | [0]" --output text)" \
  --query "Subscriptions[0].SubscriptionArn" --output text 2>&1)"
if [ "$SUBS" = "PendingConfirmation" ]; then
  echo "AVISO la suscripción de correo sigue pendiente: abre el correo de AWS y confirma, o no llegará el aviso."
fi

START="$(date +%s%3N)"

# 1. INSERT: debe producir exactamente un aviso.
"${AWS[@]}" dynamodb put-item --table-name "$TABLE" --item file://fixtures/alert_sample.json \
  --condition-expression "attribute_not_exists(alert_id)" >/dev/null 2>&1 || bad "no se pudo insertar la alerta"
N="$(wait_for "$START" 1)"
if [ "$N" = "1" ]; then ok "la alerta nueva disparó la Lambda (1 aviso publicado)"; else bad "se esperaba 1 aviso y hay '$N'"; fi

# 2. MODIFY: no debe producir otro aviso.
"${AWS[@]}" dynamodb update-item --table-name "$TABLE" --key file://fixtures/alert_key.json \
  --update-expression "SET #s = :c" \
  --expression-attribute-names file://fixtures/alert_update_names.json \
  --expression-attribute-values file://fixtures/alert_update_values.json >/dev/null 2>&1 || bad "no se pudo actualizar el estado"
sleep 20
N2="$(sent_since "$START")"
if [ "$N2" = "1" ]; then ok "el cambio de estado no generó un segundo aviso (filtro INSERT)"; else bad "tras el cambio de estado hay '$N2' avisos (se esperaba 1)"; fi

echo
echo "Revisa tu correo: debe haber llegado un mensaje 'FraudRadar: alerta rafaga - usuario u-test'."
[ "$FAIL" = "0" ] && echo "Todo bien." || { echo "Hay pruebas fallidas."; exit 1; }
