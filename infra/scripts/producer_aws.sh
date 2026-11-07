#!/usr/bin/env bash
# Opera el productor en la EC2 de la aplicación, contra MSK.
# Uso: bash infra/scripts/producer_aws.sh <acción> [tasa]
#   push         envía el código actual a la EC2
#   topic        crea el topic transactions en MSK (6 particiones, RF 2)
#   start [tasa] genera los catálogos y arranca el productor (20 ev/s por defecto)
#   check        verifica que los mensajes están llegando a MSK
#   stop         detiene el productor
set -euo pipefail
. "$(dirname "$0")/lib.sh"

ACTION="${1:-}"
[ "$ACTION" = "push" ] && exec bash "$ROOT/infra/scripts/push_code.sh"

CLUSTER="$(stack_output MskClusterArn)"
APP="$(stack_output AppInstanceId)"
[ -n "$APP" ] && [ "$APP" != "None" ] || { echo "No hay stack por sesión. Ejecuta session_up.sh."; exit 1; }
BROKERS="$("${AWS[@]}" kafka get-bootstrap-brokers --cluster-arn "$CLUSTER" --query BootstrapBrokerStringTls --output text)"

case "$ACTION" in
  topic) run_remote "$APP" "bash /opt/fraudradar/infra/remote/create_topic.sh '$BROKERS'" ;;
  start) run_remote "$APP" "bash /opt/fraudradar/infra/remote/start_producer.sh '$BROKERS' ${2:-20}" ;;
  check) run_remote "$APP" "bash /opt/fraudradar/infra/remote/check_producer.sh '$BROKERS'" ;;
  stop)  run_remote "$APP" "bash /opt/fraudradar/infra/remote/stop_producer.sh" ;;
  *) echo "Uso: producer_aws.sh push|topic|start [tasa]|check|stop"; exit 1 ;;
esac
