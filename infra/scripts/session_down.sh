#!/usr/bin/env bash
# Borra el stack por sesión (MSK y las 2 EC2) para dejar de pagar por hora.
# Los datos NO se pierden: viven en el stack persistente (S3 y DynamoDB).
# Uso: bash infra/scripts/session_down.sh [--yes]
set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
STACK="fraudradar-session"
AWS=(aws --profile "$PROFILE" --region "$REGION")

echo "Se borrará el stack '$STACK' (MSK y las 2 EC2). El contenido de las EC2 y los topics de Kafka se pierden."
echo "Lo que está en S3 y DynamoDB se conserva."
if [ "${1:-}" != "--yes" ]; then
  read -r -p "Escribe 'si' para borrar: " answer
  [ "$answer" = "si" ] || { echo "Cancelado."; exit 1; }
fi

"${AWS[@]}" cloudformation delete-stack --stack-name "$STACK"
echo "Borrando (MSK tarda varios minutos)..."
"${AWS[@]}" cloudformation wait stack-delete-complete --stack-name "$STACK"
echo "Stack '$STACK' borrado."
rm -f "$ROOT/infra/.build/session_started"

echo
bash "$ROOT/infra/scripts/session_status.sh"
