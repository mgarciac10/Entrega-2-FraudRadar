#!/usr/bin/env bash
# Crea o actualiza el stack persistente de FraudRadar.
# Uso: infra/scripts/deploy_persistent.sh (lee ALERT_EMAIL, AWS_PROFILE y AWS_REGION del .env)
set -euo pipefail

# Carga el .env de la raíz del repositorio si existe (el .env no se sube a git).
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
STACK="fraudradar-persistent"
: "${ALERT_EMAIL:?Define ALERT_EMAIL en el .env (correo para las alertas de Budgets)}"

cd "$(dirname "$0")/.."

aws cloudformation deploy \
  --stack-name "$STACK" \
  --template-file cloudformation/persistent.yaml \
  --parameter-overrides "NotificationEmail=$ALERT_EMAIL" \
  --no-fail-on-empty-changeset \
  --profile "$PROFILE" --region "$REGION"

aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].StackStatus" --output text \
  --profile "$PROFILE" --region "$REGION"
