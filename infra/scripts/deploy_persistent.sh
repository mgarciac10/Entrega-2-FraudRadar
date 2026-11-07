#!/usr/bin/env bash
# Crea o actualiza el stack persistente de FraudRadar.
# Uso: bash infra/scripts/deploy_persistent.sh
# Lee BUDGET_EMAIL, ALERT_EMAIL, S3_BUCKET, AWS_PROFILE y AWS_REGION del .env de la raíz.
set -euo pipefail

# Carga el .env de la raíz del repositorio si existe (el .env no se sube a git).
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
STACK="fraudradar-persistent"
: "${BUDGET_EMAIL:?Define BUDGET_EMAIL en el .env (correo de los avisos de costo)}"
: "${ALERT_EMAIL:?Define ALERT_EMAIL en el .env (correo del analista para las alertas de fraude)}"
: "${S3_BUCKET:?Define S3_BUCKET en el .env (fraudradar-<id de cuenta>-<región>)}"

cd "$ROOT"

# Empaqueta la Lambda y la sube al bucket. La clave lleva el hash del código:
# si el código no cambia, la clave no cambia y CloudFormation no actualiza la función.
mkdir -p infra/.build
rm -f infra/.build/notify_alert.zip
(cd lambda/notify_alert && python -m zipfile -c ../../infra/.build/notify_alert.zip handler.py)
HASH="$(sha256sum lambda/notify_alert/handler.py | cut -c1-12)"
LAMBDA_KEY="artifacts/lambda/notify_alert-$HASH.zip"
aws s3 cp infra/.build/notify_alert.zip "s3://$S3_BUCKET/$LAMBDA_KEY" \
  --profile "$PROFILE" --region "$REGION"

cd infra

aws cloudformation deploy \
  --stack-name "$STACK" \
  --template-file cloudformation/persistent.yaml \
  --parameter-overrides "BudgetEmail=$BUDGET_EMAIL" "AlertEmail=$ALERT_EMAIL" "LambdaCodeKey=$LAMBDA_KEY" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset \
  --profile "$PROFILE" --region "$REGION"

aws cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].StackStatus" --output text \
  --profile "$PROFILE" --region "$REGION"
