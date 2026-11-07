#!/usr/bin/env bash
# Borra el stack persistente. Pide confirmación porque incluirá S3 y DynamoDB con datos.
set -euo pipefail

# Carga el .env de la raíz del repositorio si existe (el .env no se sube a git).
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
STACK="fraudradar-persistent"

read -r -p "Esto borra el stack '$STACK' y sus recursos. Escribe '$STACK' para confirmar: " answer
[ "$answer" = "$STACK" ] || { echo "Cancelado."; exit 1; }

aws cloudformation delete-stack --stack-name "$STACK" --profile "$PROFILE" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "$STACK" --profile "$PROFILE" --region "$REGION"
echo "Stack '$STACK' borrado."
