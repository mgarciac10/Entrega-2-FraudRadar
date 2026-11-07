#!/usr/bin/env bash
# Crea el stack por sesión: Amazon MSK (2 brokers) + 2 EC2. COBRA POR HORA.
# Uso: bash infra/scripts/session_up.sh [--yes]
# Al terminar de trabajar: bash infra/scripts/session_down.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
STACK="fraudradar-session"
: "${S3_BUCKET:?Define S3_BUCKET en el .env (fraudradar-<id de cuenta>-<región>)}"
AWS=(aws --profile "$PROFILE" --region "$REGION")

cat <<'EOF'
Se va a crear el stack por sesión de FraudRadar:
  - Amazon MSK: 2 brokers kafka.t3.small   ~USD 0,09/h
  - EC2 Spark   t3.xlarge                  ~USD 0,17/h
  - EC2 Airflow t3.medium                  ~USD 0,04/h
  - IPv4 públicas y discos                 ~USD 0,02/h
  TOTAL aproximado: USD 0,32 por hora (precios on-demand de referencia; el crédito
  de USD 100 alcanza para unas 300 horas). MSK tarda 20-30 minutos en crearse y
  se cobra desde que empieza. MSK NO se puede pausar: hay que borrarlo.
EOF
if [ "${1:-}" != "--yes" ]; then
  read -r -p "Escribe 'si' para crear el stack: " answer
  [ "$answer" = "si" ] || { echo "Cancelado."; exit 1; }
fi

VPC="$("${AWS[@]}" ec2 describe-vpcs --filters Name=isDefault,Values=true --query "Vpcs[0].VpcId" --output text)"
[ "$VPC" != "None" ] || { echo "No hay VPC por defecto en $REGION."; exit 1; }
# Una subred por zona de disponibilidad (MSK usa las dos primeras).
SUBNETS="$(echo $("${AWS[@]}" ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC" Name=default-for-az,Values=true \
  --query "Subnets[].SubnetId" --output text) | tr ' ' ',')"
echo "VPC: $VPC   Subredes: $SUBNETS"

mkdir -p "$ROOT/infra/.build"
date +%s > "$ROOT/infra/.build/session_started"

cd "$ROOT/infra"
"${AWS[@]}" cloudformation deploy \
  --stack-name "$STACK" \
  --template-file cloudformation/session.yaml \
  --parameter-overrides "VpcId=$VPC" "SubnetIds=$SUBNETS" "S3Bucket=$S3_BUCKET" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset

"${AWS[@]}" cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output text
echo
echo "Listo. Verifica con: bash infra/scripts/test_session.sh"
echo "RECUERDA borrarlo al terminar: bash infra/scripts/session_down.sh"
