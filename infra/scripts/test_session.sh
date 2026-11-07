#!/usr/bin/env bash
# Prueba de humo del stack por sesión. Verifica, usando SSM Run Command (no hace
# falta SSH ni el plugin de Session Manager):
#  1. MSK está ACTIVE y las dos EC2 están en línea en SSM.
#  2. Docker y Docker Compose quedaron instalados en las dos EC2.
#  3. Las EC2 alcanzan los brokers de Kafka por TLS (puerto 9094).
#  4. El rol de las EC2 permite leer S3 y DynamoDB.
# Uso: bash infra/scripts/test_session.sh
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
STACK="fraudradar-session"
: "${S3_BUCKET:?Define S3_BUCKET en el .env}"
AWS=(aws --profile "$PROFILE" --region "$REGION")

cd "$ROOT"
mkdir -p infra/.build
FAIL=0
ok()  { echo "OK   $1"; }
bad() { echo "FALLA $1"; FAIL=1; }

out() { "${AWS[@]}" cloudformation describe-stacks --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue | [0]" --output text; }

CLUSTER="$(out MskClusterArn)"; SPARK="$(out SparkInstanceId)"; APP="$(out AppInstanceId)"
if [ -z "$CLUSTER" ] || [ "$CLUSTER" = "None" ]; then echo "No existe el stack '$STACK'. Ejecuta session_up.sh."; exit 1; fi

# Ejecuta un comando en una EC2 por SSM. Imprime la salida; devuelve 0 si tuvo éxito.
run_on() {
  python -c 'import json,sys;print(json.dumps({"commands":[sys.argv[1]]}))' "$2" > infra/.build/cmd.json
  local cid st=Pending
  cid="$("${AWS[@]}" ssm send-command --instance-ids "$1" --document-name AWS-RunShellScript \
    --parameters file://infra/.build/cmd.json --query Command.CommandId --output text)" || return 1
  for _ in $(seq 1 40); do
    st="$("${AWS[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$1" \
      --query Status --output text 2>/dev/null || echo Pending)"
    case "$st" in Success|Failed|Cancelled|TimedOut) break ;; esac
    sleep 3
  done
  "${AWS[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$1" \
    --query StandardOutputContent --output text
  [ "$st" = "Success" ]
}

# 1. MSK y SSM
STATE="$("${AWS[@]}" kafka describe-cluster-v2 --cluster-arn "$CLUSTER" --query "ClusterInfo.State" --output text)"
[ "$STATE" = "ACTIVE" ] && ok "MSK está ACTIVE" || bad "MSK está en estado '$STATE'"
for ID in "$SPARK" "$APP"; do
  PING="$("${AWS[@]}" ssm describe-instance-information --filters "Key=InstanceIds,Values=$ID" \
    --query "InstanceInformationList[0].PingStatus" --output text)"
  [ "$PING" = "Online" ] && ok "$ID está en línea en SSM" || bad "$ID no está en línea en SSM ('$PING'); espera un minuto y repite"
done

# 2. Docker y Compose (el arranque puede tardar un par de minutos)
for ID in "$SPARK" "$APP"; do
  R=""
  for _ in $(seq 1 12); do
    R="$(run_on "$ID" 'grep -q "bootstrap terminado" /var/log/fraudradar-bootstrap.log && docker --version && docker compose version' 2>/dev/null)" && break
    sleep 10
  done
  if echo "$R" | grep -q "Docker Compose"; then ok "$ID tiene Docker y Compose"; else bad "$ID: Docker/Compose no están listos (revisa /var/log/fraudradar-bootstrap.log)"; fi
done

# 3. Brokers por TLS desde la EC2 de la aplicación
BROKERS="$("${AWS[@]}" kafka get-bootstrap-brokers --cluster-arn "$CLUSTER" --query BootstrapBrokerStringTls --output text)"
CMD='for b in BROKERLIST; do h=${b%:*}; if timeout 5 bash -c "echo > /dev/tcp/${h}/9094"; then echo "${h} OK"; else echo "${h} FALLA"; fi; done'
CMD="${CMD/BROKERLIST/${BROKERS//,/ }}"
R="$(run_on "$APP" "$CMD")"
N_OK="$(echo "$R" | grep -c ' OK')"
if [ "$N_OK" = "2" ]; then ok "la EC2 alcanza los 2 brokers de Kafka por TLS (9094)"; else bad "solo $N_OK de 2 brokers alcanzables: $R"; fi

# 4. Permisos del rol de las EC2
R="$(run_on "$SPARK" "aws s3 ls s3://$S3_BUCKET/ --region $REGION > /dev/null && echo S3_OK; aws dynamodb describe-table --table-name fraud_alerts --region $REGION --query Table.TableStatus --output text")"
echo "$R" | grep -q S3_OK && ok "el rol de la EC2 lee el bucket de S3" || bad "la EC2 no puede leer S3: $R"
echo "$R" | grep -q ACTIVE && ok "el rol de la EC2 lee la tabla fraud_alerts" || bad "la EC2 no puede leer DynamoDB: $R"

echo
[ "$FAIL" = "0" ] && echo "Todo bien." || { echo "Hay pruebas fallidas."; exit 1; }
