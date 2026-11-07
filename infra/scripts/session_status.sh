#!/usr/bin/env bash
# Muestra si hay algo encendido que cobra por hora: stack por sesión, clústeres
# de MSK e instancias EC2. Úsalo antes de cerrar el portátil.
# Uso: bash infra/scripts/session_status.sh
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
AWS=(aws --profile "$PROFILE" --region "$REGION")
ON=0

STACK_STATUS="$("${AWS[@]}" cloudformation describe-stacks --stack-name fraudradar-session \
  --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "no existe")"
echo "Stack por sesión : $STACK_STATUS"
[ "$STACK_STATUS" != "no existe" ] && ON=1

MSK="$("${AWS[@]}" kafka list-clusters-v2 --query "ClusterInfoList[].[ClusterName,State]" --output text)"
echo "Clústeres MSK    : ${MSK:-ninguno}"
[ -n "$MSK" ] && ON=1

EC2="$("${AWS[@]}" ec2 describe-instances \
  --filters "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].[InstanceId,InstanceType,State.Name]" --output text)"
echo "Instancias EC2   : ${EC2:-ninguna}"
[ -n "$EC2" ] && ON=1

START_FILE="$ROOT/infra/.build/session_started"
if [ -f "$START_FILE" ]; then
  ELAPSED=$(( $(date +%s) - $(cat "$START_FILE") ))
  H=$(( ELAPSED / 3600 )); M=$(( (ELAPSED % 3600) / 60 ))
  COST="$(python -c "print(round($ELAPSED/3600*0.32, 2))")"
  echo "Sesión abierta hace ${H} h ${M} min  ->  costo aproximado hasta ahora: USD $COST"
fi

echo
if [ "$ON" = "1" ]; then
  echo "HAY RECURSOS ENCENDIDOS que cobran por hora. Si ya terminaste: bash infra/scripts/session_down.sh"
else
  echo "Todo apagado: no hay nada cobrando por hora."
fi
