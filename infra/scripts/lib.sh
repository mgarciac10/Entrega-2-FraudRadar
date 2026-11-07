#!/usr/bin/env bash
# Funciones comunes de los scripts que operan sobre las EC2 por SSM.
# Se carga con:  . "$(dirname "$0")/lib.sh"
export MSYS_NO_PATHCONV=1
# La salida remota (p. ej. la barra de progreso de docker build) trae Unicode: sin esto la CLI
# de AWS en Windows falla con "charmap codec can't encode".
export PYTHONIOENCODING=utf-8 PYTHONUTF8=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "$ROOT/.env" ]; then set -a; . <(sed 's/\r$//' "$ROOT/.env"); set +a; fi

PROFILE="${AWS_PROFILE:-fraudradar}"
REGION="${AWS_REGION:-us-east-2}"
AWS=(aws --profile "$PROFILE" --region "$REGION")
cd "$ROOT"
mkdir -p infra/.build

# Salida de un stack: stack_output <Clave>
stack_output() {
  "${AWS[@]}" cloudformation describe-stacks --stack-name fraudradar-session \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue | [0]" --output text
}

# Ejecuta un comando en una EC2 por SSM y muestra su salida. Devuelve 0 si tuvo éxito.
#   run_remote <instance-id> "<comando>"
run_remote() {
  python -c 'import json,sys;print(json.dumps({"commands":[sys.argv[1]],"executionTimeout":["1800"]}))' "$2" > infra/.build/cmd.json
  local cid st=Pending
  cid="$("${AWS[@]}" ssm send-command --instance-ids "$1" --document-name AWS-RunShellScript \
    --parameters file://infra/.build/cmd.json --query Command.CommandId --output text)" || return 1
  for _ in $(seq 1 600); do
    st="$("${AWS[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$1" \
      --query Status --output text 2>/dev/null || echo Pending)"
    case "$st" in Success|Failed|Cancelled|TimedOut) break ;; esac
    sleep 3
  done
  "${AWS[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$1" --query StandardOutputContent --output text
  local err
  err="$("${AWS[@]}" ssm get-command-invocation --command-id "$cid" --instance-id "$1" --query StandardErrorContent --output text)"
  [ -n "$err" ] && [ "$err" != "None" ] && { echo "--- stderr:"; echo "$err"; }
  [ "$st" = "Success" ]
}
