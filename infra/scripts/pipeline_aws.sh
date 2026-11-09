#!/usr/bin/env bash
# Opera Spark (streaming y batch) y Airflow en la EC2 de Spark, contra MSK, S3 y DynamoDB.
# Antes: session_up.sh, push_code.sh y producer_aws.sh topic + start (sube los catálogos a S3).
# Uso: bash infra/scripts/pipeline_aws.sh <acción> [argumentos]
#   start               arranca Spark, ingest-bronze, detect-fraud y Airflow (STREAM_SESSION nueva)
#   status              estado de los contenedores y de las corridas de los DAG
#   dag <dag_id>        activa el DAG y lanza una corrida (daily_batch primero, luego lakehouse_15min)
#   logs <servicio>     últimas líneas de un servicio (ingest-bronze, detect-fraud, airflow-scheduler...)
#   register            registra las tablas de Gold en Glue (después de la primera corrida de Gold)
#   stop                detiene los contenedores (los datos quedan en S3 y DynamoDB)
# Las interfaces (Airflow 8081, Spark 8080) se ven con reenvío de puertos de Session Manager:
#   aws ssm start-session --profile fraudradar --region us-east-2 --target <SparkInstanceId> \
#     --document-name AWS-StartPortForwardingSession --parameters portNumber=8081,localPortNumber=8081
set -euo pipefail
. "$(dirname "$0")/lib.sh"
: "${S3_BUCKET:?Define S3_BUCKET en el .env}"

ACTION="${1:-}"
SPARK="$(stack_output SparkInstanceId)"
[ -n "$SPARK" ] && [ "$SPARK" != "None" ] || { echo "No hay stack por sesión. Ejecuta session_up.sh."; exit 1; }
COMPOSE="cd /opt/fraudradar && docker compose --profile spark --profile streaming --profile airflow"
AIRFLOW="$COMPOSE exec -T airflow-scheduler airflow"

case "$ACTION" in
  start)
    CLUSTER="$(stack_output MskClusterArn)"
    BROKERS="$("${AWS[@]}" kafka get-bootstrap-brokers --cluster-arn "$CLUSTER" --query BootstrapBrokerStringTls --output text)"
    # Cada MSK nuevo reinicia los offsets: carpeta de checkpoint y _session nuevas
    SESSION="aws-$(date -u +%Y%m%dT%H%M)"
    echo "STREAM_SESSION=$SESSION"
    run_remote "$SPARK" "bash /opt/fraudradar/infra/remote/start_pipeline.sh '$BROKERS' '$S3_BUCKET' '$REGION' '$SESSION'" ;;
  status)
    run_remote "$SPARK" "$COMPOSE ps --format 'table {{.Service}}\t{{.Status}}'; for d in daily_batch lakehouse_15min; do echo \"--- \$d\"; $AIRFLOW dags list-runs -d \$d -o plain 2>/dev/null | head -4; done" ;;
  dag)
    DAG="${2:?Uso: pipeline_aws.sh dag <daily_batch|lakehouse_15min>}"
    run_remote "$SPARK" "$AIRFLOW dags unpause $DAG && $AIRFLOW dags trigger $DAG" ;;
  logs)
    run_remote "$SPARK" "$COMPOSE logs --tail ${3:-40} ${2:?Uso: pipeline_aws.sh logs <servicio>}" ;;
  register)
    run_remote "$SPARK" "$COMPOSE run --rm --no-deps -v /opt/fraudradar/sql/athena:/opt/fraudradar/sql/athena:ro spark-master python3 /opt/fraudradar/sql/athena/register_gold_tables.py --database fraudradar --lake-uri s3a://$S3_BUCKET/lake --region $REGION" ;;
  stop)
    run_remote "$SPARK" "$COMPOSE stop" ;;
  *) echo "Uso: pipeline_aws.sh start|status|dag <id>|logs <servicio>|register|stop"; exit 1 ;;
esac
