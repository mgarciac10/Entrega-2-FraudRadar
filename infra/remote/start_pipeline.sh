#!/usr/bin/env bash
# Corre EN LA EC2 DE SPARK (lo lanza pipeline_aws.sh por SSM). Arranca el clúster de Spark, los dos
# jobs de streaming (ingest-bronze, detect-fraud) y Airflow, todo contra MSK, S3 y DynamoDB.
# Airflow va en esta EC2 y no en la del productor: lanza los batch como driver dentro de su contenedor,
# y los ejecutores tienen que poder conectarse de vuelta a él por la red de Docker (ADR-05).
# Uso: bash start_pipeline.sh <brokers TLS> <bucket> <región> <sesión de streaming>
set -euo pipefail
export HOME=/root
BROKERS="${1:?Falta la lista de brokers TLS}"
BUCKET="${2:?Falta el bucket}"
REGION="${3:?Falta la región}"
SESSION="${4:?Falta la sesión de streaming}"

cd /opt/fraudradar
mkdir -p data airflow/logs
# Spark (UID 185) y Airflow (UID 50000, grupo 0) escriben en data/ y airflow/logs
chgrp -R 0 data airflow/logs && chmod -R g+rwX data airflow/logs

# Rutas explícitas: sin ellas Airflow caería en /data/lake mientras el streaming escribe en S3.
# t3.xlarge (4 vCPU, 16 GB) con Airflow en la misma máquina: 4 cupos y 6 GB para el worker.
cat > .env <<EOF
AWS_REGION=$REGION
S3_BUCKET=$BUCKET
LAKE_URI=s3a://$BUCKET/lake
LANDING_URI=s3a://$BUCKET/landing
CHECKPOINT_URI=s3a://$BUCKET/checkpoints
KAFKA_BOOTSTRAP_SERVERS=$BROKERS
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_TOPIC=transactions
STREAM_SESSION=$SESSION
ALERTS_SINK=dynamodb
ALERTS_SOURCE=dynamodb
DYNAMODB_ALERTS_TABLE=fraud_alerts
DYNAMODB_ENDPOINT_URL=
SPARK_WORKER_CORES=4
SPARK_WORKER_MEMORY=6g
STREAM_EXECUTOR_MEMORY=1g
EOF

PROFILES="--profile spark --profile streaming --profile airflow"
docker compose $PROFILES up -d --build
sleep 20
docker compose $PROFILES ps --format 'table {{.Service}}\t{{.Status}}'
