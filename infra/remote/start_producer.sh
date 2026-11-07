#!/usr/bin/env bash
# Corre EN LA EC2 (lo lanza producer_aws.sh por SSM). Genera los catálogos y arranca el productor.
# Uso: bash start_producer.sh <brokers TLS> [tasa en ev/s]
set -euo pipefail
export HOME=/root
BROKERS="${1:?Falta la lista de brokers TLS}"
RATE="${2:-20}"

cd /opt/fraudradar
docker compose --profile producer down 2>/dev/null || true

# Catálogos: el universo de usuarios debe ser >= tasa x 600 (ver producer/generate_catalogs.py)
USERS=$(( RATE * 600 > 100000 ? RATE * 600 : 100000 ))
rm -rf data/catalogs
python3 producer/generate_catalogs.py --users "$USERS" --merchants 1000 --seed 42 --out data/catalogs

cat > .env <<EOF
KAFKA_BOOTSTRAP_SERVERS=$BROKERS
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_TOPIC=transactions
PRODUCER_RATE_EPS=$RATE
PRODUCER_FRAUD_RATIO=0.02
EOF

docker compose --profile producer up -d --build
sleep 15
docker compose --profile producer ps
docker compose --profile producer logs --tail 8 producer
