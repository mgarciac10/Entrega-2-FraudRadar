#!/usr/bin/env bash
# Corre EN LA EC2 (lo lanza producer_aws.sh por SSM). Verifica que el productor está llegando a MSK.
# Uso: bash check_producer.sh <brokers TLS>
set -uo pipefail
export HOME=/root
BROKERS="${1:?Falta la lista de brokers TLS}"
printf 'security.protocol=SSL\n' > /tmp/client.properties
KAFKA="docker run --rm -v /tmp/client.properties:/client.properties:ro apache/kafka:3.9.0"

cd /opt/fraudradar
echo "--- contenedor del productor:"
docker compose --profile producer ps
docker compose --profile producer logs --tail 3 producer

echo "--- mensajes por partición (offset final):"
OFFSETS="$($KAFKA /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BROKERS" --command-config /client.properties --topic transactions 2>&1)"
echo "$OFFSETS"
echo "$OFFSETS" | awk -F: 'NF==3 {s+=$3} END {print "TOTAL mensajes en el topic:", s}'

echo "--- 2 mensajes de la partición 0:"
$KAFKA /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BROKERS" --consumer.config /client.properties \
  --topic transactions --partition 0 --offset earliest --max-messages 2 --timeout-ms 20000 2>&1 | grep -v "^Processed"
