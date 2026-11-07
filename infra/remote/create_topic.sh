#!/usr/bin/env bash
# Corre EN LA EC2 (lo lanza producer_aws.sh por SSM). Crea el topic transactions en MSK.
# Uso: bash create_topic.sh <brokers TLS separados por coma>
set -euo pipefail
export HOME=/root
BROKERS="${1:?Falta la lista de brokers TLS}"
TOPIC=transactions

# Cliente de Kafka por TLS: los brokers de MSK usan certificados de Amazon, que Java ya confía.
printf 'security.protocol=SSL\n' > /tmp/client.properties
KAFKA="docker run --rm -v /tmp/client.properties:/client.properties:ro apache/kafka:3.9.0"

# 6 particiones (llave user_id) y RF 2, el máximo con 2 brokers (ADR-01).
$KAFKA /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKERS" --command-config /client.properties \
  --create --if-not-exists --topic "$TOPIC" --partitions 6 --replication-factor 2

echo "--- descripción del topic:"
$KAFKA /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKERS" --command-config /client.properties \
  --describe --topic "$TOPIC"
