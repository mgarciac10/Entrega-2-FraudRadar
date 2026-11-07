#!/usr/bin/env bash
# Corre EN LA EC2 (lo lanza producer_aws.sh por SSM). Detiene el productor.
set -uo pipefail
export HOME=/root
cd /opt/fraudradar
docker compose --profile producer logs --tail 2 producer
docker compose --profile producer down
echo "Productor detenido."
