#!/usr/bin/env bash
# Corre las verificaciones de Spark dentro del contenedor, sobre un lakehouse local.
# Construye la imagen la primera vez (descarga ~1 GB; después es rápido).
# Uso: bash tests/run_spark_checks.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."

docker build -q -f spark/Dockerfile -t fraudradar-spark .

# Catálogos pequeños y un lakehouse local desechable
rm -rf data/test_spark
python producer/generate_catalogs.py --users 2000 --merchants 100 --seed 3 --out data/test_spark/catalogs > /dev/null
HERE="$(pwd -W 2>/dev/null || pwd)"

docker run --rm \
  -v "$HERE/data/test_spark:/data" \
  -v "$HERE/spark:/opt/fraudradar/spark:ro" \
  -v "$HERE/tests:/opt/fraudradar/tests:ro" \
  -e LANDING_URI=/data/catalogs -e LAKE_URI=/data/lake \
  fraudradar-spark \
  /opt/spark/bin/spark-submit --master "local[2]" /opt/fraudradar/tests/spark_check_ingest_catalogs.py
