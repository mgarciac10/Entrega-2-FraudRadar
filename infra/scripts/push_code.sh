#!/usr/bin/env bash
# Envía el código del proyecto a las dos EC2 (vía S3 y SSM), sin GitHub ni SSH: la del productor y la
# de Spark + Airflow. Sube lo que hay en tu carpeta, aunque no esté en un commit. Queda en /opt/fraudradar.
# Uso: bash infra/scripts/push_code.sh
set -euo pipefail
. "$(dirname "$0")/lib.sh"
: "${S3_BUCKET:?Define S3_BUCKET en el .env}"

python - <<'PYEOF'
import glob, os, zipfile
files = (glob.glob("producer/*.py") + ["producer/Dockerfile", "producer/requirements.txt"]
         + glob.glob("spark/**/*.py", recursive=True) + ["spark/Dockerfile"]
         + ["airflow/Dockerfile"] + glob.glob("airflow/dags/*.py")
         + glob.glob("sql/athena/*") + glob.glob("tests/*.py") + glob.glob("tests/fixtures/*")
         + glob.glob("analyst/*.py") + glob.glob("infra/local/*.py")
         + glob.glob("infra/remote/*.sh") + ["docker-compose.yml"])
with zipfile.ZipFile("infra/.build/code.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for f in sorted(files):
        z.write(f, f.replace(os.sep, "/"))
print(f"{len(files)} archivos empaquetados")
PYEOF

KEY="artifacts/code/fraudradar-code.zip"
"${AWS[@]}" s3 cp infra/.build/code.zip "s3://$S3_BUCKET/$KEY" --only-show-errors

APP="$(stack_output AppInstanceId)"
SPARK="$(stack_output SparkInstanceId)"
[ -n "$APP" ] && [ "$APP" != "None" ] || { echo "No hay stack por sesión. Ejecuta session_up.sh."; exit 1; }

for id in "$APP" "$SPARK"; do
  echo "--- $id"
  run_remote "$id" "set -e; mkdir -p /opt/fraudradar; aws s3 cp s3://$S3_BUCKET/$KEY /tmp/code.zip --region $REGION --only-show-errors; python3 -m zipfile -e /tmp/code.zip /opt/fraudradar; ls /opt/fraudradar"
done
