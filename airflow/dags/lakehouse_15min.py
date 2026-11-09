"""DAG de actualización Bronze → Silver → Gold cada 15 minutos."""
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import boto3
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator

MAX_BRONZE_AGE = timedelta(minutes=10)
SPARK_SUBMIT = (
    "umask 0002 && spark-submit --master spark://spark-master:7077 "
    "--conf spark.hadoop.fs.permissions.umask-mode=0002 "
    "--conf spark.driver.host=airflow-scheduler "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.cores.max=1 --executor-memory 1g "
    "--jars /opt/spark-extra-jars/delta-spark_2.12-3.2.1.jar,"
    "/opt/spark-extra-jars/delta-storage-3.2.1.jar,"
    "/opt/spark-extra-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark-extra-jars/aws-java-sdk-bundle-1.12.262.jar"
)


class BronzeFreshnessSensor(BaseSensorOperator):
    def poke(self, context):
        lake_uri = os.environ.get("LAKE_URI", "/data/lake").rstrip("/")
        delta_uri = f"{lake_uri}/bronze/transactions/_delta_log"
        parsed = urlparse(delta_uri)
        if parsed.scheme in {"s3", "s3a"}:
            s3 = boto3.client(
                "s3", region_name=os.environ.get("AWS_REGION", "us-east-2")
            )
            timestamps = []
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=parsed.netloc, Prefix=f"{parsed.path.strip('/')}/"
            ):
                timestamps.extend(
                    item["LastModified"]
                    for item in page.get("Contents", [])
                    if item["Key"].rsplit("/", 1)[-1].endswith(".json")
                )
        elif not parsed.scheme:
            delta_path = Path(delta_uri)
            timestamps = [
                datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                for path in delta_path.glob("*.json")
            ] if delta_path.is_dir() else []
        else:
            raise AirflowException(f"Esquema de LAKE_URI no soportado por el sensor: {parsed.scheme}")

        if not timestamps:
            logging.info(
                "No hay commits Delta en %s; se volverá a consultar. "
                "En local, inicie el productor y el servicio ingest-bronze "
                "y espere a que escriba datos en /data/lake.",
                delta_uri,
            )
            return False
        latest = max(timestamps)
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - latest.astimezone(timezone.utc)
        fresh = timedelta(0) <= age < MAX_BRONZE_AGE
        logging.info("Último commit Bronze: %s; antigüedad=%s; fresco=%s", latest, age, fresh)
        return fresh


DEFAULT_ARGS = {
    "owner": "FraudRadar",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_15min",
    description="Procesa Bronze→Silver→Gold y valida la calidad del lakehouse.",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudradar", "lakehouse"],
) as dag:
    check_bronze_freshness = BronzeFreshnessSensor(
        task_id="check_bronze_freshness",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/bronze_to_silver.py "
        "--run-id '{{ run_id }}'",
    )
    extract_alert_status = BashOperator(
        task_id="extract_alert_status",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/extract_alert_status.py "
        "--run-id '{{ run_id }}'",
    )
    build_dimensions = BashOperator(
        task_id="build_dimensions",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/silver_to_gold.py "
        "--dimensions-only --run-id '{{ run_id }}'",
    )
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/silver_to_gold.py "
        "--facts-only --run-id '{{ run_id }}'",
    )
    data_quality_checks = BashOperator(
        task_id="data_quality_checks",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/data_quality_checks.py "
        "--run-id '{{ run_id }}'",
    )

    check_bronze_freshness >> bronze_to_silver
    bronze_to_silver >> extract_alert_status >> build_dimensions >> silver_to_gold >> data_quality_checks
