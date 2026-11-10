"""DAG diario para catálogos, perfil de usuarios y mantenimiento Delta."""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

CATALOGS = ("ciudades", "usuarios", "comercios", "perfil_inicial")


def check_local_catalogs():
    landing = os.environ.get("LANDING_URI", "/data/catalogs").rstrip("/")
    if urlparse(landing).scheme:
        return

    missing = [name for name in CATALOGS if not (Path(landing) / f"{name}.csv").is_file()]
    if missing:
        raise AirflowException(
            f"Faltan catálogos en {landing}: {', '.join(f'{name}.csv' for name in missing)}. "
            "Genéralos en la raíz del repositorio con "
            "'python producer/generate_catalogs.py --users 20000 --out data/catalogs'. "
            "El directorio ./data del host debe estar montado en /data."
        )

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
DEFAULT_ARGS = {
    "owner": "FraudRadar",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_batch",
    description="Carga catálogos, actualiza el perfil y compacta las tablas Delta.",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fraudradar", "lakehouse"],
) as dag:
    check_catalog_inputs = PythonOperator(
        task_id="check_catalog_inputs",
        python_callable=check_local_catalogs,
    )
    ingest_catalogs = BashOperator(
        task_id="ingest_catalogs",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/ingest_catalogs.py "
        "--run-id '{{ ds }}'",
    )
    build_user_profile = BashOperator(
        task_id="build_user_profile",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/build_user_profile.py "
        "--run-id '{{ run_id }}'",
    )
    optimize_and_vacuum = BashOperator(
        task_id="optimize_and_vacuum",
        bash_command=f"{SPARK_SUBMIT} /opt/fraudradar/spark/batch/optimize_and_vacuum.py "
        "--run-id '{{ run_id }}'",
    )

    check_catalog_inputs >> ingest_catalogs >> build_user_profile >> optimize_and_vacuum
