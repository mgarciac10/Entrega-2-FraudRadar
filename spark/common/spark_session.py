"""SparkSession y rutas del lakehouse compartidas por todos los jobs.

Todos los jobs deben usar `get_spark()` y `lake_path()` para que la configuración de Delta y de S3
y la estructura de carpetas sean las mismas en todos.

Las ubicaciones salen de variables de entorno, así el mismo código corre en AWS y en local:
  LAKE_URI      raíz del lakehouse        s3a://<bucket>/lake   (local: /data/lake)
  LANDING_URI   catálogos de la fuente    s3a://<bucket>/landing (local: /data/catalogs)
  CHECKPOINT_URI checkpoints de streaming s3a://<bucket>/checkpoints
Si no están definidas pero sí S3_BUCKET, se derivan de él.

Credenciales de S3: en la EC2 vienen del rol de la instancia (cadena por defecto de S3A). En local,
variables AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN.
"""
import os

from pyspark.sql import SparkSession


def _uri(name, suffix):
    value = os.environ.get(name)
    if value:
        return value.rstrip("/")
    bucket = os.environ.get("S3_BUCKET")
    if bucket:
        return f"s3a://{bucket}/{suffix}"
    raise RuntimeError(f"Define {name} o S3_BUCKET")


def lake_uri():
    return _uri("LAKE_URI", "lake")


def landing_uri():
    return _uri("LANDING_URI", "landing")


def checkpoint_uri():
    return _uri("CHECKPOINT_URI", "checkpoints")


def lake_path(layer, table):
    """Ruta de una tabla Delta: lake_path('bronze', 'transactions') -> <lake>/bronze/transactions."""
    return f"{lake_uri()}/{layer}/{table}"


def get_spark(app_name, **conf):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # 6 particiones de Kafka: no hace falta el valor por defecto de 200 para este volumen
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "6"))
        .config("spark.sql.session.timeZone", "UTC")
        # El driver de Airflow y los ejecutores Spark usan UID distintos en el volumen local.
        # Deja archivos y directorios Delta con escritura de grupo para que ambos puedan confirmar.
        .config("spark.hadoop.fs.permissions.umask-mode", "0002")
        # S3A: inofensivo si el job trabaja con rutas locales
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-2"))
        # Rol de la instancia en la EC2, o variables de entorno en local
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )
    if _lake_on_local_disk():
        # Disco local (solo desarrollo): sin sumas de verificación .crc. Con el sistema de archivos
        # local por defecto, Delta falla con ChecksumException al releer su propio _delta_log.
        # NO en S3: S3A prepara las subidas en disco local y exige LocalFileSystem; con esto falla
        # con ClassCastException (RawLocalFileSystem cannot be cast to LocalFileSystem).
        builder = builder.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)
    return builder.getOrCreate()


def _lake_on_local_disk():
    try:
        return not lake_uri().startswith(("s3a://", "s3://"))
    except RuntimeError:  # sin LAKE_URI ni S3_BUCKET: pruebas que solo usan rutas locales
        return True
