"""ingest_catalogs: carga los catálogos CSV de landing/ en Bronze (Delta).

Lo dispara el DAG daily_batch de Airflow, antes de build_user_profile. Airflow no mueve datos:
solo lanza este job.

  landing/ciudades.csv        ->  <lake>/bronze/catalogs/ciudades
  landing/usuarios.csv        ->  <lake>/bronze/catalogs/usuarios
  landing/comercios.csv       ->  <lake>/bronze/catalogs/comercios
  landing/perfil_inicial.csv  ->  <lake>/bronze/catalogs/perfil_inicial

Bronze es crudo: todas las columnas quedan como texto, tal como vienen del core, más metadatos de
linaje (_source_file, _ingested_at, _run_id). La conversión de tipos se hace en Silver.

Idempotente: cada corrida lleva un _run_id (por defecto la fecha UTC). Repetir la misma corrida
(reintento de Airflow) reemplaza solo las filas de ese _run_id; no duplica ni toca otras corridas.

Uso:
  spark-submit spark/batch/ingest_catalogs.py [--run-id 2026-11-07] [--landing URI] [--lake URI]
"""
import argparse
from datetime import datetime, timezone

from pyspark.sql import functions as F

from spark_session import get_spark, lake_uri, landing_uri

CATALOGS = ("ciudades", "usuarios", "comercios", "perfil_inicial")


def ingest(spark, landing, lake, run_id):
    results = {}
    for name in CATALOGS:
        df = (
            spark.read.option("header", True)
            .option("encoding", "UTF-8")
            .option("inferSchema", False)  # crudo: todo texto
            .csv(f"{landing}/{name}.csv")
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_run_id", F.lit(run_id))
        )
        target = f"{lake}/bronze/catalogs/{name}"
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_run_id = '{run_id}'")  # idempotente por corrida
            .save(target)
        )
        results[name] = df.count()
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    p.add_argument("--landing", default=None)
    p.add_argument("--lake", default=None)
    args = p.parse_args()

    spark = get_spark("ingest_catalogs")
    spark.sparkContext.setLogLevel("WARN")
    counts = ingest(spark, args.landing or landing_uri(), args.lake or lake_uri(), args.run_id)
    for name, n in counts.items():
        print(f"bronze/catalogs/{name}: {n} filas (run_id={args.run_id})")
    spark.stop()


if __name__ == "__main__":
    main()
