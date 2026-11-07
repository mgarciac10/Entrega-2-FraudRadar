"""Verifica ingest_catalogs sobre un lakehouse local. Corre DENTRO del contenedor de Spark.

Comprueba que Bronze quedó bien (filas, columnas de linaje, texto sin convertir) y que repetir la
misma corrida es idempotente. Lo lanza tests/run_spark_checks.sh.
"""
import csv
import os
import subprocess
import sys

from pyspark.sql import functions as F

from spark_session import get_spark

LANDING = os.environ["LANDING_URI"]
LAKE = os.environ["LAKE_URI"]
RUN = "2026-11-07"
SUBMIT = ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/opt/fraudradar/spark/batch/ingest_catalogs.py",
          "--run-id", RUN]


def csv_rows(name):
    with open(f"{LANDING}/{name}.csv", newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.DictReader(f))


def run_job(run_id=RUN):
    cmd = SUBMIT[:-1] + [run_id]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        print(out.stdout[-3000:], out.stderr[-3000:])
        sys.exit("ingest_catalogs falló")


failures = []


def check(cond, msg):
    print(("OK    " if cond else "FALLA ") + msg)
    if not cond:
        failures.append(msg)


run_job()
spark = get_spark("check_ingest_catalogs")
spark.sparkContext.setLogLevel("ERROR")

for name in ("ciudades", "usuarios", "comercios", "perfil_inicial"):
    df = spark.read.format("delta").load(f"{LAKE}/bronze/catalogs/{name}")
    expected = csv_rows(name)
    check(df.count() == expected, f"{name}: {df.count()} filas en Bronze = {expected} del CSV")
    check({"_source_file", "_ingested_at", "_run_id"} <= set(df.columns), f"{name}: columnas de linaje presentes")
    check(df.filter(F.col("_run_id") != RUN).count() == 0, f"{name}: todas las filas llevan el _run_id")
    # Bronze es crudo: todo texto, sin convertir
    data_cols = [c for c in df.columns if not c.startswith("_")]
    check(all(t == "string" for c, t in df.dtypes if c in data_cols), f"{name}: columnas de datos como texto")

# Tildes bien leídas (UTF-8)
ciudades = spark.read.format("delta").load(f"{LAKE}/bronze/catalogs/ciudades")
check(ciudades.filter(F.col("city") == "Bogotá D.C.").count() == 1, "UTF-8: 'Bogotá D.C.' se lee con su tilde")

# Idempotencia: repetir la misma corrida no duplica
before = {n: spark.read.format("delta").load(f"{LAKE}/bronze/catalogs/{n}").count() for n in ("usuarios", "comercios")}
run_job()
after = {n: spark.read.format("delta").load(f"{LAKE}/bronze/catalogs/{n}").count() for n in ("usuarios", "comercios")}
check(before == after, f"idempotente: repetir la corrida deja {after} (antes {before})")

# Otra corrida (otro día) se acumula sin tocar la anterior: Bronze es append por corrida
run_job("2026-11-08")
usuarios = spark.read.format("delta").load(f"{LAKE}/bronze/catalogs/usuarios")
runs = {r["_run_id"]: r["n"] for r in usuarios.groupBy("_run_id").agg(F.count("*").alias("n")).collect()}
check(set(runs) == {RUN, "2026-11-08"} and runs[RUN] == runs["2026-11-08"],
      f"una corrida nueva se suma sin tocar la anterior: {runs}")

spark.stop()
print("\nTodo bien." if not failures else f"\n{len(failures)} verificaciones fallidas.")
sys.exit(1 if failures else 0)
