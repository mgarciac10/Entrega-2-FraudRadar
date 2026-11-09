"""Verifica build_user_profile sobre un lakehouse local, sin Kafka. Corre DENTRO del contenedor de Spark.

Carga los catálogos (incluido perfil_inicial), arma a mano un Silver con transacciones de un usuario
y corre el job: comprueba la mezcla con el perfil inicial, el decaimiento del peso inicial,
la ventana de 30 días, que no se acumule al repetir y que no toque a usuarios sin actividad.
Lo lanza tests/run_spark_checks.sh.
"""
import csv
import os
import subprocess
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

from spark_session import get_spark

LANDING = os.environ["LANDING_URI"]
LAKE = os.environ["LAKE_URI"]
SPARK_DIR = "/opt/fraudradar/spark"

failures = []


def check(cond, msg):
    print(("OK    " if cond else "FALLA ") + msg)
    if not cond:
        failures.append(msg)


def submit(script, *args):
    cmd = ["/opt/spark/bin/spark-submit", "--master", "local[2]", f"{SPARK_DIR}/{script}", *args]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        print(out.stdout[-3000:], out.stderr[-3000:])
        sys.exit(f"{script} falló")
    return out.stdout


def initial_profiles():
    with open(f"{LANDING}/perfil_inicial.csv", newline="", encoding="utf-8") as f:
        return {r["user_id"]: (float(r["avg_amount_30d"]), int(float(r["n_tx_30d"]))) for r in csv.DictReader(f)}


def write_silver(spark, rows):
    df = spark.createDataFrame(rows, "user_id string, ts string, amount_cop long").selectExpr(
        "user_id", "to_timestamp(ts) as event_ts", "amount_cop")
    df.write.format("delta").mode("overwrite").save(f"{LAKE}/silver/transactions")


def profile(spark):
    rows = spark.read.format("delta").load(f"{LAKE}/silver/user_profile").collect()
    return {r["user_id"]: r for r in rows}


submit("batch/ingest_catalogs.py", "--run-id", "2026-11-07")
init = initial_profiles()
# Solo cuentan usuarios con historial inicial (n > 0). U1 tendrá transacciones; U2 y U3 solo perfil inicial
con_historial = [u for u, (_, n) in init.items() if n > 0]
U1, U2, U3 = con_historial[:3]
avg1, n1 = init[U1]

spark = get_spark("check_build_user_profile")
spark.sparkContext.setLogLevel("ERROR")

# Lake vacío (primer arranque de una sesión): sin silver/transactions, el perfil sale de perfil_inicial
submit("batch/build_user_profile.py", "--as-of", "2026-11-08", "--run-id", "run-0")
p0 = profile(spark)
check(len(p0) == len(con_historial) and {r["profile_source"] for r in p0.values()} == {"inicial"},
      f"sin Silver, perfil solo con perfil_inicial ({len(p0)} usuarios)")

# Silver con 1 día de historia antes del corte (2026-11-08): 3 transacciones de 200.000 de U1
# y una de U1 posterior al corte, que no debe contar
write_silver(spark, [
    (U1, "2026-11-07 00:00:00", 200000), (U1, "2026-11-07 06:00:00", 200000), (U1, "2026-11-07 12:00:00", 200000),
    (U1, "2026-11-09 10:00:00", 9999999),
])
out = submit("batch/build_user_profile.py", "--as-of", "2026-11-08", "--run-id", "run-1")
p = profile(spark)
w = n1 * 29 / 30  # peso del perfil inicial con 1 día de historia
esperado = (avg1 * w + 600000) / (w + 3)
check(len(p) == len(con_historial), f"un perfil por usuario con historial inicial ({len(p)} de {len(con_historial)})")
check(abs(p[U1]["avg_amount_30d"] - esperado) < 0.01, f"U1 mezcla inicial y real: {p[U1]['avg_amount_30d']:.2f} (esperado {esperado:.2f})")
check(p[U1]["profile_source"] == "mezcla" and p[U1]["n_tx_30d"] == round(w + 3), f"U1 marcado como mezcla, n={p[U1]['n_tx_30d']}")
check(p[U2]["profile_source"] == "inicial" and abs(p[U2]["avg_amount_30d"] - init[U2][0]) < 0.01,
      "usuario sin transacciones conserva su perfil inicial")
dtypes = dict(spark.read.format("delta").load(f"{LAKE}/silver/user_profile").dtypes)
check(dtypes["avg_amount_30d"] == "double" and dtypes["n_tx_30d"] == "bigint" and dtypes["as_of"] == "timestamp", "tipos de la tabla")
check(p[U1]["_run_id"] == "run-1", "lleva el run_id")
check("user_profile al" in out, "el job imprime su resumen")

# Repetir con el mismo corte: mismo resultado (no se acumula)
submit("batch/build_user_profile.py", "--as-of", "2026-11-08", "--run-id", "run-2")
p2 = profile(spark)
check(len(p2) == len(p) and abs(p2[U1]["avg_amount_30d"] - p[U1]["avg_amount_30d"]) < 1e-9, "repetir con el mismo as-of no cambia el perfil")

# 40 días de historia: el perfil inicial deja de contar y U1 queda con lo real de la ventana de 30 días
write_silver(spark, [
    (U1, "2026-09-29 00:00:00", 999999),  # fuera de la ventana
    (U1, "2026-10-20 00:00:00", 100000), (U1, "2026-11-01 00:00:00", 300000),
])
submit("batch/build_user_profile.py", "--as-of", "2026-11-08", "--run-id", "run-3")
p3 = profile(spark)
check(abs(p3[U1]["avg_amount_30d"] - 200000.0) < 0.01 and p3[U1]["n_tx_30d"] == 2 and p3[U1]["profile_source"] == "real",
      "con 30+ días de historia solo cuenta lo real de la ventana")
check(U2 not in p3 and U3 not in p3, "sin actividad en la ventana ni peso inicial, no hay perfil")

spark.stop()
print("\nTodo bien." if not failures else f"\n{len(failures)} verificaciones fallidas.")
sys.exit(1 if failures else 0)
