"""build_user_profile: silver/transactions (+ perfil_inicial) -> silver/user_profile.

Batch diario (lo dispara Airflow). Calcula, por usuario, el monto promedio y el número de
transacciones de los últimos PROFILE_DAYS días. Es el insumo de la regla de monto atípico en
detect_fraud (monto > factor x avg_amount_30d).

Arranque en frío: al inicio el pipeline casi no tiene historial, y sin perfil la regla no podría
disparar. Por eso se parte de bronze/catalogs/perfil_inicial (el "historial previo" del core) y se
mezcla con lo observado. El peso del perfil inicial decae linealmente conforme el pipeline acumula
historial, y a los PROFILE_DAYS días de datos deja de contar:

    historia      = días entre la primera transacción de Silver y as_of, acotado a [0, PROFILE_DAYS]
    peso_inicial  = n_tx_30d_inicial x (1 - historia / PROFILE_DAYS)
    avg           = (avg_inicial x peso_inicial + suma_real) / (peso_inicial + n_real)

Idempotente: siempre se calcula desde Silver y desde el perfil inicial (nunca desde el perfil
anterior), así que correr dos veces con el mismo --as-of da el mismo resultado y no se acumula.

No lee las etiquetas del productor (is_fraud_injected, attack_*): el perfil se calcula con todas
las transacciones, como lo haría el sistema real, que no sabe cuáles son fraude.

Uso:
  spark-submit spark/batch/build_user_profile.py [--as-of 2026-11-08T00:00:00] [--run-id <run_id>]
"""
import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

import reglas
from spark_session import get_spark, lake_path

PROFILE_DAYS = reglas.MONTO_ATIPICO["profile_days"]
SECONDS_PER_DAY = 86400.0


def compute_profile(tx, initial, as_of):
    """Perfil por usuario a la fecha `as_of` (datetime UTC).

    tx:      user_id, event_ts, amount_cop
    initial: user_id, avg_amount_30d, n_tx_30d  (perfil_inicial)
    Devuelve: user_id, avg_amount_30d, n_tx_30d, profile_source, as_of
    """
    # as_of viaja como segundos epoch: no depende de la zona horaria de Python ni de la del sistema
    as_of_epoch = as_of.timestamp()
    as_of_lit = F.lit(as_of_epoch).cast("timestamp")
    window_start = as_of_lit - F.expr(f"INTERVAL {PROFILE_DAYS} DAYS")

    # Solo lo ocurrido antes de as_of: así el perfil de un día no se contamina con eventos posteriores
    tx = tx.filter(F.col("event_ts") < as_of_lit)

    first_epoch = tx.agg(F.min(F.col("event_ts").cast("double"))).first()[0]
    if first_epoch is None:
        history_days = 0.0
    else:
        history_days = min(max((as_of_epoch - first_epoch) / SECONDS_PER_DAY, 0.0), float(PROFILE_DAYS))
    initial_weight_factor = 1.0 - history_days / PROFILE_DAYS

    real = (
        tx.filter(F.col("event_ts") >= window_start)
        .groupBy("user_id")
        .agg(F.count("*").alias("n_real"), F.sum("amount_cop").alias("sum_real"))
    )
    init = initial.select(
        "user_id",
        F.col("avg_amount_30d").cast("double").alias("avg_init"),
        (F.col("n_tx_30d").cast("double") * F.lit(initial_weight_factor)).alias("w_init"),
    )

    both = real.join(init, "user_id", "full_outer").select(
        "user_id",
        F.coalesce("n_real", F.lit(0)).cast("double").alias("n_real"),
        F.coalesce("sum_real", F.lit(0)).cast("double").alias("sum_real"),
        F.coalesce("avg_init", F.lit(0.0)).alias("avg_init"),
        F.coalesce("w_init", F.lit(0.0)).alias("w_init"),
    )
    n_eff = F.col("n_real") + F.col("w_init")
    return (
        both.filter(n_eff > 0)  # sin historial ni perfil inicial no hay promedio que calcular
        .select(
            "user_id",
            ((F.col("avg_init") * F.col("w_init") + F.col("sum_real")) / n_eff).alias("avg_amount_30d"),
            F.round(n_eff).cast("long").alias("n_tx_30d"),
            F.when(F.col("w_init") == 0, "real")
            .when(F.col("n_real") == 0, "inicial")
            .otherwise("mezcla")
            .alias("profile_source"),
        )
        .withColumn("as_of", as_of_lit)
    )


def latest_catalog(spark, name):
    """Última corrida de un catálogo en Bronze (mismo criterio que bronze_to_silver)."""
    df = spark.read.format("delta").load(lake_path("bronze", f"catalogs/{name}"))
    last_run = df.agg(F.max("_run_id")).first()[0]
    return df.filter(F.col("_run_id") == last_run)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--as-of", help="Fecha de corte UTC (YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS). Por defecto, ahora.")
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H:%M:%S"))
    args = p.parse_args()

    if args.as_of:
        fmt = "%Y-%m-%dT%H:%M:%S" if "T" in args.as_of else "%Y-%m-%d"
        as_of = datetime.strptime(args.as_of, fmt).replace(tzinfo=timezone.utc)
    else:
        as_of = datetime.now(timezone.utc).replace(microsecond=0)

    spark = get_spark("build_user_profile")
    spark.sparkContext.setLogLevel("WARN")

    tx_path = lake_path("silver", "transactions")
    if DeltaTable.isDeltaTable(spark, tx_path):
        tx = spark.read.format("delta").load(tx_path).select("user_id", "event_ts", "amount_cop")
    else:
        # Primer arranque (lake vacío): todavía no hay Silver. El perfil sale solo de perfil_inicial,
        # y así detect_fraud puede arrancar antes de la primera corrida de bronze_to_silver.
        print(f"{tx_path} no existe todavía: perfil solo con perfil_inicial")
        tx = spark.createDataFrame([], "user_id string, event_ts timestamp, amount_cop long")
    initial = latest_catalog(spark, "perfil_inicial")

    profile = (
        compute_profile(tx, initial, as_of)
        .withColumn("_run_id", F.lit(args.run_id))
        .withColumn("_updated_at", F.current_timestamp())
    )
    # Tabla pequeña (una fila por usuario): se reescribe completa. Delta deja la versión anterior
    # disponible con time travel, y detect_fraud (stream-static) lee siempre la última versión.
    profile.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(lake_path("silver", "user_profile"))

    out = spark.read.format("delta").load(lake_path("silver", "user_profile"))
    by_source = {r["profile_source"]: r["count"] for r in out.groupBy("profile_source").count().collect()}
    print(f"user_profile al {as_of.isoformat()}: {out.count()} usuarios {by_source}")
    spark.stop()


if __name__ == "__main__":
    main()
