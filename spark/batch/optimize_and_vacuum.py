"""Compacta tablas Delta y aplica VACUUM con la retención segura por defecto de Delta."""
import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable

from spark_session import get_spark, lake_path

TABLES = (
    "bronze/catalogs/ciudades",
    "bronze/catalogs/usuarios",
    "bronze/catalogs/comercios",
    "bronze/catalogs/perfil_inicial",
    "bronze/transactions",
    "silver/transactions",
    "silver/transactions_rejected",
    "silver/users",
    "silver/merchants",
    "silver/cities",
    "silver/user_profile",
    "silver/alert_status",
    "gold/dim_date",
    "gold/dim_time",
    "gold/dim_user",
    "gold/dim_merchant",
    "gold/dim_location",
    "gold/dim_channel",
    "gold/dim_rule",
    "gold/fact_transaction",
    "gold/fact_alert",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H:%M:%S"))
    parser.add_argument("--retention-hours", type=int, default=168)
    args = parser.parse_args()
    if args.retention_hours < 168:
        parser.error("--retention-hours no puede ser menor a 168 horas")

    spark = get_spark("optimize_and_vacuum")
    spark.sparkContext.setLogLevel("WARN")
    for table in TABLES:
        path = lake_path(*table.split("/", 1))
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"omite {path}: tabla aún no creada")
            continue
        delta = DeltaTable.forPath(spark, path)
        delta.optimize().executeCompaction()
        removed = delta.vacuum(args.retention_hours)
        print(f"{path}: compactada; VACUUM eliminó {removed.count()} archivos")
    print(f"optimize_and_vacuum completado (run_id={args.run_id})")
    spark.stop()


if __name__ == "__main__":
    main()
