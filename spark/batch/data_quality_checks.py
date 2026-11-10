"""Valida llaves, referencias, dominios y medidas del modelo Gold."""
import argparse
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from schemas import ATTACK_PATTERNS, TX_TYPES
from spark_session import get_spark, lake_path


def _table(spark, name):
    path = lake_path("gold", name)
    if not DeltaTable.isDeltaTable(spark, path):
        raise FileNotFoundError(f"Falta la tabla Gold requerida: {path}")
    return spark.read.format("delta").load(path)


def _assert_unique(df, key, label):
    duplicates = df.groupBy(key).count().filter(F.col("count") > 1).count()
    if duplicates:
        raise ValueError(f"{label}: {duplicates} llaves {key} duplicadas")


def _assert_no_rows(df, label):
    count = df.count()
    if count:
        raise ValueError(f"{label}: {count} filas incumplen el contrato")


def run_checks(spark):
    dimensions = {
        "dim_date": ("date_key",),
        "dim_time": ("time_key",),
        "dim_user": ("user_key",),
        "dim_merchant": ("merchant_key",),
        "dim_location": ("location_key",),
        "dim_channel": ("channel_key",),
        "dim_rule": ("rule_key",),
    }
    dims = {name: _table(spark, name) for name in dimensions}
    for name, (key,) in dimensions.items():
        _assert_unique(dims[name], key, name)
        _assert_no_rows(dims[name].filter(F.col(key).isNull()), f"{name}: llave nula")

    transactions = _table(spark, "fact_transaction")
    alerts = _table(spark, "fact_alert")
    _assert_unique(transactions, "transaction_id", "fact_transaction")
    _assert_unique(alerts, "alert_id", "fact_alert")
    _assert_no_rows(
        transactions.filter(
            F.col("transaction_id").isNull()
            | F.col("event_ts").isNull()
            | F.col("kafka_session").isNull()
            | F.col("kafka_partition").isNull()
            | F.col("kafka_offset").isNull()
            | F.col("date_key").isNull()
            | F.col("time_key").isNull()
            | F.col("user_key").isNull()
            | F.col("location_key").isNull()
            | F.col("channel_key").isNull()
            | F.col("tx_type").isNull()
            | (~F.col("tx_type").isin(*TX_TYPES))
            | F.col("amount_cop").isNull()
            | (F.col("amount_cop") <= 0)
        ),
        "fact_transaction: campos requeridos o monto inválido",
    )
    _assert_no_rows(
        transactions.filter(
            (F.col("is_fraud_injected") & (F.col("attack_id").isNull() | F.col("attack_pattern").isNull()))
            | (~F.col("is_fraud_injected") & (F.col("attack_id").isNotNull() | F.col("attack_pattern").isNotNull()))
            | F.col("is_fraud_injected").isNull()
            | (F.col("attack_pattern").isNotNull() & ~F.col("attack_pattern").isin(*ATTACK_PATTERNS))
        ),
        "fact_transaction: etiquetas de fraude inconsistentes",
    )
    _assert_no_rows(
        alerts.filter(
            F.col("alert_id").isNull()
            | F.col("date_key").isNull()
            | F.col("time_key").isNull()
            | F.col("user_key").isNull()
            | F.col("rule_key").isNull()
            | F.col("location_key").isNull()
            | F.col("status").isNull()
            | F.col("amount_at_risk_cop").isNull()
            | F.col("transaction_ids").isNull()
            | F.col("detection_latency_s").isNull()
            | (F.col("amount_at_risk_cop") < 0)
            | (~F.col("status").isin("abierta", "confirmada", "descartada"))
            | (F.col("detection_latency_s") < 0)
        ),
        "fact_alert: campos, estado, monto o latencia inválidos",
    )

    references = (
        (transactions, "date_key", dims["dim_date"], "date_key", "fact_transaction → dim_date"),
        (transactions, "time_key", dims["dim_time"], "time_key", "fact_transaction → dim_time"),
        (transactions, "user_key", dims["dim_user"], "user_key", "fact_transaction → dim_user"),
        (transactions, "merchant_key", dims["dim_merchant"], "merchant_key", "fact_transaction → dim_merchant"),
        (transactions, "location_key", dims["dim_location"], "location_key", "fact_transaction → dim_location"),
        (transactions, "channel_key", dims["dim_channel"], "channel_key", "fact_transaction → dim_channel"),
        (alerts, "date_key", dims["dim_date"], "date_key", "fact_alert → dim_date"),
        (alerts, "time_key", dims["dim_time"], "time_key", "fact_alert → dim_time"),
        (alerts, "user_key", dims["dim_user"], "user_key", "fact_alert → dim_user"),
        (alerts, "rule_key", dims["dim_rule"], "rule_key", "fact_alert → dim_rule"),
        (alerts, "location_key", dims["dim_location"], "location_key", "fact_alert → dim_location"),
    )
    for fact, fact_key, dimension, dimension_key, label in references:
        fact_keys = fact.filter(F.col(fact_key).isNotNull()).select(
            F.col(fact_key).alias("_fact_key")
        ).distinct()
        dimension_keys = dimension.select(F.col(dimension_key).alias("_dimension_key")).distinct()
        missing = fact_keys.join(
            dimension_keys,
            fact_keys["_fact_key"] == dimension_keys["_dimension_key"],
            "left_anti",
        ).count()
        if missing:
            raise ValueError(f"{label}: {missing} llaves foráneas sin dimensión")

    counts = {
        name: dims[name].count() for name in dimensions
    }
    counts.update({"fact_transaction": transactions.count(), "fact_alert": alerts.count()})
    print("Calidad Gold: " + ", ".join(f"{name}={count}" for name, count in counts.items()))
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("manual__%Y-%m-%dT%H:%M:%S"))
    args = parser.parse_args()
    spark = get_spark("data_quality_checks")
    spark.sparkContext.setLogLevel("WARN")
    run_checks(spark)
    print(f"data_quality_checks aprobado (run_id={args.run_id})")
    spark.stop()


if __name__ == "__main__":
    main()
