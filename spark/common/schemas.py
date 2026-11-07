"""Contratos de datos de FraudRadar (fuente única de verdad).

Es el contrato entre integrantes: el productor emite este evento, y
ingest_bronze, detect_fraud y bronze_to_silver lo leen. Si cambia, hay que
avisar al equipo y actualizar spark/common/README.md.

Tipos permitidos: string, long, double, boolean, timestamp.
"""

# (campo, tipo, admite nulo, descripción)
TRANSACTION_EVENT = [
    ("transaction_id", "string", False, "UUID v4 de la transacción"),
    ("event_ts", "timestamp", False, "Cuándo ocurrió, ISO-8601 UTC con milisegundos (2026-11-07T22:40:02.123Z)"),
    ("user_id", "string", False, "Titular de la cuenta; es la llave de partición en Kafka"),
    ("tx_type", "string", False, "PAGO | P2P | RETIRO"),
    ("channel", "string", False, "app | qr | corresponsal"),
    ("amount_cop", "long", False, "Monto en pesos colombianos"),
    ("merchant_id", "string", True, "Solo en PAGO"),
    ("counterparty_user_id", "string", True, "Solo en P2P: quién recibe"),
    ("city_code", "string", False, "Ciudad desde donde se hizo (ver ciudades.csv)"),
    ("lat", "double", False, "Latitud del dispositivo o corresponsal"),
    ("lon", "double", False, "Longitud del dispositivo o corresponsal"),
    # Etiquetas del productor. Las reglas NO las leen (detect_fraud las ignora):
    # solo sirven para medir precisión, recall y fraude evitable en Gold (Q8, Q9).
    ("is_fraud_injected", "boolean", False, "True si el productor generó la transacción como parte de un ataque"),
    ("attack_id", "string", True, "Identificador del ataque; nulo si no es fraude"),
    ("attack_pattern", "string", True, "rafaga | monto_acumulado | monto_atipico | viaje_imposible; nulo si no es fraude"),
]

TX_TYPES = ("PAGO", "P2P", "RETIRO")
CHANNELS = ("app", "qr", "corresponsal")
ATTACK_PATTERNS = ("rafaga", "monto_acumulado", "monto_atipico", "viaje_imposible")

FIELD_NAMES = [f[0] for f in TRANSACTION_EVENT]
REQUIRED_FIELDS = [f[0] for f in TRANSACTION_EVENT if not f[2]]

_SPARK_TYPES = {
    "string": "StringType",
    "long": "LongType",
    "double": "DoubleType",
    "boolean": "BooleanType",
    "timestamp": "TimestampType",
}


def transaction_struct():
    """StructType de PySpark del evento (importa pyspark solo si se llama)."""
    from pyspark.sql import types as T

    return T.StructType(
        [T.StructField(name, getattr(T, _SPARK_TYPES[kind])(), nullable) for name, kind, nullable, _ in TRANSACTION_EVENT]
    )
