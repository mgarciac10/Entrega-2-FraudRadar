# producer

**Responsable:** Mateo García · **Par:** Juan José Vargas

Simula el core de PagaYa.

| Archivo | Qué hace |
|---|---|
| `generate_catalogs.py` | Genera los CSV de usuarios, comercios y ciudades (export diario del core) y los sube a `S3 landing/` |
| `producer.py` | Publica transacciones JSON en el topic `transactions` con `user_id` como llave. Tasa configurable (10 a 1.000 ev/s). Inyecta fraude etiquetado (ráfaga, monto atípico, viaje imposible) en la proporción de `PRODUCER_FRAUD_RATIO` |

Reglas de implementación:

- El productor toma usuarios y comercios de los mismos catálogos, para que los joins de Silver no fallen.
- Productor idempotente: `enable.idempotence=true`, `acks=all`.
