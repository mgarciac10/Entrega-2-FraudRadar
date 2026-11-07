# producer

**Responsable:** Mateo García · **Par:** Juan José Vargas

Simula el core de PagaYa.

| Archivo | Qué hace |
|---|---|
| `world.py` | Ciudades, usuarios y comercios simulados; carga de los catálogos |
| `generate_catalogs.py` | Genera los CSV de ciudades, usuarios, comercios y perfil inicial (export diario del core). Se suben a `S3 landing/` |
| `producer.py` | Publica transacciones JSON en el topic `transactions` con `user_id` como llave. Tasa configurable (10 a 1.000 ev/s). Inyecta fraude etiquetado (ráfaga, monto atípico, viaje imposible) en la proporción de `PRODUCER_FRAUD_RATIO` |

Reglas de implementación:

- El productor toma usuarios y comercios de los mismos catálogos, para que los joins de Silver no fallen.
- Productor idempotente: `enable.idempotence=true`, `acks=all`.
- Cada evento lleva `is_fraud_injected` (true si el productor lo generó como fraude) y `attack_id` (identificador del ataque al que pertenece; nulo si no es fraude). Las reglas no los usan; Gold los usa para medir precisión, recall y fraude evitable (Q8, Q9).

## Catálogos

```bash
python producer/generate_catalogs.py --users 100000 --merchants 1000 --seed 42 --out data/catalogs
aws s3 cp data/catalogs s3://<bucket>/landing/ --recursive --profile fraudradar
```

Es determinista (misma semilla, mismos archivos). Solo usa la biblioteca estándar de Python.

**El número de usuarios debe crecer con la tasa de eventos.** Con pocos usuarios y mucha tasa, cada usuario haría una transacción cada pocos segundos y todos dispararían la regla de ráfaga. Regla práctica: usuarios ≥ tasa (ev/s) × 600, es decir, una transacción por usuario cada ~10 minutos. 100.000 usuarios sostienen unos 150 ev/s sin ráfagas por azar.

El contrato del evento y de los catálogos está en `spark/common/README.md`.

