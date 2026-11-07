# spark/common

Contratos compartidos entre integrantes. **Cambiarlos se avisa al equipo.**

| Archivo | Qué contiene |
|---|---|
| `schemas.py` | Contrato del evento de transacción (`TRANSACTION_EVENT`) y `transaction_struct()`, que arma el `StructType` de PySpark |
| `reglas.py` | Umbrales de las 4 reglas de fraude. Configuración, no lógica: se calibran con la tasa de falsos positivos (Q7) |

## Evento de transacción (topic `transactions`)

JSON, una transacción por mensaje, llave de Kafka = `user_id`.

| Campo | Tipo | Nulo | Descripción |
|---|---|---|---|
| `transaction_id` | string | no | UUID v4 |
| `event_ts` | timestamp | no | Cuándo ocurrió; ISO-8601 UTC con milisegundos |
| `user_id` | string | no | Titular de la cuenta |
| `tx_type` | string | no | `PAGO` · `P2P` · `RETIRO` |
| `channel` | string | no | `app` · `qr` · `corresponsal` |
| `amount_cop` | long | no | Monto en pesos |
| `merchant_id` | string | sí | Solo en `PAGO` |
| `counterparty_user_id` | string | sí | Solo en `P2P` |
| `city_code` | string | no | Ciudad (ver `ciudades.csv`) |
| `lat`, `lon` | double | no | Coordenadas del dispositivo o corresponsal |
| `is_fraud_injected` | boolean | no | Etiqueta del productor |
| `attack_id` | string | sí | Ataque al que pertenece; nulo si no es fraude |
| `attack_pattern` | string | sí | `rafaga` · `monto_acumulado` · `monto_atipico` · `viaje_imposible` |

Las tres últimas columnas son **etiquetas del productor**: `detect_fraud` no las lee. Gold las usa para medir precisión, recall por regla (Q9) y fraude evitable (Q8).

`attack_pattern` se agregó porque sin él no se puede calcular el recall **por regla**: hay que saber qué regla debía detectar cada ataque.

## Catálogos (S3 `landing/`)

Los genera `producer/generate_catalogs.py`; simulan el export diario del core.

| Archivo | Columnas |
|---|---|
| `ciudades.csv` | `city_code, city, department, lat, lon, population_weight` |
| `usuarios.csv` | `user_id, kyc_level, segment, home_city_code, registered_at` |
| `comercios.csv` | `merchant_id, name, category, city_code` |
| `perfil_inicial.csv` | `user_id, avg_amount_30d, n_tx_30d` |

`perfil_inicial.csv` es el **arranque en frío** de `silver.user_profile`: sin historial, la regla de monto atípico no podría disparar. `build_user_profile` lo reemplaza con datos reales conforme corre el pipeline.
