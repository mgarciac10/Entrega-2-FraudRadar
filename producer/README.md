# producer

**Responsable:** Mateo García · **Par:** Juan José Vargas

Simula el core de PagaYa.

| Archivo | Qué hace |
|---|---|
| `world.py` | Ciudades, usuarios y comercios simulados; carga de los catálogos |
| `generate_catalogs.py` | Genera los CSV de ciudades, usuarios, comercios y perfil inicial (export diario del core). Se suben a `S3 landing/` |
| `producer.py` | Publica transacciones JSON en el topic `transactions` con `user_id` como llave. Tasa configurable (10 a 1.000 ev/s). Inyecta fraude etiquetado (ráfaga, monto atípico, viaje imposible) en la proporción de `PRODUCER_FRAUD_RATIO` |

## Cómo funciona

- **`events.py`** (lógica pura): transacciones normales y los 4 patrones de ataque. Cada ataque fabrica transacciones que **superan el umbral de su regla** (los umbrales salen de `spark/common/reglas.py`).
- **`simulator.py`**: decide qué eventos salen en cada instante a la tasa pedida y reparte los ataques en el tiempo; un ataque dura de segundos a unos minutos.
- **`producer.py`**: el programa. Envía a Kafka (productor idempotente, `acks=all`, reintentos hasta 2 min, llave = `user_id`) o a la salida estándar (`--sink stdout`) para depurar.

### Transacciones normales

Un usuario al azar paga (70 %), transfiere (22 %) o retira (8 %) cerca de su monto típico, siempre desde su ciudad de residencia. **No se simulan viajes**, para que el viaje imposible solo aparezca en los ataques.

### Los 4 ataques

| Patrón | Qué fabrica | Transacciones que dispara |
|---|---|---|
| `rafaga` | 6 a 8 transferencias pequeñas a una misma cuenta en menos de 4 min | La 6.ª; las siguientes son fraude evitable |
| `monto_acumulado` | 3 a 5 transferencias que suman entre $2,2 M y $3,5 M en menos de 8 min | La que cruza los $2 M |
| `monto_atipico` | Una compra de 6 a 10 veces el promedio del usuario | Ella misma (no hay fraude evitable) |
| `viaje_imposible` | Una compra legítima del titular y, 1 a 6 min después, 2 a 3 compras desde una ciudad a más de 400 km | La primera desde la ciudad lejana |

Los ataques se solapan a propósito: una transferencia de $800.000 es también atípica para casi cualquier usuario. Por eso al medir una regla se usa `attack_pattern` (qué debía detectar cada ataque) para el recall y se cuenta como acierto cualquier alerta sobre una transacción fraudulenta para la precisión.

`--fraud-ratio` es la fracción de **eventos** (no de ataques) que son fraude. La ráfaga domina el conteo de eventos porque cada una tiene 6 a 8.

### Tráfico normal y falsos positivos

Medido con 50 ev/s durante 10 minutos (30.000 eventos, sin ataques): 0 disparos de monto acumulado y de viaje imposible, 2 de ráfaga por azar y 55 de monto atípico (0,18 %). Esa cola natural de montos altos es lo que produce falsos positivos reales para la pregunta 7.

## Ejecutar

**Sin Kafka (depurar):**

```bash
python producer/generate_catalogs.py --users 20000 --merchants 500 --out data/catalogs
python producer/producer.py --catalogs data/catalogs --rate 20 --sink stdout --duration 10
```

Solo usa la biblioteca estándar. Para Kafka necesita `confluent-kafka` (`pip install -r producer/requirements.txt`).

**Desarrollo local con Kafka propio (sin AWS):**

```bash
python producer/generate_catalogs.py --users 100000 --out data/catalogs
docker compose --profile local --profile producer up --build
```

Levanta Kafka en modo KRaft, crea el topic `transactions` (6 particiones) y arranca el productor. Para ver los mensajes:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic transactions --from-beginning --max-messages 5
```

**En AWS (contra MSK):** en la EC2 de la aplicación, con el repositorio clonado y el `.env` con `KAFKA_BOOTSTRAP_SERVERS` (los brokers TLS de MSK) y `KAFKA_SECURITY_PROTOCOL=SSL`. El topic hay que crearlo antes (MSK tiene `auto.create.topics.enable=false`). Se opera con `bash infra/scripts/producer_aws.sh push|topic|start|check|stop` (ver `infra/README.md`). **Probado** contra MSK real: 20 ev/s estables, 0 errores, mensajes repartidos entre las 6 particiones.

## Tasa y correo

Cada alerta es un correo, así que la tasa importa. A 50 ev/s con 2 % de fraude, la referencia en Python da unas 20 alertas por minuto (un tercio son falsos positivos naturales). Para la demo de alertas, 10 a 20 ev/s. Para las pruebas de carga (200 y 1.000 ev/s) hay que **pausar la suscripción de correo de SNS**, o llegarán miles de correos.

## Reglas de implementación

- El productor toma usuarios y comercios de los mismos catálogos, así los joins de Silver no fallan.
- Productor idempotente: `enable.idempotence=true`, `acks=all`.
- Cada evento lleva `is_fraud_injected`, `attack_id` y `attack_pattern`. Las reglas no los usan; Gold los usa para medir precisión, recall y fraude evitable (Q8, Q9).
- Con una semilla fija (`--seed`) la corrida es reproducible.

## Catálogos

```bash
python producer/generate_catalogs.py --users 100000 --merchants 1000 --seed 42 --out data/catalogs
aws s3 cp data/catalogs s3://<bucket>/landing/ --recursive --profile fraudradar
```

Es determinista (misma semilla, mismos archivos). Solo usa la biblioteca estándar de Python.

**El número de usuarios debe crecer con la tasa de eventos.** Con pocos usuarios y mucha tasa, cada usuario haría una transacción cada pocos segundos y todos dispararían la regla de ráfaga. Regla práctica: usuarios ≥ tasa (ev/s) × 600, es decir, una transacción por usuario cada ~10 minutos. 100.000 usuarios sostienen unos 150 ev/s sin ráfagas por azar.

El contrato del evento y de los catálogos está en `spark/common/README.md`.

