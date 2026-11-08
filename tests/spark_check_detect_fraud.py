"""Verifica detect_fraud con Spark Structured Streaming real, sin Kafka ni AWS. Corre DENTRO del contenedor.

La fuente son archivos de texto con un JSON por línea (igual que el `value` de un mensaje de Kafka),
entregados de a un archivo por micro-batch: así el estado de cada usuario tiene que sobrevivir entre
lotes. Compara lo que dispara contra tests/reference_rules.py (la especificación) y revisa la
deduplicación, los eventos inválidos, que una ráfaga sea UNA sola alerta y el formato del ítem de DynamoDB.
Lo lanza tests/run_spark_checks.sh.
"""
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/opt/fraudradar/tests")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))

import fraud_rules  # noqa: E402
import reference_rules as ref  # noqa: E402
from detect_fraud import alert_item, build_alerts, parse_events  # noqa: E402
from spark_session import get_spark  # noqa: E402

failures = []


def check(cond, msg):
    print(("OK    " if cond else "FALLA ") + msg)
    if not cond:
        failures.append(msg)


T0 = datetime(2026, 11, 8, 12, 0, 0, tzinfo=timezone.utc)
_n = 0


def event(user, seconds, amount=50_000, city="MDE", tx_id=None, **extra):
    global _n
    _n += 1
    dt = T0 + timedelta(seconds=seconds)
    e = {
        "transaction_id": tx_id or f"tx-{_n:04d}", "event_ts": dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "user_id": user,
        "tx_type": "P2P", "channel": "app", "amount_cop": amount, "merchant_id": None,
        "counterparty_user_id": "u-other", "city_code": city, "lat": 6.25, "lon": -75.56,
        "is_fraud_injected": False, "attack_id": None, "attack_pattern": None,
    }
    e.update(extra)
    return e


AVG = {"uA": 80_000.0, "uB": 80_000.0, "uC": 100_000.0, "uD": 80_000.0, "uE": 80_000.0, "uF": 80_000.0, "uG": 80_000.0}

events = []
events += [event("uA", i * 10) for i in range(9)]                                            # ráfaga: 9 tx en 80 s
events += [event("uB", 100, 900_000), event("uB", 160, 900_000), event("uB", 220, 400_000)]  # monto acumulado: 2,2 M en 2 min
events += [event("uC", 300, 900_000)]                                                        # atípico: 9x su promedio
events += [event("uD", 400, city="MDE"), event("uD", 1000, city="BOG")]                      # viaje imposible: 10 min
events += [event("uE", i * 400) for i in range(5)]                                           # normal: 5 tx espaciadas
events += [event("uF", i * 10, tx_id=f"f-{i}") for i in range(5)]                            # 5 tx únicas...
duplicates = [event("uF", 20, tx_id="f-2")]                                                  # ...y un reenvío: no debe contar como 6.ª
events_invalid = [event("uG", i * 10) for i in range(5)] + [event("uG", 55, tx_type="CHEQUE")]  # 5 válidas + 1 inválida

ordered = sorted(events + duplicates + events_invalid, key=lambda e: e["event_ts"])
lines = [json.dumps(e) for e in ordered] + ["{esto no es json"]
# 4 archivos = 4 micro-batches, en orden de tiempo: las ventanas de un mismo usuario quedan
# repartidas entre lotes, así que su libreta tiene que sobrevivir de un lote al siguiente
step = -(-len(lines) // 4)
files = [lines[i:i + step] for i in range(0, len(lines), step)]

tmp = tempfile.mkdtemp(prefix="detect_fraud_")
for i, content in enumerate(files):
    path = os.path.join(tmp, f"batch_{i:02d}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(content) + "\n")
    os.utime(path, (time.time() + i, time.time() + i))  # el orden de llegada es el de los archivos

spark = get_spark("check_detect_fraud")
spark.sparkContext.setLogLevel("ERROR")
profile = spark.createDataFrame(list(AVG.items()), "user_id string, avg_amount_30d double")

raw = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(tmp)
alerts = build_alerts(parse_events(raw), profile)
query = alerts.writeStream.outputMode("update").format("memory").queryName("alerts").start()
query.processAllAvailable()
query.stop()
rows = [r.asDict() for r in spark.table("alerts").collect()]
by = {}
for r in rows:
    by.setdefault((r["user_id"], r["rule"]), []).append(r)

# Especificación: lo que dispara reference_rules sobre los eventos válidos y sin duplicar
valid = [e for e in ordered if e["tx_type"] != "CHEQUE" and e is not duplicates[0]]
expected = ref.all_rules(valid, AVG)
expected_pairs = {(e["user_id"], rule) for rule, ids in expected.items() for e in valid if e["transaction_id"] in ids}
check(set(by) == expected_pairs, f"mismas parejas (usuario, regla) que la referencia: {sorted(by)}")

check(len(by.get(("uA", "rafaga"), [])) == 1, "una ráfaga de 9 transacciones es UNA alerta, no varias")
a = by[("uA", "rafaga")][0]
check(a["trigger_tx_id"] in expected["rafaga"] and len(a["transaction_ids"]) == 6,
      f"la ráfaga se detecta en la 6.ª transacción ({len(a['transaction_ids'])} transacciones en la alerta)")
check(a["window_start_ms"] == int(T0.timestamp() * 1000), "window_start es la primera transacción de la ventana")
check(by[("uB", "monto_acumulado")][0]["amount_at_risk_cop"] == 2_200_000, "monto acumulado: 2.200.000 en riesgo")
c = by[("uC", "monto_atipico")][0]
check(json.loads(c["evidence"])["times_avg"] == 9.0 and c["transaction_ids"] == [c["trigger_tx_id"]], "monto atípico: 9 veces su promedio")
d = by[("uD", "viaje_imposible")][0]
check(len(d["transaction_ids"]) == 2 and json.loads(d["evidence"])["to_city"] == "BOG", "viaje imposible: MDE -> BOG en 10 min")
check(not any(u == "uE" for u, _ in by), "tráfico normal: sin alertas")
check(not any(u == "uF" for u, _ in by), "un reenvío con el mismo transaction_id no cuenta dos veces (5 únicas: sin ráfaga)")
check(not any(u == "uG" for u, _ in by), "el evento inválido no cuenta (5 válidas: sin ráfaga) y el JSON roto no tumba el job")
# Una alerta por episodio: la cuenta de Spark debe coincidir con la de la lógica pura y no repetir alert_id
n_episodes = 0
for user in AVG:
    mine = [{**e, "ts_ms": int(datetime.strptime(e["event_ts"], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc).timestamp() * 1000),
             "avg_amount_30d": AVG[user]} for e in valid if e["user_id"] == user]
    n_episodes += sum(f["first_of_episode"] for f in fraud_rules.evaluate(fraud_rules.new_state(), mine)[1])
ids = [fraud_rules.alert_id(r["user_id"], r["rule"], r["window_start_ms"]) for r in rows]
check(len(rows) == n_episodes and len(set(ids)) == len(ids), f"{len(rows)} alertas = {n_episodes} episodios, sin alert_id repetido")

# Formato del ítem que se escribe en DynamoDB
item = alert_item(a)
fixture_keys = {"alert_id", "user_id", "rule", "window_start", "status", "amount_at_risk_cop", "transaction_ids",
                "trigger_event_ts", "created_at", "evidence"}
check(set(item) == fixture_keys, "el ítem tiene los mismos atributos que infra/scripts/fixtures/alert_sample.json")
check(item["status"] == "abierta" and item["alert_id"] == fraud_rules.alert_id("uA", "rafaga", a["window_start_ms"]),
      "estado abierta y alert_id determinístico")
check(item["window_start"] == "2026-11-08T12:00:00.000Z", f"window_start en ISO-8601 UTC ({item['window_start']})")
check(not any(isinstance(v, float) for v in item["evidence"].values()), "la evidencia no trae float (boto3 los rechaza)")

spark.stop()
print("\nTodo bien." if not failures else f"\n{len(failures)} verificaciones fallidas.")
sys.exit(1 if failures else 0)
