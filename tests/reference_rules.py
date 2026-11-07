"""Implementación de REFERENCIA de las 4 reglas de fraude, en Python puro.

Es la especificación ejecutable de lo que debe detectar `detect_fraud` (Spark): sirve para
comprobar que los ataques del productor de verdad superan los umbrales, y para que quien
implemente las reglas en Spark tenga contra qué comparar.

Cada función devuelve el conjunto de `transaction_id` que DISPARAN la regla: la transacción
en la que, por primera vez, se cumple la condición para su ventana.
"""
import os
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "common"))
import reglas  # noqa: E402


def parse_ts(iso):
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp()


def _by_user(events):
    users = defaultdict(list)
    for e in events:
        users[e["user_id"]].append((parse_ts(e["event_ts"]), e))
    for rows in users.values():
        rows.sort(key=lambda r: r[0])
    return users


def rafaga(events):
    cfg, out = reglas.RAFAGA, set()
    window = cfg["window_minutes"] * 60
    for rows in _by_user(events).values():
        q = deque()
        for ts, e in rows:
            q.append(ts)
            while ts - q[0] > window:
                q.popleft()
            if len(q) > cfg["max_tx"]:
                out.add(e["transaction_id"])
    return out


def monto_acumulado(events):
    cfg, out = reglas.MONTO_ACUMULADO, set()
    window = cfg["window_minutes"] * 60
    for rows in _by_user(events).values():
        q, total = deque(), 0
        for ts, e in rows:
            q.append((ts, e["amount_cop"]))
            total += e["amount_cop"]
            while ts - q[0][0] > window:
                total -= q.popleft()[1]
            if total > cfg["max_amount_cop"]:
                out.add(e["transaction_id"])
    return out


def monto_atipico(events, avg_by_user):
    factor, out = reglas.MONTO_ATIPICO["factor"], set()
    for e in events:
        if e["amount_cop"] > factor * avg_by_user[e["user_id"]]:
            out.add(e["transaction_id"])
    return out


def viaje_imposible(events):
    window, out = reglas.VIAJE_IMPOSIBLE["window_minutes"] * 60, set()
    for rows in _by_user(events).values():
        for (t1, e1), (t2, e2) in zip(rows, rows[1:]):
            if e1["city_code"] != e2["city_code"] and t2 - t1 < window:
                out.add(e2["transaction_id"])
    return out


def all_rules(events, avg_by_user):
    return {
        "rafaga": rafaga(events),
        "monto_acumulado": monto_acumulado(events),
        "monto_atipico": monto_atipico(events, avg_by_user),
        "viaje_imposible": viaje_imposible(events),
    }
