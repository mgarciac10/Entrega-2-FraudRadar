"""Pruebas de fraud_rules.py (la lógica de las 4 reglas dentro de detect_fraud), sin Spark.

La comparación clave es contra tests/reference_rules.py, la especificación ejecutable: con los mismos
eventos, las dos implementaciones deben disparar en las mismas transacciones.
"""
import json
import os
import random
import sys
import unittest
from collections import defaultdict
from datetime import datetime, timedelta, timezone

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "spark", "common"))
sys.path.insert(0, os.path.join(ROOT, "spark", "streaming"))
sys.path.insert(0, os.path.dirname(__file__))

import fraud_rules as fr  # noqa: E402
import reference_rules as ref  # noqa: E402

T0 = datetime(2026, 11, 8, 12, 0, 0, tzinfo=timezone.utc)
CITIES = ("MDE", "BOG", "CLO")


def ms(dt):
    return int(dt.timestamp() * 1000)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def ev(i, user, seconds, amount=50_000, city="MDE", avg=None):
    """Transacción en el instante T0 + seconds."""
    dt = T0 + timedelta(seconds=seconds)
    return {"transaction_id": f"tx-{i}", "user_id": user, "event_ts": iso(dt), "ts_ms": ms(dt),
            "amount_cop": amount, "city_code": city, "avg_amount_30d": avg}


def run(events, chunk=1):
    """Corre los eventos por lotes de `chunk`, pasando la libreta por JSON entre lotes (como Spark)."""
    by_user = defaultdict(list)
    for e in events:
        by_user[e["user_id"]].append(e)
    fires = []
    for user, rows in by_user.items():
        state = fr.new_state()
        for i in range(0, len(rows), chunk):
            state = json.loads(json.dumps(state))
            state, out = fr.evaluate(state, rows[i:i + chunk])
            fires += [dict(f, user_id=user) for f in out]
    return fires


def fired(fires, rule):
    return {f["trigger_tx_id"] for f in fires if f["rule"] == rule}


class AgainstReference(unittest.TestCase):
    def check(self, events, chunk):
        avg = {e["user_id"]: e["avg_amount_30d"] for e in events}
        expected = ref.all_rules(events, avg)
        got = run(events, chunk)
        for rule in expected:
            self.assertEqual(fired(got, rule), expected[rule], f"{rule} (lote de {chunk})")

    def test_random_traffic_matches_reference(self):
        for seed in range(30):
            rnd = random.Random(seed)
            events, n = [], 0
            for u in range(6):
                avg = rnd.choice([20_000, 80_000, 300_000])
                t = 0
                for _ in range(rnd.randint(5, 60)):
                    t += rnd.choice([1, 5, 20, 45, 90, 200, 400, 700])
                    amount = rnd.choice([5_000, 30_000, 120_000, 400_000, 1_500_000])
                    events.append(ev(n, f"u{u}", t, amount, rnd.choice(CITIES), avg))
                    n += 1
            for chunk in (1, 3, 1000):  # evento por evento, lotes pequeños y todo de una vez
                self.check(events, chunk)


class Episodes(unittest.TestCase):
    def test_burst_is_one_alert(self):
        events = [ev(i, "u1", i * 10) for i in range(12)]  # 12 tx en 110 s
        fires = [f for f in run(events) if f["rule"] == "rafaga"]
        self.assertEqual(len(fires), 7)  # dispara desde la 6.ª transacción
        self.assertEqual(sum(f["first_of_episode"] for f in fires), 1)
        self.assertEqual(len({f["window_start_ms"] for f in fires}), 1)
        first = next(f for f in fires if f["first_of_episode"])
        self.assertEqual(first["trigger_tx_id"], "tx-5")
        self.assertEqual(first["transaction_ids"], [f"tx-{i}" for i in range(6)])
        self.assertEqual(first["window_start_ms"], ms(T0))
        self.assertEqual(len({fr.alert_id("u1", f["rule"], f["window_start_ms"]) for f in fires}), 1)

    def test_new_burst_after_a_calm_period_is_a_new_alert(self):
        events = [ev(i, "u1", i * 10) for i in range(7)] + [ev(100 + i, "u1", 3000 + i * 10) for i in range(7)]
        fires = [f for f in run(events) if f["rule"] == "rafaga" and f["first_of_episode"]]
        self.assertEqual(len(fires), 2)
        self.assertNotEqual(fires[0]["window_start_ms"], fires[1]["window_start_ms"])

    def test_threshold_is_strict(self):
        self.assertEqual(fired(run([ev(i, "u1", i * 10) for i in range(5)]), "rafaga"), set())  # 5 tx: no
        self.assertEqual(len(fired(run([ev(i, "u1", i * 10) for i in range(6)]), "rafaga")), 1)  # 6 tx: sí

    def test_monto_acumulado(self):
        events = [ev(0, "u1", 0, 900_000), ev(1, "u1", 60, 900_000), ev(2, "u1", 120, 300_000)]  # 2.100.000 en 2 min
        f = [x for x in run(events) if x["rule"] == "monto_acumulado"]
        self.assertEqual([x["trigger_tx_id"] for x in f], ["tx-2"])
        self.assertEqual(f[0]["amount_at_risk_cop"], 2_100_000)
        # fuera de la ventana de 10 min no suma
        self.assertEqual(fired(run([ev(0, "u1", 0, 1_500_000), ev(1, "u1", 601, 1_500_000)]), "monto_acumulado"), set())

    def test_monto_atipico_needs_profile(self):
        self.assertEqual(fired(run([ev(0, "u1", 0, 600_000, avg=100_000)]), "monto_atipico"), {"tx-0"})
        self.assertEqual(fired(run([ev(0, "u1", 0, 500_000, avg=100_000)]), "monto_atipico"), set())  # = 5x: no
        self.assertEqual(fired(run([ev(0, "u1", 0, 9_000_000, avg=None)]), "monto_atipico"), set())  # sin perfil
        self.assertEqual(fired(run([ev(0, "u1", 0, 9_000_000, avg=float("nan"))]), "monto_atipico"), set())

    def test_viaje_imposible(self):
        f = [x for x in run([ev(0, "u1", 0, city="MDE"), ev(1, "u1", 600, city="BOG")]) if x["rule"] == "viaje_imposible"]
        self.assertEqual(len(f), 1)
        self.assertEqual(f[0]["transaction_ids"], ["tx-0", "tx-1"])
        self.assertEqual(f[0]["window_start_ms"], ms(T0))
        self.assertEqual(f[0]["evidence"]["from_city"], "MDE")
        self.assertEqual(fired(run([ev(0, "u1", 0, city="MDE"), ev(1, "u1", 1800, city="BOG")]), "viaje_imposible"), set())

    def test_out_of_order_within_a_batch_is_sorted(self):
        events = [ev(i, "u1", i * 10) for i in range(8)]
        shuffled = events[:]
        random.Random(1).shuffle(shuffled)
        state, out = fr.evaluate(fr.new_state(), shuffled)
        _, expected = fr.evaluate(fr.new_state(), events)
        self.assertEqual([f["trigger_tx_id"] for f in out], [f["trigger_tx_id"] for f in expected])

    def test_state_is_pruned(self):
        events = [ev(i, "u1", i * 600) for i in range(20)]  # cada 10 min durante ~3 h
        state = fr.new_state()
        state, _ = fr.evaluate(state, events)
        self.assertLessEqual(len(state["events"]), 4)  # solo la última media hora

    def test_alert_id_is_deterministic(self):
        self.assertEqual(fr.alert_id("u1", "rafaga", 1000), fr.alert_id("u1", "rafaga", 1000))
        self.assertNotEqual(fr.alert_id("u1", "rafaga", 1000), fr.alert_id("u1", "rafaga", 1001))
        self.assertNotEqual(fr.alert_id("u1", "rafaga", 1000), fr.alert_id("u1", "monto_acumulado", 1000))
        self.assertEqual(fr.iso_utc(ms(T0)), "2026-11-08T12:00:00.000Z")


if __name__ == "__main__":
    unittest.main()
