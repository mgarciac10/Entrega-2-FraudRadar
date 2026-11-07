"""Pruebas del generador de eventos y del simulador del productor (sin Kafka ni reloj real)."""
import os
import re
import sys
import tempfile
import unittest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "producer"))
sys.path.insert(0, os.path.join(ROOT, "spark", "common"))
sys.path.insert(0, os.path.dirname(__file__))

import events  # noqa: E402
import generate_catalogs  # noqa: E402
import reference_rules as ref  # noqa: E402
import schemas  # noqa: E402
import simulator  # noqa: E402
import world as world_mod  # noqa: E402

ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
PY_TYPES = {"string": str, "long": int, "double": float, "boolean": bool}
T0 = 1_794_000_000.0  # un instante cualquiera de noviembre de 2026


def make_world(users=30_000):
    tmp = tempfile.TemporaryDirectory()
    generate_catalogs.generate(users, 500, 11, tmp.name)
    w = world_mod.load_world(tmp.name)
    tmp.cleanup()
    return w


def run_simulation(world, rate, fraud_ratio, seconds, seed=3, step=0.1):
    sim = simulator.Simulator(events.EventGenerator(world, seed=seed), rate, fraud_ratio, seed=seed)
    out, now = [], T0
    for _ in range(int(seconds / step)):
        out.extend(sim.step(now))
        now += step
    return sim, out


class ProductorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.world = make_world()
        cls.avg = {u.user_id: u.avg_amount_30d for u in cls.world.users}
        cls.users = {u.user_id: u for u in cls.world.users}

    # ---- contrato ---------------------------------------------------------
    def check_contract(self, e):
        self.assertEqual(set(e), set(schemas.FIELD_NAMES))
        for name, kind, nullable, _ in schemas.TRANSACTION_EVENT:
            v = e[name]
            if v is None:
                self.assertTrue(nullable, f"{name} no admite nulo")
            elif kind == "timestamp":
                self.assertRegex(v, ISO_RE)
            else:
                self.assertIs(type(v), PY_TYPES[kind], f"{name}={v!r}")
        self.assertIn(e["tx_type"], schemas.TX_TYPES)
        self.assertIn(e["channel"], schemas.CHANNELS)
        self.assertIn(e["city_code"], self.world.cities)
        self.assertGreaterEqual(e["amount_cop"], 1000)

    def test_evento_normal_cumple_el_contrato_y_la_logica_de_negocio(self):
        gen = events.EventGenerator(self.world, seed=1)
        merchants = {m.merchant_id: m for m in self.world.merchants}
        seen = set()
        for i in range(3000):
            e = gen.normal_event(T0 + i)
            self.check_contract(e)
            seen.add(e["tx_type"])
            self.assertFalse(e["is_fraud_injected"])
            self.assertIsNone(e["attack_id"])
            self.assertIsNone(e["attack_pattern"])
            user = self.users[e["user_id"]]
            self.assertEqual(e["city_code"], user.home_city)  # los normales no viajan
            if e["tx_type"] == "PAGO":
                self.assertIn(e["channel"], ("app", "qr"))
                self.assertEqual(merchants[e["merchant_id"]].city_code, user.home_city)
                self.assertIsNone(e["counterparty_user_id"])
            elif e["tx_type"] == "P2P":
                self.assertEqual(e["channel"], "app")
                self.assertIn(e["counterparty_user_id"], self.users)
                self.assertNotEqual(e["counterparty_user_id"], e["user_id"])
                self.assertIsNone(e["merchant_id"])
            else:
                self.assertEqual(e["channel"], "corresponsal")
                self.assertIsNone(e["merchant_id"])
                self.assertIsNone(e["counterparty_user_id"])
        self.assertEqual(seen, set(schemas.TX_TYPES))

    def test_determinista(self):
        a, b = events.EventGenerator(self.world, seed=5), events.EventGenerator(self.world, seed=5)
        for i in range(300):
            self.assertEqual(a.normal_event(T0 + i), b.normal_event(T0 + i))
        for p in schemas.ATTACK_PATTERNS:
            self.assertEqual(a.attack_episode(p, T0), b.attack_episode(p, T0))

    # ---- ataques ----------------------------------------------------------
    def episodes(self, pattern, n=150):
        gen = events.EventGenerator(self.world, seed=21)
        for _ in range(n):
            user = gen.rng.choice(self.world.users)
            yield user, gen.attack_episode(pattern, T0, user=user)

    def test_cada_ataque_supera_el_umbral_de_su_regla(self):
        rule = {"rafaga": lambda ev, u: ref.rafaga(ev),
                "monto_acumulado": lambda ev, u: ref.monto_acumulado(ev),
                "monto_atipico": lambda ev, u: ref.monto_atipico(ev, {u.user_id: u.avg_amount_30d}),
                "viaje_imposible": lambda ev, u: ref.viaje_imposible(ev)}
        for pattern in schemas.ATTACK_PATTERNS:
            for user, ep in self.episodes(pattern):
                evs = [e for _, e in ep]
                fraud_ids = {e["transaction_id"] for e in evs if e["is_fraud_injected"]}
                self.assertTrue(fraud_ids, pattern)
                triggers = rule[pattern](evs, user)
                self.assertTrue(triggers & fraud_ids, f"{pattern}: ninguna transacción fraudulenta dispara la regla")

    def test_etiquetas_y_contrato_de_los_ataques(self):
        for pattern in schemas.ATTACK_PATTERNS:
            for user, ep in self.episodes(pattern, n=60):
                ids = {e["transaction_id"] for _, e in ep}
                self.assertEqual(len(ids), len(ep), "transaction_id repetido")
                attack_ids = set()
                for ts, e in ep:
                    self.check_contract(e)
                    self.assertEqual(e["user_id"], user.user_id)
                    if e["is_fraud_injected"]:
                        self.assertEqual(e["attack_pattern"], pattern)
                        attack_ids.add(e["attack_id"])
                    else:
                        self.assertIsNone(e["attack_id"])
                        self.assertIsNone(e["attack_pattern"])
                self.assertEqual(len(attack_ids), 1, "un ataque debe tener un solo attack_id")
                # el orden de la lista y de los instantes coincide
                times = [t for t, _ in ep]
                self.assertEqual(times, sorted(times))
                self.assertLess(times[-1] - times[0], 25 * 60)

    def test_solo_el_viaje_imposible_incluye_una_transaccion_legitima_previa(self):
        for pattern in schemas.ATTACK_PATTERNS:
            for _, ep in self.episodes(pattern, n=40):
                legit = [e for _, e in ep if not e["is_fraud_injected"]]
                self.assertEqual(len(legit), 1 if pattern == "viaje_imposible" else 0, pattern)

    def test_el_viaje_es_a_una_ciudad_lejana(self):
        for user, ep in self.episodes("viaje_imposible", n=60):
            home = self.world.cities[user.home_city]
            for _, e in ep:
                if e["is_fraud_injected"]:
                    c = self.world.cities[e["city_code"]]
                    self.assertGreater(world_mod.haversine_km(home.lat, home.lon, c.lat, c.lon), events.FAR_CITY_KM)

    def test_hay_fraude_evitable_despues_del_disparo(self):
        # Q8 necesita transacciones fraudulentas posteriores a la que dispara la regla.
        rules = {"rafaga": ref.rafaga, "monto_acumulado": ref.monto_acumulado, "viaje_imposible": ref.viaje_imposible}
        for pattern, fn in rules.items():
            con_evitable = 0
            for _, ep in self.episodes(pattern, n=100):
                evs = [e for _, e in ep]
                trig = [e for e in evs if e["transaction_id"] in fn(evs)]
                t_first = min(ref.parse_ts(e["event_ts"]) for e in trig)
                con_evitable += any(e["is_fraud_injected"] and ref.parse_ts(e["event_ts"]) > t_first for e in evs)
            self.assertGreater(con_evitable, 30, f"{pattern}: pocos ataques con fraude evitable ({con_evitable}/100)")

    # ---- simulador --------------------------------------------------------
    def test_tasa_y_proporcion_de_fraude(self):
        sim, out = run_simulation(self.world, rate=50, fraud_ratio=0.02, seconds=900)
        self.assertAlmostEqual(len(out) / (50 * 900), 1.0, delta=0.01)
        frac = sum(e["is_fraud_injected"] for e in out) / len(out)
        self.assertAlmostEqual(frac, 0.02, delta=0.006)
        self.assertEqual(len({e["transaction_id"] for e in out}), len(out))
        self.assertGreater(sim.attacks_started, 20)

    def test_no_emite_eventos_del_futuro(self):
        sim = simulator.Simulator(events.EventGenerator(self.world, seed=2), 50, 0.05, seed=2)
        now = T0
        for _ in range(3000):
            for e in sim.step(now):
                self.assertLessEqual(ref.parse_ts(e["event_ts"]), now + 0.001)
            now += 0.1

    def test_sin_fraude(self):
        _, out = run_simulation(self.world, rate=50, fraud_ratio=0.0, seconds=60)
        self.assertFalse(any(e["is_fraud_injected"] for e in out))

    def test_el_trafico_normal_casi_no_dispara_las_reglas(self):
        # usuarios (30k) >= tasa (50) x 600: la regla práctica de generate_catalogs.py
        _, out = run_simulation(self.world, rate=50, fraud_ratio=0.0, seconds=600)
        r = ref.all_rules(out, self.avg)
        self.assertEqual(len(r["monto_acumulado"]), 0)
        self.assertEqual(len(r["viaje_imposible"]), 0)
        self.assertLessEqual(len(r["rafaga"]), 3)
        self.assertLess(len(r["monto_atipico"]) / len(out), 0.005)

    def test_un_universo_chico_provoca_rafagas_por_azar(self):
        # La advertencia de generate_catalogs.py, comprobada: 500 usuarios a 50 ev/s.
        small = make_world(users=500)
        _, out = run_simulation(small, rate=50, fraud_ratio=0.0, seconds=300)
        self.assertGreater(len(ref.rafaga(out)), 1000)


if __name__ == "__main__":
    unittest.main()
