"""Generación de transacciones normales y de ataques de fraude.

Lógica pura (sin Kafka ni reloj), para poder probarla de forma determinista.

Transacciones normales: un usuario elegido al azar paga, transfiere o retira cerca de su
monto típico y siempre desde su ciudad de residencia (no se simulan viajes: así el viaje
imposible solo aparece en los ataques).

Ataques: cada patrón fabrica transacciones que SUPERAN el umbral de su regla (los umbrales
salen de spark/common/reglas.py). Un ataque tiene varias transacciones repartidas en el tiempo;
las posteriores a la que dispara la regla son el fraude "evitable" que mide Q8.
"""
import os
import random
import sys
import uuid
from datetime import datetime, timezone

from world import CATEGORIAS, haversine_km

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "spark", "common"))
import reglas  # noqa: E402

TX_WEIGHTS = (("PAGO", 0.70), ("P2P", 0.22), ("RETIRO", 0.08))
TYPE_MULTIPLIER = {"P2P": 1.0, "RETIRO": 2.5}
SIGMA = 0.35  # dispersión del monto de un usuario alrededor de su monto típico
JITTER_DEG = 0.04  # ~4 km: el dispositivo no está exactamente en el centro de la ciudad
FAR_CITY_KM = 400  # ciudad "lejana" para el viaje imposible
DEFAULT_PATTERN_WEIGHTS = {"rafaga": 0.35, "monto_acumulado": 0.20, "monto_atipico": 0.25, "viaje_imposible": 0.20}


def iso(ts):
    """Epoch en segundos -> ISO-8601 UTC con milisegundos (el formato del contrato)."""
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{d.microsecond // 1000:03d}Z"


class EventGenerator:
    def __init__(self, world, seed=1):
        self.world = world
        self.rng = random.Random(seed)
        self._city_codes = list(world.cities)
        self._merchants_by_city = {c: world.merchants_in(c) for c in self._city_codes}
        self._ecommerce = [m for m in world.merchants if m.category == "ECOMMERCE"] or list(world.merchants)
        self._cat_mult = CATEGORIAS

    # ---- piezas -----------------------------------------------------------
    def _tx_id(self):
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _loc(self, city_code):
        c = self.world.cities[city_code]
        return (round(c.lat + self.rng.uniform(-JITTER_DEG, JITTER_DEG), 5),
                round(c.lon + self.rng.uniform(-JITTER_DEG, JITTER_DEG), 5))

    def _event(self, ts, user, tx_type, channel, amount, city_code, merchant=None, counterparty=None, attack=None):
        lat, lon = self._loc(city_code)
        return {
            "transaction_id": self._tx_id(),
            "event_ts": iso(ts),
            "user_id": user.user_id,
            "tx_type": tx_type,
            "channel": channel,
            "amount_cop": int(amount),
            "merchant_id": merchant.merchant_id if merchant else None,
            "counterparty_user_id": counterparty.user_id if counterparty else None,
            "city_code": city_code,
            "lat": lat,
            "lon": lon,
            "is_fraud_injected": attack is not None,
            "attack_id": attack[0] if attack else None,
            "attack_pattern": attack[1] if attack else None,
        }

    def _other_user(self, user):
        while True:
            other = self.rng.choice(self.world.users)
            if other.user_id != user.user_id:
                return other

    def _amount(self, base, sigma=SIGMA, digits=-2):
        return max(1000, round(base * self.rng.lognormvariate(0, sigma), digits))

    # ---- transacciones normales -------------------------------------------
    def normal_event(self, ts, user=None):
        rng = self.rng
        user = user or rng.choice(self.world.users)
        tx_type = rng.choices([t for t, _ in TX_WEIGHTS], [w for _, w in TX_WEIGHTS])[0]
        city = user.home_city
        if tx_type == "PAGO":
            merchant = rng.choice(self._merchants_by_city[city])
            amount = self._amount(user.avg_amount_30d * self._cat_mult[merchant.category])
            channel = "qr" if rng.random() < 0.4 else "app"
            return self._event(ts, user, "PAGO", channel, amount, city, merchant=merchant)
        if tx_type == "P2P":
            amount = self._amount(user.avg_amount_30d * TYPE_MULTIPLIER["P2P"])
            return self._event(ts, user, "P2P", "app", amount, city, counterparty=self._other_user(user))
        amount = self._amount(user.avg_amount_30d * TYPE_MULTIPLIER["RETIRO"], digits=-3)
        return self._event(ts, user, "RETIRO", "corresponsal", amount, city)

    # ---- ataques ----------------------------------------------------------
    def attack_episode(self, pattern, start_ts, user=None):
        """Devuelve [(ts, evento)] ordenado por tiempo. Las etiquetas marcan solo el fraude."""
        user = user or self.rng.choice(self.world.users)
        attack = (f"atk-{self.rng.getrandbits(40):010x}", pattern)
        build = {"rafaga": self._rafaga, "monto_acumulado": self._monto_acumulado,
                 "monto_atipico": self._monto_atipico, "viaje_imposible": self._viaje_imposible}[pattern]
        return sorted(build(user, start_ts, attack), key=lambda pair: pair[0])

    def _offsets(self, n, span):
        """n instantes en [0, span], el primero en 0."""
        return [0.0] + sorted(self.rng.uniform(0, span) for _ in range(n - 1))

    def _rafaga(self, user, t0, attack):
        r = reglas.RAFAGA
        n = self.rng.randint(r["max_tx"] + 1, r["max_tx"] + 3)
        mule = self._other_user(user)
        span = r["window_minutes"] * 60 * 0.8  # todas caben holgadas en la ventana
        out = []
        for off in self._offsets(n, span):
            amount = max(1000, round(user.avg_amount_30d * self.rng.uniform(0.4, 1.2), -2))
            out.append((t0 + off, self._event(t0 + off, user, "P2P", "app", amount, user.home_city,
                                              counterparty=mule, attack=attack)))
        return out

    def _monto_acumulado(self, user, t0, attack):
        r = reglas.MONTO_ACUMULADO
        n = self.rng.randint(3, max(3, reglas.RAFAGA["max_tx"]))  # sin llegar a ser ráfaga
        total = r["max_amount_cop"] * self.rng.uniform(1.1, 1.75)
        weights = [self.rng.uniform(0.7, 1.3) for _ in range(n)]
        amounts = [int(round(total * w / sum(weights), -2)) for w in weights]
        mule = self._other_user(user)
        span = r["window_minutes"] * 60 * 0.8
        out = []
        for off, amount in zip(self._offsets(n, span), amounts):
            out.append((t0 + off, self._event(t0 + off, user, "P2P", "app", amount, user.home_city,
                                              counterparty=mule, attack=attack)))
        return out

    def _monto_atipico(self, user, t0, attack):
        r = reglas.MONTO_ATIPICO
        top = reglas.MONTO_ACUMULADO["max_amount_cop"] * 0.95  # no confundirlo con el monto acumulado
        amount = round(min(user.avg_amount_30d * self.rng.uniform(r["factor"] * 1.2, r["factor"] * 2), top), -2)
        merchant = self.rng.choice(self._ecommerce)
        return [(t0, self._event(t0, user, "PAGO", "app", amount, user.home_city, merchant=merchant, attack=attack))]

    def _viaje_imposible(self, user, t0, attack):
        home = self.world.cities[user.home_city]
        far = [c for c in self._city_codes
               if haversine_km(home.lat, home.lon, self.world.cities[c].lat, self.world.cities[c].lon) > FAR_CITY_KM]
        far_city = self.rng.choice(far)
        # 1) la transacción legítima del titular, en su ciudad (no es fraude)
        out = [(t0, self.normal_event(t0, user=user))]
        out[0][1]["city_code"] = user.home_city
        # 2) el atacante opera desde una ciudad lejana, minutos después (la que dispara la regla)
        t = t0 + self.rng.uniform(60, 360)
        merchant = self.rng.choice(self._ecommerce)
        for k in range(self.rng.randint(2, 3)):
            amount = round(user.avg_amount_30d * self.rng.uniform(1.5, 3.0), -2)
            out.append((t, self._event(t, user, "PAGO", "app", amount, far_city, merchant=merchant, attack=attack)))
            t += self.rng.uniform(30, 120)
        return out

    # ---- calibración ------------------------------------------------------
    def mean_fraud_events(self, pattern_weights=None, samples=600):
        """Transacciones fraudulentas por ataque, en promedio (sin alterar el generador principal)."""
        weights = pattern_weights or DEFAULT_PATTERN_WEIGHTS
        probe = EventGenerator(self.world, seed=987654321)
        patterns, w = list(weights), list(weights.values())
        total = 0
        for _ in range(samples):
            p = probe.rng.choices(patterns, w)[0]
            total += sum(1 for _, e in probe.attack_episode(p, 0.0) if e["is_fraud_injected"])
        return total / samples
