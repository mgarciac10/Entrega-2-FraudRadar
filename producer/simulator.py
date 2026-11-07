"""Simulador: decide qué eventos emitir en cada instante.

Recibe el instante actual (no usa el reloj del sistema), así que se prueba con un reloj
virtual. Mantiene una cola con los eventos futuros de los ataques en curso: un ataque dura
de segundos a varios minutos, y cada transacción se emite cuando le toca.
"""
import heapq
import random

from events import DEFAULT_PATTERN_WEIGHTS


class Simulator:
    def __init__(self, generator, rate, fraud_ratio=0.02, pattern_weights=None, seed=7):
        if not 0 <= fraud_ratio < 1:
            raise ValueError("fraud_ratio debe estar en [0, 1)")
        self.gen = generator
        self.rate = rate
        self.fraud_ratio = fraud_ratio
        self.weights = pattern_weights or DEFAULT_PATTERN_WEIGHTS
        self.rng = random.Random(seed)
        # Si una fracción f de TODOS los eventos es fraude, hay f/(1-f) eventos de fraude por
        # cada evento normal; se reparten en ataques de mean_fraud eventos cada uno.
        mean_fraud = generator.mean_fraud_events(self.weights)
        self.attack_prob = (fraud_ratio / (1 - fraud_ratio)) / mean_fraud if fraud_ratio else 0.0
        self._heap = []
        self._seq = 0
        self._start = None
        self.emitted = 0
        self.attacks_started = 0
        self.pending = 0

    def _schedule(self, ts, event):
        heapq.heappush(self._heap, (ts, self._seq, event))
        self._seq += 1

    def step(self, now):
        """Eventos que deben salir hasta el instante `now` (epoch en segundos)."""
        if self._start is None:
            self._start = now
        out = []
        while self._heap and self._heap[0][0] <= now:
            out.append(heapq.heappop(self._heap)[2])
        quota = int((now - self._start) * self.rate) - self.emitted - len(out)
        patterns, w = list(self.weights), list(self.weights.values())
        for _ in range(max(0, quota)):
            out.append(self.gen.normal_event(now))
            if self.attack_prob and self.rng.random() < self.attack_prob:
                self.attacks_started += 1
                for ts, event in self.gen.attack_episode(self.rng.choices(patterns, w)[0], now):
                    if ts <= now:
                        out.append(event)
                    else:
                        self._schedule(ts, event)
        self.emitted += len(out)
        self.pending = len(self._heap)
        return out
