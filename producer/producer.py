"""Productor sintético de PagaYa: publica transacciones en el topic `transactions` de Kafka.

Simula el core de PagaYa. Emite en tiempo real a una tasa configurable e inyecta ataques de
fraude etiquetados (ráfaga, monto acumulado, monto atípico y viaje imposible).

Uso (la configuración también se lee de variables de entorno):
  python producer/producer.py --catalogs data/catalogs --rate 20 --fraud-ratio 0.02 --sink stdout
  python producer/producer.py --catalogs data/catalogs --rate 200 --bootstrap broker:9094 --protocol SSL

Variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_SECURITY_PROTOCOL, PRODUCER_RATE_EPS,
PRODUCER_FRAUD_RATIO, CATALOG_DIR.
"""
import argparse
import json
import os
import signal
import sys
import time

from events import EventGenerator
from simulator import Simulator
from world import load_world

TICK_SECONDS = 0.05  # cada cuánto se decide qué emitir


class StdoutSink:
    """Una línea JSON por evento; útil para depurar sin Kafka."""

    def __init__(self):
        self.sent = 0

    def send(self, key, value):
        sys.stdout.write(value.decode("utf-8") + "\n")
        self.sent += 1

    def flush(self):
        sys.stdout.flush()

    @property
    def errors(self):
        return 0


class KafkaSink:
    """Productor idempotente: acks=all y reintentos sin duplicados (ADR-01)."""

    def __init__(self, bootstrap, topic, protocol):
        from confluent_kafka import Producer  # solo se importa si se usa Kafka

        self.topic = topic
        self.sent = 0
        self.errors = 0
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "security.protocol": protocol,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 120_000,  # tolera hasta 2 min sin broker (RNF-05)
            "linger.ms": 20,
            "compression.type": "lz4",
        })

    def _on_delivery(self, err, msg):
        if err is not None:
            self.errors += 1
            print(f"ERROR entrega: {err}", file=sys.stderr)
        else:
            self.sent += 1

    def send(self, key, value):
        while True:
            try:
                self._producer.produce(self.topic, key=key, value=value, on_delivery=self._on_delivery)
                break
            except BufferError:  # cola local llena: dejar que se vacíe
                self._producer.poll(0.1)
        self._producer.poll(0)

    def flush(self):
        self._producer.flush(30)


def parse_args(argv=None):
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalogs", default=env("CATALOG_DIR", "data/catalogs"), help="carpeta con los CSV de generate_catalogs.py")
    p.add_argument("--rate", type=float, default=float(env("PRODUCER_RATE_EPS") or 20), help="eventos por segundo (10 a 1000)")
    p.add_argument("--fraud-ratio", type=float, default=float(env("PRODUCER_FRAUD_RATIO") or 0.02), help="fracción de eventos que son fraude")
    p.add_argument("--sink", choices=("kafka", "stdout"), default="kafka")
    p.add_argument("--bootstrap", default=env("KAFKA_BOOTSTRAP_SERVERS") or "localhost:29092")
    p.add_argument("--topic", default=env("KAFKA_TOPIC") or "transactions")
    p.add_argument("--protocol", default=env("KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT", help="PLAINTEXT (local) o SSL (MSK)")
    p.add_argument("--duration", type=float, default=0, help="segundos a correr (0 = hasta Ctrl-C)")
    p.add_argument("--seed", type=int, default=int(time.time()), help="semilla; fíjala para reproducir una corrida")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    world = load_world(args.catalogs)
    # Regla práctica de generate_catalogs.py: con pocos usuarios los normales disparan ráfagas por azar.
    if len(world.users) < args.rate * 600:
        print(f"AVISO: {len(world.users)} usuarios para {args.rate:g} ev/s; se recomiendan al menos "
              f"{int(args.rate * 600)} o aparecerán ráfagas por azar.", file=sys.stderr)

    sim = Simulator(EventGenerator(world, seed=args.seed), args.rate, args.fraud_ratio, seed=args.seed)
    sink = StdoutSink() if args.sink == "stdout" else KafkaSink(args.bootstrap, args.topic, args.protocol)
    print(f"Productor: {args.rate:g} ev/s, fraude {args.fraud_ratio:.1%}, {len(world.users)} usuarios, "
          f"sink={args.sink}, semilla={args.seed}", file=sys.stderr)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    started = time.time()
    last_report, last_count = started, 0
    while not stop["flag"]:
        now = time.time()
        if args.duration and now - started >= args.duration:
            break
        for event in sim.step(now):
            sink.send(event["user_id"].encode("utf-8"), json.dumps(event, ensure_ascii=False).encode("utf-8"))
        if now - last_report >= 10:
            rate = (sim.emitted - last_count) / (now - last_report)
            print(f"[{now - started:6.0f}s] emitidos={sim.emitted} ({rate:.0f} ev/s) ataques={sim.attacks_started} "
                  f"en_cola={sim.pending} errores={sink.errors}", file=sys.stderr)
            last_report, last_count = now, sim.emitted
        time.sleep(max(0.0, TICK_SECONDS - (time.time() - now)))

    sink.flush()
    print(f"Fin: emitidos={sim.emitted} ataques={sim.attacks_started} errores={sink.errors}", file=sys.stderr)
    return 0 if sink.errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
