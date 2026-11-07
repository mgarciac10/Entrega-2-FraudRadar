"""Genera los catálogos que simulan el export diario de las tablas maestras del core.

Escribe 4 CSV en el directorio de salida:
  ciudades.csv        ciudades con coordenadas (para el viaje imposible)
  usuarios.csv        usuarios con nivel KYC, segmento y ciudad de residencia
  comercios.csv       comercios con categoría y ciudad
  perfil_inicial.csv  promedio y actividad de 30 días de cada usuario: arranque en
                      frío de silver.user_profile (sin historial, la regla de monto
                      atípico no podría disparar). build_user_profile lo reemplaza
                      con datos reales a medida que corre el pipeline.

Es determinista: la misma semilla produce los mismos archivos.

Tamaño del universo: debe crecer con la tasa del productor. Con pocos usuarios y mucha
tasa, cada usuario haría una transacción cada pocos segundos y TODOS dispararían la regla
de ráfaga. Regla práctica: usuarios >= tasa (ev/s) x 600, o sea ~1 transacción por usuario
cada 10 minutos. 100.000 usuarios sostienen 150 ev/s sin ráfagas por azar.

Uso:
  python producer/generate_catalogs.py --users 100000 --merchants 1000 --seed 42 --out data/catalogs
Luego se suben a S3 landing/:
  aws s3 cp data/catalogs s3://<bucket>/landing/ --recursive --profile fraudradar
"""
import argparse
import csv
import random
from datetime import date, timedelta
from pathlib import Path

from world import (CATEGORIAS, CIUDADES, CIUDADES_HEADER, COMERCIOS_HEADER, KYC_LEVELS, PERFIL_HEADER, SEGMENTOS,
                   USUARIOS_HEADER)

SEGMENT_WEIGHTS = {"JOVEN": 0.25, "ADULTO": 0.55, "ADULTO_MAYOR": 0.20}
KYC_WEIGHTS = {"BASICO": 0.6, "COMPLETO": 0.4}
SIGMA_AVG = 0.35  # dispersión del monto típico entre usuarios del mismo segmento


def _write(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def generate(n_users, n_merchants, seed, out_dir, today=None):
    rng = random.Random(seed)
    today = today or date(2026, 11, 1)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    codes = [c[0] for c in CIUDADES]
    weights = [c[5] for c in CIUDADES]

    _write(out / "ciudades.csv", CIUDADES_HEADER, CIUDADES)

    usuarios, perfiles = [], []
    for i in range(1, n_users + 1):
        uid = f"u-{i:06d}"
        segment = rng.choices(list(SEGMENT_WEIGHTS), list(SEGMENT_WEIGHTS.values()))[0]
        kyc = rng.choices(KYC_LEVELS, [KYC_WEIGHTS[k] for k in KYC_LEVELS])[0]
        city = rng.choices(codes, weights)[0]
        registered = today - timedelta(days=rng.randint(30, 3 * 365))
        usuarios.append([uid, kyc, segment, city, registered.isoformat()])
        avg = round(SEGMENTOS[segment] * rng.lognormvariate(0, SIGMA_AVG), -2)
        n_tx = max(4, int(rng.lognormvariate(3.2, 0.5)))  # ~25 transacciones al mes de mediana
        perfiles.append([uid, int(avg), n_tx])
    _write(out / "usuarios.csv", USUARIOS_HEADER, usuarios)
    _write(out / "perfil_inicial.csv", PERFIL_HEADER, perfiles)

    comercios = []
    cats = list(CATEGORIAS)
    for i in range(1, n_merchants + 1):
        cat = rng.choice(cats)
        comercios.append([f"m-{i:04d}", f"{cat.title()} {i:04d}", cat, rng.choices(codes, weights)[0]])
    # Garantiza al menos un comercio por ciudad, para que siempre haya dónde pagar.
    present = {c[3] for c in comercios}
    for code in codes:
        if code not in present:
            i = len(comercios) + 1
            cat = rng.choice(cats)
            comercios.append([f"m-{i:04d}", f"{cat.title()} {i:04d}", cat, code])
    _write(out / "comercios.csv", COMERCIOS_HEADER, comercios)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--users", type=int, default=100_000, help="usuarios a simular; >= tasa x 600 (por defecto 100000)")
    p.add_argument("--merchants", type=int, default=1000, help="comercios (por defecto 1000)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default="data/catalogs")
    args = p.parse_args()
    out = generate(args.users, args.merchants, args.seed, args.out)
    print(f"Catálogos escritos en {out}: {args.users} usuarios, {args.merchants}+ comercios, {len(CIUDADES)} ciudades.")


if __name__ == "__main__":
    main()
