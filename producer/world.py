"""Mundo simulado de PagaYa: ciudades, usuarios y comercios.

Representa las tablas maestras del core (base relacional) que se exportan cada
noche en CSV a S3 landing/. El productor y generate_catalogs.py comparten estos
datos: así los IDs de las transacciones siempre existen en los catálogos y los
joins de Silver no fallan.
"""
import csv
import math
from dataclasses import dataclass
from pathlib import Path

# (código, ciudad, departamento, latitud, longitud, peso poblacional relativo)
CIUDADES = [
    ("BOG", "Bogotá D.C.", "Cundinamarca", 4.7110, -74.0721, 0.30),
    ("MDE", "Medellín", "Antioquia", 6.2442, -75.5812, 0.16),
    ("CLO", "Cali", "Valle del Cauca", 3.4516, -76.5320, 0.12),
    ("BAQ", "Barranquilla", "Atlántico", 10.9685, -74.7813, 0.09),
    ("CTG", "Cartagena", "Bolívar", 10.3910, -75.4794, 0.06),
    ("BGA", "Bucaramanga", "Santander", 7.1193, -73.1227, 0.05),
    ("PEI", "Pereira", "Risaralda", 4.8133, -75.6961, 0.04),
    ("MZL", "Manizales", "Caldas", 5.0703, -75.5138, 0.03),
    ("CUC", "Cúcuta", "Norte de Santander", 7.8939, -72.5078, 0.04),
    ("SMR", "Santa Marta", "Magdalena", 11.2408, -74.1990, 0.03),
    ("IBE", "Ibagué", "Tolima", 4.4389, -75.2322, 0.03),
    ("VVC", "Villavicencio", "Meta", 4.1420, -73.6266, 0.03),
    ("PSO", "Pasto", "Nariño", 1.2136, -77.2811, 0.02),
    ("MTR", "Montería", "Córdoba", 8.7480, -75.8814, 0.02),
    ("AXM", "Armenia", "Quindío", 4.5339, -75.6811, 0.02),
]

# Multiplicador del monto típico del usuario según la categoría del comercio.
CATEGORIAS = {
    "SUPERMERCADO": 1.5,
    "RESTAURANTE": 0.9,
    "TRANSPORTE": 0.4,
    "FARMACIA": 0.8,
    "ECOMMERCE": 1.5,
    "SERVICIOS": 1.2,
    "ENTRETENIMIENTO": 0.9,
}

# Monto típico (mediana, COP) por segmento de usuario.
SEGMENTOS = {"JOVEN": 25_000, "ADULTO": 45_000, "ADULTO_MAYOR": 35_000}

KYC_LEVELS = ("BASICO", "COMPLETO")

CIUDADES_HEADER = ["city_code", "city", "department", "lat", "lon", "population_weight"]
USUARIOS_HEADER = ["user_id", "kyc_level", "segment", "home_city_code", "registered_at"]
COMERCIOS_HEADER = ["merchant_id", "name", "category", "city_code"]
# Perfil inicial: arranque en frío de silver.user_profile (promedio y actividad de 30 días).
PERFIL_HEADER = ["user_id", "avg_amount_30d", "n_tx_30d"]


@dataclass(frozen=True)
class City:
    code: str
    name: str
    department: str
    lat: float
    lon: float
    weight: float


@dataclass(frozen=True)
class User:
    user_id: str
    kyc_level: str
    segment: str
    home_city: str
    registered_at: str
    avg_amount_30d: float
    n_tx_30d: int


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    name: str
    category: str
    city_code: str


@dataclass
class World:
    cities: dict
    users: list
    merchants: list

    def merchants_in(self, city_code):
        return [m for m in self.merchants if m.city_code == city_code]


def haversine_km(lat1, lon1, lat2, lon2):
    """Distancia en kilómetros entre dos coordenadas."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _read(path, name):
    with open(Path(path) / name, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_world(catalog_dir):
    """Carga los 4 CSV que escribe generate_catalogs.py."""
    cities = {
        r["city_code"]: City(r["city_code"], r["city"], r["department"], float(r["lat"]), float(r["lon"]),
                             float(r["population_weight"]))
        for r in _read(catalog_dir, "ciudades.csv")
    }
    perfil = {r["user_id"]: r for r in _read(catalog_dir, "perfil_inicial.csv")}
    users = [
        User(r["user_id"], r["kyc_level"], r["segment"], r["home_city_code"], r["registered_at"],
             float(perfil[r["user_id"]]["avg_amount_30d"]), int(perfil[r["user_id"]]["n_tx_30d"]))
        for r in _read(catalog_dir, "usuarios.csv")
    ]
    merchants = [Merchant(r["merchant_id"], r["name"], r["category"], r["city_code"])
                 for r in _read(catalog_dir, "comercios.csv")]
    return World(cities, users, merchants)
