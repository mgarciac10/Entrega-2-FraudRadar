"""Pruebas de los catálogos, el contrato del evento y los umbrales de las reglas."""
import os
import sys
import tempfile
import unittest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "producer"))
sys.path.insert(0, os.path.join(ROOT, "spark", "common"))

import generate_catalogs  # noqa: E402
import reglas  # noqa: E402
import schemas  # noqa: E402
import world  # noqa: E402


class CatalogsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dir = os.path.join(cls.tmp.name, "a")
        generate_catalogs.generate(500, 60, 7, cls.dir)
        cls.world = world.load_world(cls.dir)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_cantidades(self):
        self.assertEqual(len(self.world.users), 500)
        self.assertGreaterEqual(len(self.world.merchants), 60)
        self.assertEqual(len(self.world.cities), len(world.CIUDADES))

    def test_ids_unicos(self):
        ids = [u.user_id for u in self.world.users]
        self.assertEqual(len(ids), len(set(ids)))
        mids = [m.merchant_id for m in self.world.merchants]
        self.assertEqual(len(mids), len(set(mids)))

    def test_integridad_referencial(self):
        # Todo lo que el productor pueda elegir debe existir en las ciudades.
        for u in self.world.users:
            self.assertIn(u.home_city, self.world.cities)
        for m in self.world.merchants:
            self.assertIn(m.city_code, self.world.cities)

    def test_hay_comercio_en_cada_ciudad(self):
        for code in self.world.cities:
            self.assertTrue(self.world.merchants_in(code), f"sin comercios en {code}")

    def test_valores_validos(self):
        for u in self.world.users:
            self.assertIn(u.kyc_level, world.KYC_LEVELS)
            self.assertIn(u.segment, world.SEGMENTOS)
            self.assertGreater(u.avg_amount_30d, 0)
            self.assertGreaterEqual(u.n_tx_30d, 4)
        for m in self.world.merchants:
            self.assertIn(m.category, world.CATEGORIAS)

    def test_determinista(self):
        with tempfile.TemporaryDirectory() as other:
            generate_catalogs.generate(500, 60, 7, other)
            for name in ("usuarios.csv", "comercios.csv", "perfil_inicial.csv", "ciudades.csv"):
                with open(os.path.join(self.dir, name), "rb") as a, open(os.path.join(other, name), "rb") as b:
                    self.assertEqual(a.read(), b.read(), name)

    def test_semilla_distinta_cambia_resultado(self):
        with tempfile.TemporaryDirectory() as other:
            generate_catalogs.generate(500, 60, 8, other)
            with open(os.path.join(self.dir, "perfil_inicial.csv"), "rb") as a, \
                    open(os.path.join(other, "perfil_inicial.csv"), "rb") as b:
                self.assertNotEqual(a.read(), b.read())

    def test_haversine(self):
        bog, mde = self.world.cities["BOG"], self.world.cities["MDE"]
        d = world.haversine_km(bog.lat, bog.lon, mde.lat, mde.lon)
        self.assertTrue(200 < d < 280, d)  # Bogotá-Medellín: ~240 km en línea recta
        self.assertAlmostEqual(world.haversine_km(bog.lat, bog.lon, bog.lat, bog.lon), 0.0)


class ContratoTest(unittest.TestCase):
    def test_campos_unicos_y_tipos_validos(self):
        names = [f[0] for f in schemas.TRANSACTION_EVENT]
        self.assertEqual(len(names), len(set(names)))
        for _, kind, nullable, desc in schemas.TRANSACTION_EVENT:
            self.assertIn(kind, {"string", "long", "double", "boolean", "timestamp"})
            self.assertIsInstance(nullable, bool)
            self.assertTrue(desc)

    def test_etiquetas_de_fraude_presentes(self):
        for field in ("is_fraud_injected", "attack_id", "attack_pattern"):
            self.assertIn(field, schemas.FIELD_NAMES)

    def test_campos_obligatorios_no_incluyen_los_opcionales(self):
        for field in ("merchant_id", "counterparty_user_id", "attack_id", "attack_pattern"):
            self.assertNotIn(field, schemas.REQUIRED_FIELDS)

    def test_patrones_coinciden_con_las_reglas(self):
        self.assertEqual(set(schemas.ATTACK_PATTERNS), {"rafaga", "monto_acumulado", "monto_atipico", "viaje_imposible"})


class ReglasTest(unittest.TestCase):
    def test_umbrales_razonables(self):
        self.assertGreater(reglas.RAFAGA["max_tx"], 0)
        self.assertGreater(reglas.RAFAGA["window_minutes"], 0)
        self.assertGreater(reglas.MONTO_ACUMULADO["max_amount_cop"], 0)
        self.assertGreater(reglas.MONTO_ATIPICO["factor"], 1)
        self.assertGreater(reglas.VIAJE_IMPOSIBLE["window_minutes"], 0)


if __name__ == "__main__":
    unittest.main()
