# tests

Pruebas de las reglas de fraude y de las transformaciones, más las pruebas de requisitos no funcionales que se muestran en la demo:

- Matar un job de Spark y verificar que Silver y las alertas no cambian al reiniciar (exactly-once, RNF-03).
- 200 ev/s durante 10 min y pico de 1.000 ev/s (RNF-01).
- Latencia evento → alerta menor a 2 min (RNF-04).
