"""Umbrales de las 4 reglas de fraude.

Son supuestos iniciales del equipo, no cifras de la industria (ver
docs/estado_del_arte.md, sección 4). Viven aquí, y no dentro del código de
detect_fraud, para poder calibrarlos con la tasa de falsos positivos (Q7)
sin tocar la lógica. El productor los usa para fabricar ataques que
realmente los superen.
"""

# Ráfaga: más de MAX_TX transacciones de un usuario en WINDOW_MINUTES.
RAFAGA = {"window_minutes": 5, "max_tx": 5}

# Monto acumulado: más de MAX_AMOUNT_COP sumados por un usuario en WINDOW_MINUTES.
MONTO_ACUMULADO = {"window_minutes": 10, "max_amount_cop": 2_000_000}

# Monto atípico: una transacción mayor a FACTOR veces el promedio de PROFILE_DAYS días del usuario.
MONTO_ATIPICO = {"factor": 5.0, "profile_days": 30}

# Viaje imposible: transacciones de un mismo usuario en dos ciudades distintas en menos de WINDOW_MINUTES.
VIAJE_IMPOSIBLE = {"window_minutes": 30}
