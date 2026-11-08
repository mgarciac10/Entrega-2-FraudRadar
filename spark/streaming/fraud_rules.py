"""Lógica de las 4 reglas de fraude, por usuario y en orden de tiempo. Python puro (sin Spark).

La usa detect_fraud dentro de un operador con estado (una "libreta" por usuario). Está separada de
Spark para poder probarla directamente contra tests/reference_rules.py, que es la especificación.

Cómo funciona: por cada usuario se guarda una lista corta con sus transacciones recientes (las de los
últimos 30 min) y, para las reglas de ventana, el "episodio" activo. Cada transacción nueva se evalúa
contra esa libreta, igual que en reference_rules.py:

  rafaga           más de max_tx transacciones en window_minutes (incluida la actual)
  monto_acumulado  más de max_amount_cop sumados en window_minutes (incluida la actual)
  monto_atipico    monto > factor x promedio de 30 días del usuario (si no hay perfil, no evalúa)
  viaje_imposible  la transacción anterior del usuario fue en otra ciudad hace menos de window_minutes

Episodio: en ráfaga y monto acumulado la regla sigue cumpliéndose en cada transacción nueva mientras
la ventana siga "caliente". Eso es UN solo ataque, no varios: el primer disparo abre el episodio, fija
`window_start` (la transacción más antigua de la ventana en ese momento) y los siguientes disparos lo
reutilizan. Como alert_id = hash(user_id, regla, window_start), todos caen en la misma alerta.
El episodio termina cuando la regla deja de cumplirse.

Tiempos en milisegundos desde epoch (enteros): sin zonas horarias ni errores de redondeo.
"""
import hashlib
from bisect import bisect_right
from datetime import datetime, timezone

import reglas

RAFAGA, MONTO_ACUMULADO, MONTO_ATIPICO, VIAJE_IMPOSIBLE = "rafaga", "monto_acumulado", "monto_atipico", "viaje_imposible"
WINDOW_RULES = (RAFAGA, MONTO_ACUMULADO)

# La libreta guarda lo necesario para la ventana más larga (30 min, viaje imposible)
RETENTION_MS = max(
    reglas.RAFAGA["window_minutes"], reglas.MONTO_ACUMULADO["window_minutes"], reglas.VIAJE_IMPOSIBLE["window_minutes"]
) * 60_000

# Posiciones dentro de cada transacción guardada en la libreta
TS, TX_ID, AMOUNT, CITY = 0, 1, 2, 3


def new_state():
    return {"events": [], "episodes": {RAFAGA: None, MONTO_ACUMULADO: None}}


def alert_id(user_id, rule, window_start_ms):
    """Llave de la alerta en DynamoDB: la misma alerta siempre produce el mismo id."""
    return hashlib.sha256(f"{user_id}|{rule}|{window_start_ms}".encode()).hexdigest()[:32]


def iso_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _fire(rule, window_start_ms, trigger, tx_ids, amount, evidence, first):
    return {
        "rule": rule,
        "window_start_ms": window_start_ms,
        "trigger_ts_ms": trigger["ts_ms"],
        "trigger_tx_id": trigger["transaction_id"],
        "transaction_ids": tx_ids,
        "amount_at_risk_cop": amount,
        "evidence": evidence,
        "first_of_episode": first,
    }


def evaluate(state, events, cfg=None):
    """Evalúa un lote de transacciones de UN usuario.

    state:  libreta del usuario (new_state() la primera vez). Se modifica y se devuelve.
    events: dicts con transaction_id, ts_ms, amount_cop, city_code y avg_amount_30d (puede ser None).
    Devuelve (state, disparos): un disparo por cada regla que se cumple en cada transacción.
    `first_of_episode` marca el primero de cada episodio; es el único que detect_fraud emite.
    """
    cfg = cfg or {"rafaga": reglas.RAFAGA, "monto_acumulado": reglas.MONTO_ACUMULADO,
                  "monto_atipico": reglas.MONTO_ATIPICO, "viaje_imposible": reglas.VIAJE_IMPOSIBLE}
    w_rafaga = cfg["rafaga"]["window_minutes"] * 60_000
    w_monto = cfg["monto_acumulado"]["window_minutes"] * 60_000
    w_viaje = cfg["viaje_imposible"]["window_minutes"] * 60_000

    book, episodes, fires = state["events"], state["episodes"], []
    # Orden estable por tiempo: dos transacciones con el mismo instante conservan su orden de llegada
    for e in sorted(events, key=lambda x: x["ts_ms"]):
        row = [e["ts_ms"], e["transaction_id"], int(e["amount_cop"]), e["city_code"]]
        idx = bisect_right([r[TS] for r in book], e["ts_ms"])
        book.insert(idx, row)
        past = book[: idx + 1]  # lo ocurrido hasta esta transacción, incluida ella

        # --- ráfaga y monto acumulado: ventana deslizante que termina en esta transacción ---
        for rule, window in ((RAFAGA, w_rafaga), (MONTO_ACUMULADO, w_monto)):
            in_win = [r for r in past if e["ts_ms"] - r[TS] <= window]
            if rule == RAFAGA:
                hit = len(in_win) > cfg["rafaga"]["max_tx"]
                evidence = {"n_transactions": len(in_win), "max_tx": cfg["rafaga"]["max_tx"],
                            "window_minutes": cfg["rafaga"]["window_minutes"]}
            else:
                total = sum(r[AMOUNT] for r in in_win)
                hit = total > cfg["monto_acumulado"]["max_amount_cop"]
                evidence = {"total_amount_cop": total, "max_amount_cop": cfg["monto_acumulado"]["max_amount_cop"],
                            "window_minutes": cfg["monto_acumulado"]["window_minutes"]}
            if hit:
                first = episodes[rule] is None
                if first:
                    episodes[rule] = in_win[0][TS]  # inicio de la ventana al primer disparo
                fires.append(_fire(rule, episodes[rule], e, [r[TX_ID] for r in in_win],
                                   sum(r[AMOUNT] for r in in_win), evidence, first))
            else:
                episodes[rule] = None

        # --- monto atípico: esta transacción contra el promedio histórico del usuario ---
        avg = e.get("avg_amount_30d")
        if avg is not None and avg == avg:  # avg == avg descarta NaN
            factor = cfg["monto_atipico"]["factor"]
            if e["amount_cop"] > factor * avg:
                fires.append(_fire(MONTO_ATIPICO, e["ts_ms"], e, [e["transaction_id"]], int(e["amount_cop"]),
                                   {"amount_cop": int(e["amount_cop"]), "avg_amount_30d": round(float(avg), 2),
                                    "factor": factor, "times_avg": round(e["amount_cop"] / avg, 2) if avg else None},
                                   True))

        # --- viaje imposible: la transacción anterior del usuario, en otra ciudad y hace poco ---
        if idx > 0:
            prev = book[idx - 1]
            if prev[CITY] != e["city_code"] and e["ts_ms"] - prev[TS] < w_viaje:
                fires.append(_fire(VIAJE_IMPOSIBLE, prev[TS], e, [prev[TX_ID], e["transaction_id"]],
                                   prev[AMOUNT] + int(e["amount_cop"]),
                                   {"from_city": prev[CITY], "to_city": e["city_code"],
                                    "minutes_between": round((e["ts_ms"] - prev[TS]) / 60_000, 2),
                                    "window_minutes": cfg["viaje_imposible"]["window_minutes"]}, True))

    # La libreta solo guarda lo que aún puede servir a una ventana
    if book:
        horizon = book[-1][TS] - RETENTION_MS
        state["events"] = [r for r in book if r[TS] >= horizon]
    return state, fires
