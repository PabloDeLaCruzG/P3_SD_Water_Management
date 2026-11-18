import json
import time
import uuid
 
from WM_Central.central import config
from WM_Central.central import database
from WM_Central.central import kafka_utils
from WM_Central.central import control 
 
CENTRAL_ORIGIN = "FO:CENTRAL"
 
 
def available_stations():
    # Las que ahora mismo estan ACTIVADAS (ni regando, ni con fuga, ni desconectadas)
    conn = database.get_db_connection()
    try:
        return [r[0] for r in conn.execute("SELECT id FROM stations WHERE status = 'ACTIVADA' ORDER BY id")]
    finally:
        conn.close()
 
 
def request_irrigation(ws_id, duration_s):
    request_id = str(uuid.uuid4())    # id_msg = id_solicitud, igual que hace un operario
    envelope = {
        "tipo": "SOLICITUD_RIEGO",
        "id_msg": request_id,
        "origen": CENTRAL_ORIGIN,
        "ts": int(time.time()),
        "payload": {"ws_id": ws_id, "duracion_s": duration_s},
    }
    kafka_utils.producer.produce(config.TOPIC_REQ, value=json.dumps(envelope).encode(config.FORMAT))
    kafka_utils.producer.poll(0)
    return request_id
 
 
def irrigate_all(duration_s):
    # Devuelve los ids de las estaciones a las que se ha pedido riego
    if kafka_utils.producer is None:
        raise RuntimeError("Kafka aun no esta listo")
    ids = available_stations()
    for ws_id in ids:
        request_irrigation(ws_id, duration_s)
    print(f"[MENU] Central pide riego de {duration_s}s en {len(ids)} estaciones: {', '.join(ids) or '-'}")
    return ids

# Ejecuta una orden de Central sobre una estacion o sobre todas
# action: REGAR | BLOQUEAR | ACTIVAR. target: id de estacion o "TODAS"
def run_order(action, target, duration_s=None):
    if kafka_utils.producer is None:
        raise RuntimeError("Kafka aun no esta listo")

    if action == "REGAR":
        if target == "TODAS":
            ids = irrigate_all(duration_s)
            text = f"Riego pedido en {len(ids)} estaciones"
        else:
            # Igual que un operario: si no esta disponible, Central la denegara y saldra en Mensajes
            request_irrigation(target, duration_s)
            print(f"[MENU] Central pide riego de {duration_s}s en {target}")
            ids = [target]
            text = f"Riego pedido en {target} (el resultado sale en Mensajes)"
        return {"resumen": text, "mensajes": [text], "estaciones": ids}

    # BLOQUEAR / ACTIVAR: cada orden devuelve (ok, mensaje)
    conn = database.get_db_connection()
    try:
        if target == "TODAS":
            ids = [s[0] for s in control.list_stations(conn)]
        else:
            ids = [target]
        order = control.block_station if action == "BLOQUEAR" else control.activate_station
        results = [(ws_id, *order(conn, ws_id)) for ws_id in ids]
    finally:
        conn.close()

    done = [ws_id for ws_id, ok, _ in results if ok]
    return {
        "resumen": f"{action}: {len(done)} de {len(ids)} estaciones",
        "mensajes": [msg for _, _, msg in results],
        "estaciones": done,
    }