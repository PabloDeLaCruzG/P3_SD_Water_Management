# control.py, Ordenes de Central a las estaciones (BLOQUEAR / ACTIVAR)
#
# Las usa el menu de Central. Cada funcion devuelve (ok, mensaje) para que el menu lo muestre.
#
# Reglas:
#   - FUERA_DE_SERVICIO es una orden de Central, no un estado que dependa de la estacion.
#     Por eso sobrevive a una averia, a una desconexion, a un re-registro del Monitor y a un reinicio de Central
#     Solo lo quita ACTIVAR
#   - BLOQUEAR cierra todos los riegos abiertos de la estacion (con su resumen al operario) y manda BLOQUEAR al Engine
#   - ACTIVAR deja la estacion en ACTIVADA si su Monitor esta conectado, y en DESCONECTADA si no lo esta

from WM_Central.central import irrigation
from WM_Central.central import kafka_utils
from WM_Central.central import socket_server


# Lista (id, ubicacion, status) de todas las estaciones, util para el menu ("a una o a todas") y el panel
def list_stations(db):
    return db.execute("SELECT id, ubicacion, status FROM stations ORDER BY id").fetchall()


# True si el Monitor de esa estacion tiene ahora mismo una conexion abierta con Central
def is_connected(ws_id):
    with socket_server.active_ws_lock:
        return ws_id in socket_server.active_ws


def block_station(db, ws_id):
    row = db.execute("SELECT status FROM stations WHERE id = ?", (ws_id,)).fetchone()
    if row is None:
        return False, f"Estacion {ws_id} no registrada"
    if row[0] == "FUERA_DE_SERVICIO":
        return False, f"{ws_id} ya esta fuera de servicio"

    # Primero el estado, para que al cerrar los riegos la estacion no vuelva a ACTIVADA
    db.execute("UPDATE stations SET status = 'FUERA_DE_SERVICIO' WHERE id = ?", (ws_id,))
    db.commit()

    # Cierra los riegos abiertos, avisa a sus solicitantes y manda DETENER_RIEGO por cada uno
    irrigation.abort_station_irrigations(db, ws_id, "BLOQUEO", "La estacion ha sido bloqueada por Central")

    # Orden de estado al Engine (que ademas finaliza cualquier riego que le quedara)
    kafka_utils.publish_json(f"wm.ws.{ws_id}", "BLOQUEAR", {})
    print(f"[CENTRAL] {ws_id} -> FUERA_DE_SERVICIO (previo: {row[0]})")
    return True, f"{ws_id} bloqueada (FUERA_DE_SERVICIO)"


def activate_station(db, ws_id):
    row = db.execute("SELECT status FROM stations WHERE id = ?", (ws_id,)).fetchone()
    if row is None:
        return False, f"Estacion {ws_id} no registrada"
    if row[0] != "FUERA_DE_SERVICIO":
        return False, f"{ws_id} no esta fuera de servicio (estado: {row[0]})"

    new_status = "ACTIVADA" if is_connected(ws_id) else "DESCONECTADA"
    db.execute(
        "UPDATE stations SET status = ? WHERE id = ? AND status = 'FUERA_DE_SERVICIO'",
        (new_status, ws_id)
    )
    db.commit()

    kafka_utils.publish_json(f"wm.ws.{ws_id}", "ACTIVAR", {})
    print(f"[CENTRAL] {ws_id} -> {new_status} (activada por Central)")
    return True, f"{ws_id} activada ({new_status})"