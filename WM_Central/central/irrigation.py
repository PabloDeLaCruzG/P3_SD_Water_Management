# irrigation.py, Logica de negocio de los riegos:
#   - validar y registrar una SOLICITUD_RIEGO
#   - tratar lo que contesta la estacion (vuelta: RESPUESTA_INICIO, TELEMETRIA, FIN_RIEGO)
#   - cerrar riegos que se quedan esperando a la estacion
#
# Ciclo de vida de un riego en la tabla de riegos:
#   PENDIENTE -> Central ha ordenado iniciar y espera confirmacion de la estacion
#   EN_CURSO  -> la estacion confirmo y esta regando
#   FINALIZADO -> la estacion aviso del fin de reigo y Central envia el resumen
#   ABORTADO  -> se cerró sin llegar a regar normalmente (rechazo, timeout, desconexion, caida de Central, orden de parada de un operario o averia)
#
# Estado de la estacion durante un riego: ACTIVADA -> REGANDO -> ACTIVADA
#
# Parar un riego: Central es quien manda. Cuando decide cerrar un riego por su cuenta lo cierra en su BD y ademas avisa al Engine con DETENER_RIEGO
# El FIN_RIEGO que el Engine mande despues se ignora porque el riego ya esta cerrado
#
# El solicitante es "FO:<id>" (el operario) o "WS:<id>" (menu de la propia estacion)
# Los estados de la solicitud van a los dos, la telemetria y el resumen final solo al operario, porque la estacion ya sabe lo que mide

import time

from WM_Central.central import config
from WM_Central.central import kafka_utils

##################################################################
## IDA: SOLICITUD DE RIEGO

# Trata una SOLICITUD_RIEGO ya validada como json
def handle_irrigation_request(msg, db):
    request_id = msg["id_msg"]
    origin = msg["origen"]
    payload = msg["payload"]

    # Sin origen valido no se puede ni denegar, solo se anota en el log
    reply = kafka_utils.reply_topic(origin)
    if reply is None:
        print(f"[SOLICITUD] origen no valido: {origin}")
        return


    # Validar el payload: ws_id texto no vacio, duracion_s entero positivo
    ws_id = payload.get("ws_id")
    duration_s = payload.get("duracion_s")

    payload_ok = (
        isinstance(ws_id, str) and ws_id
        and isinstance(duration_s, int) 
        and not isinstance(duration_s, bool) 
        and duration_s > 0
    )
    if not payload_ok:
        print(f"[SOLICITUD] payload mal formado de {origin}: {payload}")
        kafka_utils.notify(reply, request_id, str(ws_id or ""), "DENEGADA", "Solicitud mal formada")
        return
    

    print(f"[SOLICITUD] {origin} pide riego en {ws_id} durante {duration_s}s")
    kafka_utils.notify(reply, request_id, ws_id, "RECIBIDA")


    # La estacion tiene que existir y estar activada
    row = db.execute("SELECT status FROM stations WHERE id = ?", (ws_id,)).fetchone()
    if row is None:
        kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", "Estacion no registrada")
        return
    if row[0] != "ACTIVADA":
        # En cualquier otro estado no se puede regar
        kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", f"Estacion no disponible ({row[0]})")
        return


    # No puede haber otro riego en marcha en esa estacion
    busy = db.execute(
        "SELECT 1 FROM riegos WHERE ws_id = ? AND estado IN ('PENDIENTE', 'EN_CURSO')",
        (ws_id,)
    ).fetchone()
    if busy:
        kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", "Estacion con un riego en curso")
        return


    # Todo correcto, se guarda como PENDIENTE y se ordena a la estacion que empiece
    db.execute(
        "INSERT INTO riegos (id_solicitud, ws_id, solicitante, duracion_s, estado, creado_ts) "
        "VALUES (?, ?, ?, ?, 'PENDIENTE', ?)",
        (request_id, ws_id, origin, duration_s, int(time.time()))
    )
    db.commit()

    kafka_utils.publish_json(f"wm.ws.{ws_id}", "INICIAR_RIEGO", {
        "id_solicitud": request_id,
        "duracion_s": duration_s,
    })
    kafka_utils.notify(reply, request_id, ws_id, "VALIDANDO", "Esperando confirmacion de la estacion")


##################################################################
## VUELTA: LO QUE CONTESTA LA ESTACION (topic wm.central.telemetria)

## VALIDACIONES FORMATO
# bool es subclase de int en Python, por eso se excluye aparte
def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)

def is_text(value):
    return isinstance(value, str) and value != ""
##

# Busca el riego con ese id que este en ese estado y pertenezca a esa estacion, y devuelve el solicitante
def find_irrigation(db, request_id, ws_id, state):
    row = db.execute(
        "SELECT ws_id, solicitante FROM riegos WHERE id_solicitud = ? AND estado = ?",
        (request_id, state)
    ).fetchone()
    if row is None or row[0] != ws_id:
        return None
    return row[1]

# RESPUESTA_INICIO: la estacion dice si puede empezar el riego que Central le ordenó
def handle_start_response(msg, db):
    # payload: {ws_id, id_solicitud, aceptado (bool), motivo}
    payload = msg["payload"]

    request_id = payload.get("id_solicitud")
    ws_id = payload.get("ws_id")
    accepted = payload.get("aceptado")
    reason = payload.get("motivo")
 
    # Validar el payload
    if not (is_text(request_id) and is_text(ws_id) and isinstance(accepted, bool)):
        print(f"[RIEGO] RESPUESTA_INICIO mal formada: {payload}")
        return

 
    # Tiene que haber un riego PENDIENTE de esa estacion sino es una respuesta tardia
    requester = find_irrigation(db, request_id, ws_id, "PENDIENTE")
    if requester is None:
        print(f"[RIEGO] RESPUESTA_INICIO ignorada ({request_id}): no hay riego PENDIENTE de {ws_id}")
        # Si esa estacion acepto un riego que Central ya habia abortado, esa estacion esta regando sin que nadie lo sepa, entonces se le ordena parar
        if accepted and find_irrigation(db, request_id, ws_id, "ABORTADO") is not None:
            send_stop_order(ws_id, request_id, "RESPUESTA_TARDIA")
        return

 
    # La estacion no puede regar, se cierra el riego y se avisa al solicitante
    if not accepted:
        abort_pending_irrigations(db, [(request_id, ws_id, requester)],
                                  reason if is_text(reason) else "La estacion rechazo el riego")
        return
 
    # La estacion acepta, el riego pasa a EN_CURSO
    # El "AND estado = 'PENDIENTE'" asegura que otro hilo no lo haya cerrado justo ahora
    cur = db.execute(
        "UPDATE riegos SET estado = 'EN_CURSO' WHERE id_solicitud = ? AND estado = 'PENDIENTE'",
        (request_id,)
    )
    if cur.rowcount == 0:
        db.commit()
        return
    # La estacion pasa a REGANDO, pero solo si seguia ACTIVADA
    db.execute("UPDATE stations SET status = 'REGANDO' WHERE id = ? AND status = 'ACTIVADA'", (ws_id,))
    db.commit()

 
    print(f"[RIEGO] {request_id} en curso en {ws_id}")
    reply = kafka_utils.reply_topic(requester)
    if reply:
        kafka_utils.notify(reply, request_id, ws_id, "AUTORIZADA", "Riego en curso")

# TELEMETRIA: caudal y volumen acumulado, uno por segundo mientras la estacion riega
def handle_telemetry(msg, db):
    # payload: {ws_id, id_solicitud, caudal_lpm, volumen_l}
    payload = msg["payload"]

    request_id = payload.get("id_solicitud")
    ws_id = payload.get("ws_id")
    flow_lpm = payload.get("caudal_lpm")
    volume_l = payload.get("volumen_l")
 
    if not (is_text(request_id) and is_text(ws_id) and is_number(flow_lpm) and is_number(volume_l)):
        print(f"[TELEMETRIA] mal formada: {payload}")
        return
 
    # Solo se acepta de un riego EN_CURSO
    requester = find_irrigation(db, request_id, ws_id, "EN_CURSO")
    if requester is None:
        return
 
    # Ultima medida de cada estacion, es lo que mostrara el panel de Central
    db.execute(
        "INSERT OR REPLACE INTO telemetry_latest (ws_id, id_solicitud, caudal_lpm, volumen_l, ts) "
        "VALUES (?, ?, ?, ?, ?)",
        (ws_id, request_id, flow_lpm, volume_l, int(time.time()))
    )
    db.commit()
    print(f"[TELEMETRIA] {ws_id}: {flow_lpm} L/min | {volume_l} L")
 
    # Se reenvia solo al operario de este riego
    if requester.startswith("FO:"):
        kafka_utils.publish_json(kafka_utils.reply_topic(requester), "TELEMETRIA", {
            "id_solicitud": request_id,
            "ws_id": ws_id,
            "caudal_lpm": flow_lpm,
            "volumen_l": volume_l,
        })


# Cierra un riego EN_CURSO, libera la estacion y manda el resumen final al operario
def close_running_irrigation(db, request_id, ws_id, requester, new_state, end_reason, volume_l, duration_s):

    # El "AND estado = 'EN_CURSO'" evita cerrarlo dos veces si coinciden, por ejemplo, un FIN_RIEGO y una desconexion
    cur = db.execute(
        "UPDATE riegos SET estado = ? WHERE id_solicitud = ? AND estado = 'EN_CURSO'",
        (new_state, request_id)
    )
    if cur.rowcount == 0:
        db.commit()
        return False

    # La estacion vuelve a estar disponible, pero solo si seguia REGANDO
    db.execute("UPDATE stations SET status = 'ACTIVADA' WHERE id = ? AND status = 'REGANDO'", (ws_id,))
    db.execute("DELETE FROM telemetry_latest WHERE ws_id = ?", (ws_id,))
    db.commit()

    print(f"[RIEGO] {request_id} {new_state.lower()} en {ws_id} ({end_reason}): {volume_l} L en {duration_s}s")

    # Resumen final solo para el operario
    if requester.startswith("FO:"):
        kafka_utils.publish_json(kafka_utils.reply_topic(requester), "RESUMEN_RIEGO", {
            "id_solicitud": request_id,
            "ws_id": ws_id,
            "volumen_total_l": volume_l,
            "duracion_s": duration_s,
            "motivo_fin": end_reason,
        })

    return True

# FIN_RIEGO: la estacion avisa de que el riego ha terminado
def handle_irrigation_end(msg, db):
    # payload: {ws_id, id_solicitud, motivo (TIEMPO|MANUAL|ORDEN|AVERIA), volumen_total_l, duracion_s}
    payload = msg["payload"]

    request_id = payload.get("id_solicitud")
    ws_id = payload.get("ws_id")
    end_reason = payload.get("motivo")
    total_volume_l = payload.get("volumen_total_l")
    duration_s = payload.get("duracion_s")
 
    if not (is_text(request_id) and is_text(ws_id) and is_text(end_reason)
            and is_number(total_volume_l) and is_number(duration_s)):
        print(f"[RIEGO] FIN_RIEGO mal formado: {payload}")
        return
 
    requester = find_irrigation(db, request_id, ws_id, "EN_CURSO")
    if requester is None:
        print(f"[RIEGO] FIN_RIEGO ignorado ({request_id}): no hay riego EN_CURSO de {ws_id}")
        return
 
    close_running_irrigation(db, request_id, ws_id, requester,
                             "FINALIZADO", end_reason, total_volume_l, duration_s)



##################################################################
## PARAR UN RIEGO (Central -> Engine)

# Ordena a la estacion que pare ese riego
def send_stop_order(ws_id, request_id, reason):
    # reason: ORDEN, AVERIA, DESCONEXION, RESPUESTA_TARDIA
    kafka_utils.publish_json(f"wm.ws.{ws_id}", "DETENER_RIEGO", {
        "id_solicitud": request_id,
        "ws_id": ws_id,
        "motivo": reason,
    })
    print(f"[RIEGO] DETENER_RIEGO enviado a {ws_id} ({request_id}, {reason})")

# DETENER_RIEGO pedido por un operario (o por una estacion) a Central, en wm.central.solicitudes
def handle_stop_request(msg, db):
    # payload: {id_solicitud, ws_id}. El id_solicitud es el del riego que se quiere parar.
    origin = msg["origen"]
    payload = msg["payload"]

    reply = kafka_utils.reply_topic(origin)
    if reply is None:
        print(f"[PARADA] origen no valido: {origin}")
        return

    request_id = payload.get("id_solicitud")
    ws_id = payload.get("ws_id")
    if not (is_text(request_id) and is_text(ws_id)):
        print(f"[PARADA] payload mal formado de {origin}: {payload}")
        kafka_utils.notify(reply, str(request_id or ""), str(ws_id or ""), "DENEGADA", "Orden de parada mal formada")
        return

    print(f"[PARADA] {origin} pide parar el riego {request_id} en {ws_id}")

    # El riego tiene que existir y ser de esa estacion
    row = db.execute(
        "SELECT ws_id, solicitante, estado, creado_ts FROM riegos WHERE id_solicitud = ?",
        (request_id,)
    ).fetchone()
    if row is None or row[0] != ws_id:
        kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", "No existe ese riego en esa estacion")
        return
    _, requester, state, created_ts = row

    # Segun siga pendiente o en marcha se cierra de una forma u otra
    # "closed" cuenta los riegos que este hilo ha cerrado de verdad
    if state == "PENDIENTE":
        closed = abort_pending_irrigations(db, [(request_id, ws_id, requester)],
                                           f"Detenido por orden de {origin}", stop_reason="ORDEN")
    elif state == "EN_CURSO":
        closed = abort_running_irrigations(db, [(request_id, ws_id, requester, created_ts)], "ORDEN")
    else:
        kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", f"El riego ya habia terminado ({state})")
        return

    if closed == 0:
        kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", "El riego ya habia terminado")
    elif origin != requester:
        kafka_utils.notify(reply, request_id, ws_id, "DETENIDA", "Riego detenido")



##################################################################
## CIERRE DE RIEGOS COLGADOS

# Cierra como abortado los riegos PENDIENTE indicados y avisa a cada solicitante
def abort_pending_irrigations(db, rows, reason, stop_reason=None):
    # rows = lista de (id_solicitud, ws_id, solicitante)
    # stop_reason (ORDEN, AVERIA...)
    # Devuelve cuantos ha cerrado de verdad este hilo
    closed = 0 
    for request_id, ws_id, requester in rows:
        # El "AND estado = 'PENDIENTE'" evita avisar dos veces si otro hilo lo cerro antes
        cur = db.execute(
            "UPDATE riegos SET estado = 'ABORTADO' WHERE id_solicitud = ? AND estado = 'PENDIENTE'",
            (request_id,)
        )
        db.commit()

        # rowcount == 1 significa que este hilo es el que lo ha cerrado de verdad
        if cur.rowcount == 1:
            closed += 1
            print(f"[RIEGO] {request_id} abortado: {reason}")
            reply = kafka_utils.reply_topic(requester)
            if reply:
                kafka_utils.notify(reply, request_id, ws_id, "DENEGADA", reason)
            if stop_reason:
                send_stop_order(ws_id, request_id, stop_reason)  
    return closed

# Cierra como ABORTADO los riegos EN_CURSO indicados, manda su resumen al operario con la ultima medida conocida y ordena parar a la estacion
def abort_running_irrigations(db, rows, end_reason):
    # rows = lista de (id_solicitud, ws_id, solicitante, creado_ts)
    # Devuelve cuantos ha cerrado de verdad este hilo
    closed = 0
    for request_id, ws_id, requester, created_ts in rows:
        last = db.execute(
            "SELECT volumen_l FROM telemetry_latest WHERE ws_id = ? AND id_solicitud = ?",
            (ws_id, request_id)
        ).fetchone()
        volume_l = last[0] if last else 0
        # Duracion aproximada, desde que se pidio el riego, ya que no se guarda el instante exacto de inicio
        duration_s = int(time.time()) - created_ts
        if close_running_irrigation(db, request_id, ws_id, requester, "ABORTADO", end_reason, volume_l, duration_s):
            closed += 1
            # Solo si lo cerro este hilo, asi no se manda la orden dos veces
            send_stop_order(ws_id, request_id, end_reason)
    return closed

# Cierra TODOS los riegos abiertos de una estacion. Los PENDIENTE se deniegan y los EN_CURSO se abortan con su resumen
def abort_station_irrigations(db, ws_id, end_reason, pending_reason="La estacion tiene una averia"):
    pending = db.execute(
        "SELECT id_solicitud, ws_id, solicitante FROM riegos WHERE ws_id = ? AND estado = 'PENDIENTE'",
        (ws_id,)
    ).fetchall()
    running = db.execute(
        "SELECT id_solicitud, ws_id, solicitante, creado_ts FROM riegos WHERE ws_id = ? AND estado = 'EN_CURSO'",
        (ws_id,)
    ).fetchall()
    abort_pending_irrigations(db, pending, pending_reason, stop_reason=end_reason)
    abort_running_irrigations(db, running, end_reason)

# Si la estacion no confirma el inicio a tiempo, el riego se aborta para que la estacion no quede bloqueada
# El INICIAR_RIEGO que quedo en su origen caduca por su ts
def expire_pending_irrigations(db):
    limit = int(time.time()) - config.RESPONSE_TIMEOUT_S
    rows = db.execute(
        "SELECT id_solicitud, ws_id, solicitante FROM riegos WHERE estado = 'PENDIENTE' AND creado_ts < ?",
        (limit,)
    ).fetchall()
    abort_pending_irrigations(db, rows, "La estacion no respondio a tiempo")