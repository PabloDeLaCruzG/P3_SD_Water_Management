import argparse
import json
import random
import socket
import threading
import time
import uuid
 
from confluent_kafka import Consumer, Producer
 
HEADER = 64
FORMAT = 'utf-8'
RETRY_SECONDS = 2
 
TOPIC_REQ = "wm.central.solicitudes"
TOPIC_TELEMETRY = "wm.central.telemetria"
MAX_AGE_S = 30          # una orden mas vieja que esto se ignora (lo pide el protocolo)
 
# Estado simulado del hardware: False = todo OK, True = fallo (KO)
ko = False
blocked = False
ws_id = None
 
# Kafka
producer = None
kafka_broker = None
kafka_started = False
flow_lpm_base = 12.5
 
# Riego en marcha: None si la estacion esta en reposo
current = None                      # {"id": ..., "duracion_s": ..., "caudal": ..., "volumen": ...}
manual_stop = threading.Event()     # lo activa el menu para finalizar el riego en curso
handled_ids = set()                 # INICIAR_RIEGO ya tratados (Kafka puede entregar repetidos)
 
 
def parse_args():
    parser = argparse.ArgumentParser(description="WM_WS_E - Engine de la Watering Station")
    parser.add_argument("broker", type=str,
                        help="IP:puerto del broker/bootstrap-server de Kafka, p. ej. localhost:9092")
    parser.add_argument("monitor", type=str,
                        help="IP:puerto del WM_WS_M al que pertenece este Engine, p. ej. localhost:6000")
    parser.add_argument("--flow", type=float, default=12.5, help="caudal base en litros/minuto")
    return parser.parse_args()
 
 
#################################
## SOCKETS CON EL MONITOR (sin cambios respecto a la version anterior)
#################################

def send(conn, msg):
    data = msg.encode(FORMAT)
    header = str(len(data)).encode(FORMAT)
    header += b' ' * (HEADER - len(header))
    conn.sendall(header + data)
 
 
# Lee exactamente n bytes
def recv_exact(conn, n):
    data = b''
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data
 
 
def recv(conn):
    header = recv_exact(conn, HEADER)
    if header is None:
        return None
    length = int(header.decode(FORMAT).strip())
    body = recv_exact(conn, length)
    if body is None:
        return None
    return body.decode(FORMAT)
 
 
def connect_to_monitor(ip, port):
    # Intenta conectar hasta que el Monitor este disponible
    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((ip, port))
            return s
        except OSError:
            print(f"[Engine] Monitor no disponible, reintentando en {RETRY_SECONDS}s...")
            s.close()
            time.sleep(RETRY_SECONDS)
 
 
# Atiende los mensajes del Monitor hasta que se cierre la conexion
def handle_monitor(monitor):
    global ws_id
    while True:
        msg = recv(monitor)
 
        if msg is None:
            print("[Engine] El Monitor cerro la conexion")
            return
 
        if msg.startswith("ID#"):
            new_id = msg.split("#")[1]
            if ws_id is not None and new_id != ws_id:
                print(f"[Engine] AVISO: el Monitor ahora dice {new_id} pero este Engine ya usa {ws_id}; se mantiene {ws_id}")
            else:
                ws_id = new_id
                print(f"[Engine] ID asignado por el Monitor: {ws_id}")
                start_kafka()   # solo arranca la primera vez
 
        elif msg == "SALUD":
            send(monitor, "KO" if ko else "OK")
 
        else:
            print(f"[Engine] Mensaje desconocido: {msg}")
 
 
##################################################################
## KAFKA
 
def publish(topic, msg_type, payload, id_msg=None):
    # Envuelve el payload en el sobre JSON comun y lo envia. Devuelve el id_msg
    id_msg = id_msg or str(uuid.uuid4())
    envelope = {
        "tipo": msg_type,
        "id_msg": id_msg,
        "origen": f"WS:{ws_id}",
        "ts": int(time.time()),
        "payload": payload,
    }
    producer.produce(topic, value=json.dumps(envelope).encode(FORMAT))
    producer.poll(0)
    return id_msg
 
 
def parse(raw):
    # bytes -> dict, o None si no es un sobre valido o esta caducado
    try:
        msg = json.loads(raw.decode(FORMAT))
    except ValueError:
        return None
    if not isinstance(msg, dict) or not isinstance(msg.get("payload"), dict):
        return None
    ts = msg.get("ts")
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return None
    if time.time() - ts > MAX_AGE_S:
        print(f"[Engine] mensaje caducado ignorado: {msg.get('tipo')}")
        return None
    return msg
 
 
def show_status_message(payload):
    # ESTADO_SOLICITUD de Central: como va la peticion de riego manual de esta estacion
    text = payload.get("estado", "")
    if payload.get("motivo"):
        text += f" ({payload['motivo']})"
    print(f"[Engine] Central -> solicitud {payload.get('id_solicitud')}: {text}")
 

# Fuera de Servicio, lo ordena Central y solo lo quita ACTIVAR
def apply_block(value):
    global blocked
    blocked = value
    if blocked:
        print("[Engine] FUERA DE SERVICIO por orden de Central: no se admiten riegos")
    else:
        print("[Engine] ACTIVADA por orden de Central: vuelve a estar disponible")


# Mira un mensaje llegado durante un riego. Devuelve el motivo si es un DETENER_RIEGO de ESTE riego
def stop_reason_from(raw, request_id):
    msg = parse(raw)
    if msg is None:
        return None
    tipo = msg.get("tipo")
    payload = msg["payload"]
    if tipo == "ESTADO_SOLICITUD":
        show_status_message(payload)
        return None
    # Ordenes de estado de Central recibidas durante el riego
    if tipo == "BLOQUEAR":
        apply_block(True)
        return "BLOQUEO"
    if tipo == "ACTIVAR":
        apply_block(False)
        return None
    if tipo != "DETENER_RIEGO":
        print(f"[Engine] mensaje ignorado durante el riego: {tipo}")
        return None
    if payload.get("id_solicitud") != request_id:
        print(f"[Engine] DETENER_RIEGO de otro riego ({payload.get('id_solicitud')}), ignorado")
        return None
    return payload.get("motivo") or "ORDEN"
 
 
# Espera tick_s segundos (pausa entre medidas) escuchando el buzon, el teclado y el estado de salud
# Devuelve el motivo si hay que cortar el riego, o None
def wait_tick(consumer, request_id, tick_s):
    deadline = time.time() + tick_s
    while True:
        if ko:
            return "AVERIA"
        if manual_stop.is_set():
            return "MANUAL"
        remaining = deadline - time.time()
        if remaining <= 0:
            return None
        m = consumer.poll(min(remaining, 0.2))
        if m is None or m.error():
            continue
        reason = stop_reason_from(m.value(), request_id)
        if reason:
            return reason
 
 
def run_irrigation(consumer, request_id, duration_s, tick_s=1.0):
    global current
    manual_stop.clear()
    volume_l = 0.0
    seconds = 0
    end_reason = "TIEMPO"
    current = {"id": request_id, "duracion_s": duration_s, "caudal": 0.0, "volumen": 0.0}
    try:
        while seconds < duration_s:
            reason = wait_tick(consumer, request_id, tick_s)
            if reason:
                end_reason = reason
                print(f"[Engine] {request_id}: riego cortado ({reason})")
                break
            seconds += 1
            flow = round(flow_lpm_base + random.uniform(-0.5, 0.5), 1)
            volume_l += flow / 60       # litros/minuto -> litros en 1 segundo
            current["caudal"], current["volumen"] = flow, round(volume_l, 2)
            publish(TOPIC_TELEMETRY, "TELEMETRIA", {
                "ws_id": ws_id, "id_solicitud": request_id,
                "caudal_lpm": flow, "volumen_l": round(volume_l, 2),
            })
    finally:
        current = None
 
    # Mismo topic que la telemetria, asi llega despues de la ultima medida
    publish(TOPIC_TELEMETRY, "FIN_RIEGO", {
        "ws_id": ws_id, "id_solicitud": request_id, "motivo": end_reason,
        "volumen_total_l": round(volume_l, 2), "duracion_s": seconds,
    })
    producer.flush(5)
    print(f"[Engine] {request_id}: riego terminado ({end_reason}), {volume_l:.2f} L en {seconds}s")
 
 
def handle_start_order(consumer, payload):
    request_id = payload.get("id_solicitud")
    duration_s = payload.get("duracion_s")
    if (not isinstance(request_id, str) or not request_id or isinstance(duration_s, bool)
            or not isinstance(duration_s, int) or duration_s <= 0):
        print(f"[Engine] INICIAR_RIEGO mal formado: {payload}")
        return
    if request_id in handled_ids:
        print(f"[Engine] INICIAR_RIEGO repetido ignorado: {request_id}")
        return
    handled_ids.add(request_id)

    # Fuera de Servicio, se rechaza el inicio (segunda barrera, Central ya lo deniega)
    if blocked:
        publish(TOPIC_TELEMETRY, "RESPUESTA_INICIO", {
            "ws_id": ws_id, "id_solicitud": request_id,
            "aceptado": False, "motivo": "Estacion fuera de servicio",
        })
        producer.flush(5)
        print(f"[Engine] {request_id}: riego rechazado (fuera de servicio)")
        return
 
    # 1) Contestar si se puede empezar (Central espera la respuesta como maximo 30 s)
    if ko:
        publish(TOPIC_TELEMETRY, "RESPUESTA_INICIO", {
            "ws_id": ws_id, "id_solicitud": request_id,
            "aceptado": False, "motivo": "Estacion con averia",
        })
        producer.flush(5)
        print(f"[Engine] {request_id}: riego rechazado (averia)")
        return
    publish(TOPIC_TELEMETRY, "RESPUESTA_INICIO", {
        "ws_id": ws_id, "id_solicitud": request_id, "aceptado": True, "motivo": "",
    })
    print(f"[Engine] {request_id}: riego aceptado, regando {duration_s}s")
 
    # 2) Regar
    run_irrigation(consumer, request_id, duration_s)
 
 
def kafka_loop():
    topic = f"wm.ws.{ws_id}"
    consumer = Consumer({
        "bootstrap.servers": kafka_broker,
        "group.id": f"ws-{ws_id}",          # un lector por buzon
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": True,
    })
    consumer.subscribe([topic])
    print(f"[Engine] {ws_id} esperando ordenes en {topic}")
    try:
        while True:
            m = consumer.poll(1.0)
            if m is None or m.error():
                continue
            try:
                msg = parse(m.value())
                if msg is None:
                    continue
                tipo = msg.get("tipo")
                if tipo == "INICIAR_RIEGO":
                    handle_start_order(consumer, msg["payload"])
                elif tipo == "ESTADO_SOLICITUD":
                    show_status_message(msg["payload"])
                elif tipo == "BLOQUEAR":
                    apply_block(True)
                elif tipo == "ACTIVAR":
                    apply_block(False)
                elif tipo == "DETENER_RIEGO":
                    print("[Engine] DETENER_RIEGO sin riego en marcha, ignorado")
                else:
                    print(f"[Engine] mensaje ignorado: {tipo}")
            except Exception as e:
                # El Engine no debe dejar de leer por un mensaje que falle
                print(f"[Engine] Error tratando un mensaje: {e!r}")
    finally:
        consumer.close()
 
 
def start_kafka():
    global kafka_started
    if kafka_started:
        return
    kafka_started = True
    threading.Thread(target=kafka_loop, daemon=True).start()
 
 
#################################
## MENU DE LA ESTACION
################################# 
MENU = """
===== Menu de la estacion =====
 1) Alternar OK / KO (simula fuga)
 2) Solicitar riego (manual, via Central)
 3) Finalizar el riego en curso
 4) Ver estado
 (ENTER) volver a mostrar el menu
"""
 
 
def request_manual_irrigation():
    if ws_id is None:
        print("[Engine] Aun no tengo ID: el Monitor no se ha conectado")
        return
    if ko:
        print("[Engine] La estacion esta en KO, Central denegara el riego")
    if blocked:
        print("[Engine] La estacion esta FUERA DE SERVICIO, Central denegara el riego")
    try:
        duration_s = int(input("Duracion del riego en segundos: ").strip())
    except ValueError:
        print("[Engine] Duracion no valida")
        return
    if duration_s <= 0:
        print("[Engine] La duracion debe ser positiva")
        return
    # id_msg = id_solicitud: asi Central y la estacion hablan del mismo riego
    request_id = publish(TOPIC_REQ, "SOLICITUD_RIEGO", {"ws_id": ws_id, "duracion_s": duration_s})
    print(f"[Engine] Solicitud enviada a Central (id_solicitud={request_id})")
 
 
def show_state():
    print(f"[Engine] ID: {ws_id} | salud: {'KO' if ko else 'OK'} | {'FUERA DE SERVICIO' if blocked else 'operativa'}")
    c = current
    if c:
        print(f"[Engine] Regando {c['id']}: {c['caudal']} L/min | {c['volumen']} L | duracion {c['duracion_s']}s")
    else:
        print("[Engine] En reposo")
 
 
def menu_loop():
    global ko
    print(MENU)
    while True:
        try:
            option = input("> ").strip()
        except EOFError:
            print("[Engine] Sin teclado (stdin cerrado): menu desactivado. En Docker usa 'docker run -it'")
            return
        if option == "1":
            ko = not ko
            print(f"[Engine] Estado simulado -> {'KO (fuga simulada)' if ko else 'OK'}")
        elif option == "2":
            request_manual_irrigation()
        elif option == "3":
            if current is None:
                print("[Engine] No hay ningun riego en curso")
            else:
                manual_stop.set()
        elif option == "4":
            show_state()
        else:
            print(MENU)
 
 
#################################
## MAIN
#################################
 
if __name__ == "__main__":
    args = parse_args()
 
    kafka_broker = args.broker
    flow_lpm_base = args.flow
    mon_ip, mon_port = args.monitor.split(":")
    mon_port = int(mon_port)
 
    producer = Producer({"bootstrap.servers": kafka_broker})
    threading.Thread(target=menu_loop, daemon=True).start()
 
    try:
        while True:
            monitor = connect_to_monitor(mon_ip, mon_port)
            print(f"[Engine] Conectado al Monitor en {mon_ip}:{mon_port}")
            try:
                handle_monitor(monitor)
            except OSError:
                print("[Engine] Conexion con el Monitor perdida")
            finally:
                monitor.close()
 
            print(f"[Engine] Reconectando en {RETRY_SECONDS}s...")
            time.sleep(RETRY_SECONDS)
    except KeyboardInterrupt:
        print("\n[Engine] Cerrado")