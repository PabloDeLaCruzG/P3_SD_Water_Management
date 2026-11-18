# socket_server.py , Servidor de sockets que atiende a los Monitories
#
# Cada mensaje va precedido de una cabecera de HEADER bytes con su longitud
# El contenido es "TIPO#campo#campo". Tipos: REGISTRO, SALUD, AVERIA
# Cada Monitor mantiene una conexion persistente y cada conexion se atiende en un hilo

import socket
import threading
import traceback

from WM_Central.central import config
from WM_Central.central import database
from WM_Central.central import irrigation

# ws_id -> socket de su Monitor. Dice cual es la conexion activa de cada estacion.
# Lo tocan varios hilos, por eso va acompañado de un lock.
active_ws = {}
active_ws_lock = threading.Lock()


##################################################################
## Handle de cada tipo de trama

# Trama: REGISTRO#id#ubicacion
# Devuelve el id de la estacion si se registra bien, y None en cualquier otro caso
def handle_register(conn, args, db):
    if len(args) != 3:
        conn.send("STATUS#ERROR#Se esperan 3 argumentos".encode(config.FORMAT))
        return

    _, station_id, location = args
    print(f"Station connected -> ID: {station_id}, Location: {location}")

    if station_id and location:
        # Alta, o actualizacion si la estacion ya existia
        db.execute(
            """
            INSERT INTO stations (id, ubicacion, status)
            VALUES (?, ?, 'ACTIVADA')
            ON CONFLICT(id) DO UPDATE SET
                ubicacion = excluded.ubicacion,
                status = CASE WHEN stations.status = 'FUERA_DE_SERVICIO'
                              THEN 'FUERA_DE_SERVICIO' ELSE 'ACTIVADA' END
            """,
            (station_id, location)
        )
        db.commit()
        conn.send("STATUS#OK#Estacion registrada correctamente".encode(config.FORMAT))
        return station_id
    else:
        # Algun campo vacio
        conn.send("STATUS#ERROR#Estacion NO registrada".encode(config.FORMAT))


# Trama: SALUD#id#estado
def handle_health(conn, args, db):
    if len(args) != 3:
        conn.send("STATUS#ERROR#Se esperan 3 argumentos".encode(config.FORMAT))
        return

    _, station_id, status = args
    print(f"[SALUD] {station_id} -> {status}")
    conn.send("STATUS#OK#Salud recibida".encode(config.FORMAT))


# Trama: AVERIA#id#DETECTADA|RESUELTA
def handle_fault(conn, args, db):
    if len(args) != 3:
        conn.send("STATUS#ERROR#Se esperan 3 argumentos".encode(config.FORMAT))
        return

    _, station_id, event = args

    if event not in ("DETECTADA", "RESUELTA"):   # [MODIFICADO]
        conn.send(f"STATUS#ERROR#Evento de averia desconocido: {event}".encode(config.FORMAT))
        return

    row = db.execute("SELECT status FROM stations WHERE id = ?", (station_id,)).fetchone()   # [NUEVO]
    if row is None:
        # La WS manda averia pero nunca hizo registro, osea que no existe en BD
        print(f"[AVERIA] ID desconocido: {station_id}")
        conn.send(f"STATUS#ERROR#Estacion {station_id} no registrada".encode(config.FORMAT))
        return

    # [NUEVO] El evento decide el nuevo estado de la estacion, salvo que Central la tenga bloqueada:
    # un bloqueo es una orden de Central y solo lo quita ACTIVAR
    if row[0] == "FUERA_DE_SERVICIO":
        print(f"[AVERIA] {station_id} {event}: sigue FUERA_DE_SERVICIO (bloqueada por Central)")
    else:
        new_status = "FUGA" if event == "DETECTADA" else "ACTIVADA"
        db.execute("UPDATE stations SET status = ? WHERE id = ?", (new_status, station_id))
        db.commit()
        print(f"[AVERIA] {station_id} -> {new_status}")

    conn.send(f"STATUS#OK#Averia {event.lower()} registrada".encode(config.FORMAT))

    # Con una averia no se puede seguir regando asi que se cierran los riegos de la estacion
    if event == "DETECTADA":
        irrigation.abort_station_irrigations(db, station_id, "AVERIA")


##################################################################
## DESCONEXION

# Se llama cuando se cierra el socket de una estacion registrada, la pone a DESCONECTADA y aborta sus riegos para que no queden colgados
def on_ws_disconnect(ws_id, conn, db):
    
    # Si el Monitor ya se ha vuelto a registrar por otro socket, esta conexion antigua ya no es la vigente y no debe marcar la estacion como desconectada
    with active_ws_lock:
        if active_ws.get(ws_id) is not conn:
            return
        del active_ws[ws_id]
        db.execute("UPDATE stations SET status = 'DESCONECTADA' WHERE id = ? AND status != 'FUERA_DE_SERVICIO'", (ws_id,))
        # Los PENDIENTE se leen primero porque hay que avisar a su solicitante
        pending = db.execute(
            "SELECT id_solicitud, ws_id, solicitante FROM riegos WHERE ws_id = ? AND estado = 'PENDIENTE'",
            (ws_id,)
        ).fetchall()
        running = db.execute(
            "SELECT id_solicitud, ws_id, solicitante, creado_ts FROM riegos WHERE ws_id = ? AND estado = 'EN_CURSO'",
            (ws_id,)
        ).fetchall()
        db.commit()

    print(f"[WS] {ws_id} desconectada")
    
    # Publicar en Kafka no necesita tenerlo y asi no se bloquea a otros hilos
    irrigation.abort_pending_irrigations(db, pending, "La estacion se desconecto")
    irrigation.abort_running_irrigations(db, running, "DESCONEXION")

##################################################################
## HILO DE CADA CLIENTE

# Lee exactamente n bytes, devuelve None si el otro extremo cierra la conexion
def recv_exact(conn, n):
    data = b''
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data

# Atiende a un Monitor durante toda su conexion, se ejecuta en su propio hilo
def handle_client(conn, addr):
    print(f"NEW connection from {addr}")
    connected = True
    ws_id = None  # estacion que se ha registrado por esta conexion, si llega a hacerlo

    db = database.get_db_connection()  # conexion SQLite propia de este hilo

    try:
        while connected:
            # Lee la cabecera
            header = recv_exact(conn, config.HEADER)
            if header is None:
                connected = False
                break

            msg_length = int(header.decode(config.FORMAT)) # int() ignora los espacios de relleno
            body = recv_exact(conn, msg_length)
            if body is None: 
                connected = False
                break
            msg = body.decode(config.FORMAT)

            print(f"Received Message from {addr}: {msg}")

            # Trocear "TIPO#campo#campo"
            args = msg.split("#")
            frame_type = args[0]

            if frame_type == "REGISTRO":
                # Registro y anotacion de la conexion vigente van juntos bajo el mismo lock que usa on_ws_disconnect,
                # para que un Monitor que se reconecta no sea marcado como desconectado por su conexion antigua
                with active_ws_lock:
                    registered = handle_register(conn, args, db)
                    if registered:
                        active_ws[registered] = conn
                        ws_id = registered
            elif frame_type == "SALUD":
                handle_health(conn, args, db)
            elif frame_type == "AVERIA":
                handle_fault(conn, args, db)
            else:
                conn.send(f"STATUS#ERROR#Tipo de trama desconocido: {frame_type}".encode(config.FORMAT))

    except ConnectionResetError:
        print(f"Connection with {addr} reset abruptly")
    except (OSError, ValueError) as e:
        print(f"Connection with {addr} failed: {e}")
    finally:
        print("Closing connection")
        if ws_id:
            try:
                on_ws_disconnect(ws_id, conn, db)
            except Exception:
                traceback.print_exc()
        db.close()
        conn.close()


##################################################################
## ARRANQUE DEL SERVIDOR

# Crea el socket de escucha, devuelve (server, addr)
def create_server(port):
    host = "0.0.0.0" # escuchar en todas las interfaces
    addr = (host, port)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)    # reiniciar sin "address already in use"
    server.bind(addr)
    return server, addr


# Bucle principal, acepta conexiones y lanza un hilo por cada una
def run_server(server, addr):
    server.listen()
    print(f"{addr[0]} listening")

    while True:
        conn, client_addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, client_addr))
        thread.start()
        print("Active connections: ", threading.active_count())