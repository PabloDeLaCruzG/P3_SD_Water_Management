import socket
import time
import argparse
import select

HEADER = 64
FORMAT = 'utf-8'
HEALTH_INTERVAL = 1   # segundos entre comprobaciones de salud
ENGINE_TIMEOUT = 2    # segundos que se espera la respuesta del Engine

def parse_args():
    parser = argparse.ArgumentParser(
        description="WM_WS_M - Monitor de la Watering Station"
    )
    parser.add_argument(
        "Port_Engine", 
        type=int,
        help="Puerto en el que escucha al Engine (WM_WS_E)")

    parser.add_argument(
        "Port_Central", 
        type=int,
        help="Puerto de la Central a la que se comunica (WM_Central)")

    parser.add_argument(
        "IP_Central", 
        type=str,
        help="IP de la Central a la que comunica (WM_Central)")

    parser.add_argument(
        "WS_ID", 
        type=str,
        help="Identificador único de la Watering Station")

    parser.add_argument(
        "WS_Location", 
        type=str,
        help="Ubicación de la Watering Station")
    
    return parser.parse_args()


def send(conn, msg):
    data = msg.encode(FORMAT)
    header = str(len(data)).encode(FORMAT)
    header += b' ' * (HEADER - len(header))
    conn.sendall(header + data)
 
# Lee exactamente n bytes (recv puede devolver menos de los pedidos)
def recv_exact(conn, n):
    data = b''
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            return None  # el otro extremo cerro la conexion
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

# Envia REGISTRO y devuelve True si Central lo acepta
def register_in_central(central, ws_id, location):
    send(central, f"REGISTRO#{ws_id}#{location}")
    response = central.recv(2048).decode(FORMAT)
    print(f"[Monitor] Central responde: {response}")
    return response.startswith("STATUS#OK")

 
# Conexion con Central que se rehace sola si Central se cae o se reinicia
class CentralLink:
    RETRY_INTERVAL = 3   # segundos minimos entre intentos de reconexion

    def __init__(self, ip, port, ws_id, location):
        self.ip = ip
        self.port = port
        self.ws_id = ws_id
        self.location = location
        self.sock = None
        self.reconnected = False   # True tras una reconexion, lo consume engine_handler
        self.last_try = 0

    # Abre la conexion y registra la estacion. True si Central acepta
    def connect(self):
        self.close()
        self.last_try = time.time()
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((self.ip, self.port))
            print(f"[Monitor] Conectado a Central en {self.ip}:{self.port}")
            if not register_in_central(sock, self.ws_id, self.location):
                sock.close()
                return False
        except OSError:
            return False
        self.sock = sock
        return True

    # True si la conexion sigue abierta (si Central la cierra, el socket queda legible con 0 bytes)
    def alive(self):
        if self.sock is None:
            return False
        try:
            readable, _, _ = select.select([self.sock], [], [], 0)
            if readable and not self.sock.recv(1, socket.MSG_PEEK):
                return False
            return True
        except OSError:
            return False

    # Comprueba la conexion y, si esta rota, la rehace (como mucho cada RETRY_INTERVAL s)
    def ensure(self):
        if self.alive():
            return True
        if time.time() - self.last_try < self.RETRY_INTERVAL:
            return False
        print("[Monitor] Conexion con Central perdida, reintentando...")
        if self.connect():
            self.reconnected = True
            print("[Monitor] Reconectado y registrado de nuevo en Central")
            return True
        return False

    # Envia un mensaje a Central y lee su respuesta. True si Central contesto
    def notify(self, msg):
        if not self.ensure():
            return False
        try:
            print(f"[Monitor] -> Central: {msg}")
            send(self.sock, msg)
            response = self.sock.recv(2048).decode(FORMAT)
        except OSError:
            self.close()
            return False
        if not response:   # Central cerro la conexion
            self.close()
            return False
        print(f"[Monitor] <- Central: {response}")
        return True

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

# Le dice a Engine su id y hace el bucle para ver su estado continuamente
def engine_handler(engine, link, id_ws, en_averia):

    engine.settimeout(ENGINE_TIMEOUT)
    send(engine, f"ID#{id_ws}")
    last_print = 0
 
    while True:
        connection_lost = False
        resp = None
 
        try:
            send(engine, "SALUD")
            resp = recv(engine)
            if resp is None:
                connection_lost = True
            # no respondio a tiempo
        except socket.timeout:
            resp = None
            # conexion rota
        except OSError:
            connection_lost = True

        # Latido visible, una linea cada 10 s con el estado de salud
        if time.time() - last_print >= 10:
            print(f"[Monitor] {id_ws}: Engine {'OK' if resp == 'OK' else 'KO'} (comprobacion cada {HEALTH_INTERVAL}s)")
            last_print = time.time()
 
        # Mantiene la conexion con Central y repite la averia si Central se reinicio
        link.ensure()
        if link.reconnected:
            link.reconnected = False
            if en_averia:
                link.notify(f"AVERIA#{id_ws}#DETECTADA")

        # El cambio de estado solo cuenta si Central recibio el aviso
        if resp != "OK" and not en_averia:
            if link.notify(f"AVERIA#{id_ws}#DETECTADA"):
                en_averia = True
        elif resp == "OK" and en_averia:
            if link.notify(f"AVERIA#{id_ws}#RESUELTA"):
                en_averia = False
 
        if connection_lost:
            print("[Monitor] Conexion con el Engine perdida")
            return en_averia
 
        time.sleep(HEALTH_INTERVAL)



########## MAIN ##########
if __name__ == "__main__":
    args = parse_args()
    print("Water Station Monitor")
 
    # Conectar con CENTRAL y registrar la estacion
    link = CentralLink(args.IP_Central, args.Port_Central, args.WS_ID, args.WS_Location)
    if not link.connect():
        print("[Monitor] No se pudo conectar con Central o rechazo el registro")
        raise SystemExit(1)
 
    # Abrir el servidor para el Engine
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.Port_Engine))
    server.listen(1)
    server.settimeout(1.0) 
    print(f"[Monitor] Esperando al Engine en el puerto {args.Port_Engine}...")
 
    # Aceptar al Engine y vigilarlo (si se cae, se espera a otro)
    en_averia = False
    try:
        while True:
            try:
                engine, addr = server.accept()
            except socket.timeout:
                link.ensure()   # Tambien se reconecta mientras no hay Engine
                continue
 
            print(f"[Monitor] Engine conectado desde {addr}")
            en_averia = engine_handler(engine, link, args.WS_ID, en_averia)
            engine.close()
            print("[Monitor] Esperando de nuevo al Engine...")
    except KeyboardInterrupt:
        print("\n[Monitor] Cerrando...")
    finally:
        server.close()
        link.close()