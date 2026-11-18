# database.py, SQLite
#
# Tablas:
#   stations(id, ubicacion, status) -> estado de cada estacion (WS)
#   processed_messages(id_msg, ts) -> mensajes Kafka ya tratados (anti-duplicados)
#   riegos(id_solicitud, ws_id, solicitante, duracion_s, estado, creado_ts) -> cada peticion de riego aceptada y su estado 
#   telemetry_latest(ws_id, id_solicitud, caudal_lpm, volumen_l, ts) -> ultima medida de cada estacion que esta regando

import sqlite3
import time

from WM_Central.central import config


def get_db_connection():
    # Conexion por hilo: cada handle_client abre la suya y la mantiene mientras dure esa conexion de WS. 
    # WAL permite lecturas concurrentes sin bloquear al escritor
    conn = sqlite3.connect(config.DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    # Crea las tablas si no existen
    conn = get_db_connection()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            id TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'DESCONECTADA'
        )
    """)

    # Evita ejecutar dos veces un mensaje que Kafka entregue repetido
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_messages (
            id_msg TEXT PRIMARY KEY,
            ts INTEGER NOT NULL
        )
    """)

    # Los estados puden ser PENDIENTE, EN_CURSO, FINALIZADO, ABORTADO
    conn.execute("""
        CREATE TABLE IF NOT EXISTS riegos (
            id_solicitud TEXT PRIMARY KEY,
            ws_id TEXT NOT NULL,
            solicitante TEXT NOT NULL,
            duracion_s INTEGER NOT NULL,
            estado TEXT NOT NULL,
            creado_ts INTEGER NOT NULL
        )
    """)

    # Ultima medida recibida de cada estacion que esta regando
    # Es una tabla aparte para no tener que modificar "riegos" en BDs que ya existan
    conn.execute("""
        CREATE TABLE IF NOT EXISTS telemetry_latest (
            ws_id TEXT PRIMARY KEY,
            id_solicitud TEXT NOT NULL,
            caudal_lpm REAL NOT NULL,
            volumen_l REAL NOT NULL,
            ts INTEGER NOT NULL
        )
    """)

    # Pasada la ventana de MAX_AGE_S un duplicado ya se descartaria por ts
    # por eso guardar ids de hace mas de una hora no aporta nada
    conn.execute(
        "DELETE FROM processed_messages WHERE ts < ?",
        (int(time.time()) - 3600,)
    )
    conn.commit()
    conn.close()


def reset_stations_on_startup():
    # Al arrancar, Central no conoce el estado real de ninguna WS hasta que esta
    # vuelva a conectar, asi que se fuerza DESCONECTADA para todas las que haya en BD
    conn = get_db_connection()
    # Un bloqueo de Central sobrevive al reinicio
    conn.execute("UPDATE stations SET status = 'DESCONECTADA' WHERE status != 'FUERA_DE_SERVICIO'")

    # Un riego que estuviera pendiente cuando cayo Central ya no se puede dar por bueno
    # Si no se cierra, bloquearia la estacion
    conn.execute(
        "UPDATE riegos SET estado = 'ABORTADO' WHERE estado IN ('PENDIENTE', 'EN_CURSO')"
    )
    # Y sus ultimas medidas ya no valen
    conn.execute("DELETE FROM telemetry_latest")
    conn.commit()

    rows = conn.execute("SELECT id, ubicacion, status FROM stations").fetchall()
    conn.close()

    # Lista lo que hay en BD
    if rows:
        print("Estaciones conocidas en BD (estado real pendiente de reconexion):")
        for station_id, location, status in rows:
            print(f"  - {station_id} | {location} | {status}")
    else:
        print("No hay estaciones registradas todavia en BD")


# si Central se cae a mitad, ese mensaje no se repite, el operario puede volver a pedir el riego
def already_processed(db, id_msg):
    cur = db.execute(
        "INSERT OR IGNORE INTO processed_messages (id_msg, ts) VALUES (?, ?)",
        (id_msg, int(time.time()))
    )
    db.commit()
    return cur.rowcount == 0