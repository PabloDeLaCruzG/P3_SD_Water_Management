import time
import traceback
 
from WM_Central.central import database
from WM_Central.central import kafka_utils
 
TOPIC_STATIONS = "wm.estaciones"
INTERVAL_S = 2
 
 
def snapshot(db):
    rows = db.execute("SELECT id, ubicacion, status FROM stations ORDER BY ubicacion, id").fetchall()
    return [{"id": r[0], "ubicacion": r[1], "estado": r[2]} for r in rows]
 
 
def run_feed(interval_s=INTERVAL_S):
    db = database.get_db_connection()   # conexion propia de este hilo
    while True:
        try:
            # El productor se crea en el arranque de Central: hasta entonces no hay nada que enviar
            if kafka_utils.producer is not None:
                kafka_utils.publish_json(TOPIC_STATIONS, "ESTADO_ESTACIONES", {"estaciones": snapshot(db)})
        except Exception:
            print("[ESTACIONES] Error publicando la lista:")
            traceback.print_exc()
        time.sleep(interval_s)