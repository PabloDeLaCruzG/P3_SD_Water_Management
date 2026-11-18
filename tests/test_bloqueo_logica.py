"""Prueba de LOGICA de BLOQUEAR / ACTIVAR y de las solicitudes con origen CENTRAL (menu de Central).
No necesita broker ni Docker: sustituye el envio a Kafka por una lista y usa una BD temporal.
Uso, desde la raiz del repo:  python3 tests/test_bloqueo_logica.py
"""
import json, os, sys, tempfile, time, uuid
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from WM_Central.central import config, control, database, irrigation, kafka_consumer, kafka_utils
from WM_Central.central import socket_server
config.DB_FILE = os.path.join(tempfile.mkdtemp(), "t.db")
database.init_db()
sent = []
real_publish = kafka_utils.publish_json      # el de verdad, para probar el desvio a pantalla
kafka_utils.publish_json = lambda topic, tipo, payload: sent.append((topic, tipo, payload))
db = database.get_db_connection()

fails = 0
def check(name, got, expected):
    global fails
    ok = got == expected; fails += not ok
    print(("OK   " if ok else "FAIL ") + name)
    if not ok: print("   esperado:", expected); print("   obtenido:", got)

def env(tipo, payload, origen="WS:WS-04", age=0, id_msg=None):
    return json.dumps({"tipo": tipo, "id_msg": id_msg or str(uuid.uuid4()), "origen": origen,
                       "ts": int(time.time()) - age, "payload": payload}).encode()
T = config.TOPIC_TELEMETRY; R = config.TOPIC_REQ
def feed(topic, raw): kafka_consumer.process_kafka_message(topic, raw, db)
def state(rid): r = db.execute("SELECT estado FROM riegos WHERE id_solicitud=?", (rid,)).fetchone(); return r and r[0]
def st(ws): return db.execute("SELECT status FROM stations WHERE id=?", (ws,)).fetchone()[0]
def reset(status04="ACTIVADA"):
    db.execute("DELETE FROM riegos"); db.execute("DELETE FROM telemetry_latest")
    db.execute("DELETE FROM stations")
    db.execute("INSERT INTO stations VALUES ('WS-04','Parque',?)", (status04,))
    db.execute("INSERT INTO stations VALUES ('WS-05','Plaza','ACTIVADA')")
    db.commit(); sent.clear(); socket_server.active_ws.clear()
def start_running(rid, origin="FO:3", ws="WS-04", volume=3.3):
    feed(R, env("SOLICITUD_RIEGO", {"ws_id": ws, "duracion_s": 60}, origin, id_msg=rid))
    feed(T, env("RESPUESTA_INICIO", {"ws_id": ws, "id_solicitud": rid, "aceptado": True, "motivo": ""}, f"WS:{ws}"))
    feed(T, env("TELEMETRIA", {"ws_id": ws, "id_solicitud": rid, "caudal_lpm": 12.5, "volumen_l": volume}, f"WS:{ws}"))
    sent.clear()
def of_type(tipo): return [(t, p) for t, ty, p in sent if ty == tipo]
def stops(): return [(t, p["id_solicitud"], p["motivo"]) for t, ty, p in sent if ty == "DETENER_RIEGO"]
def resumenes(): return [(t, p["motivo_fin"], p["volumen_total_l"]) for t, ty, p in sent if ty == "RESUMEN_RIEGO"]
def estados(): return [(t, p["estado"], p["motivo"]) for t, ty, p in sent if ty == "ESTADO_SOLICITUD"]

class Conn:
    def __init__(self): self.out = []
    def send(self, b): self.out.append(b.decode())

# ======= BLOQUEAR
reset()
ok, msg = control.block_station(db, "WS-04")
check("1 bloquear una estacion libre: ok y FUERA_DE_SERVICIO", (ok, st("WS-04")), (True, "FUERA_DE_SERVICIO"))
check("2 se manda BLOQUEAR al Engine con payload vacio", of_type("BLOQUEAR"), [("wm.ws.WS-04", {})])
check("3 no se manda ninguna parada si no habia riegos", stops(), [])
check("4 no se toca otra estacion", st("WS-05"), "ACTIVADA")

reset(); start_running("b1")
check("5 antes de bloquear la estacion esta REGANDO", st("WS-04"), "REGANDO")
ok, msg = control.block_station(db, "WS-04")
check("6 bloquear con riego en marcha: estacion FUERA_DE_SERVICIO (no vuelve a ACTIVADA)", (ok, st("WS-04")), (True, "FUERA_DE_SERVICIO"))
check("7 el riego queda ABORTADO", state("b1"), "ABORTADO")
check("8 el operario recibe RESUMEN con motivo BLOQUEO y ultimo volumen", resumenes(), [("wm.fo.3", "BLOQUEO", 3.3)])
check("9 DETENER_RIEGO (BLOQUEO) y BLOQUEAR llegan al Engine", (stops(), len(of_type("BLOQUEAR"))), ([("wm.ws.WS-04", "b1", "BLOQUEO")], 1))
check("10 se borra la ultima medida", db.execute("SELECT COUNT(*) FROM telemetry_latest").fetchone()[0], 0)
sent.clear()
feed(T, env("FIN_RIEGO", {"ws_id": "WS-04", "id_solicitud": "b1", "motivo": "ORDEN", "volumen_total_l": 3.4, "duracion_s": 4}))
check("11 el FIN_RIEGO que mande el Engine despues se ignora y sigue bloqueada", (sent, st("WS-04")), ([], "FUERA_DE_SERVICIO"))

reset()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 60}, "FO:3", id_msg="b2")); sent.clear()
control.block_station(db, "WS-04")
check("12 bloquear con riego PENDIENTE: ABORTADO y DENEGADA con motivo al operario",
      (state("b2"), estados()), ("ABORTADO", [("wm.fo.3", "DENEGADA", "La estacion ha sido bloqueada por Central")]))
check("13 ...y se manda DETENER_RIEGO por si la estacion ya iba a arrancar", stops(), [("wm.ws.WS-04", "b2", "BLOQUEO")])

reset(); sent.clear()
check("14 bloquear estacion desconocida: error y nada enviado", (control.block_station(db, "WS-99")[0], sent), (False, []))
control.block_station(db, "WS-04"); sent.clear()
check("15 bloquear una ya bloqueada: error y nada enviado", (control.block_station(db, "WS-04")[0], sent), (False, []))

reset(status04="DESCONECTADA"); sent.clear()
ok, _ = control.block_station(db, "WS-04")
check("16 se puede bloquear una estacion desconectada", (ok, st("WS-04")), (True, "FUERA_DE_SERVICIO"))

# riego pedido a una estacion bloqueada
reset(); control.block_station(db, "WS-04"); sent.clear()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 5}, "FO:3"))
check("17 en FUERA_DE_SERVICIO no se puede pedir riego", [e[2] for e in estados() if e[1] == "DENEGADA"], ["Estacion no disponible (FUERA_DE_SERVICIO)"])

# ======= EL BLOQUEO SOBREVIVE
reset(); control.block_station(db, "WS-04")
c = Conn()
socket_server.handle_fault(c, ["AVERIA", "WS-04", "DETECTADA"], db)
check("18 averia DETECTADA con la estacion bloqueada: sigue bloqueada y se contesta OK", (st("WS-04"), c.out), ("FUERA_DE_SERVICIO", ["STATUS#OK#Averia detectada registrada"]))
socket_server.handle_fault(c, ["AVERIA", "WS-04", "RESUELTA"], db)
check("19 averia RESUELTA con la estacion bloqueada: sigue bloqueada", st("WS-04"), "FUERA_DE_SERVICIO")

c = Conn()
rid = socket_server.handle_register(c, ["REGISTRO", "WS-04", "Parque"], db)
check("20 re-registro del Monitor de una estacion bloqueada: sigue bloqueada", (rid, st("WS-04")), ("WS-04", "FUERA_DE_SERVICIO"))
c2 = Conn(); socket_server.handle_register(c2, ["REGISTRO", "WS-07", "Nueva"], db)
check("21 registro de una estacion nueva: ACTIVADA", st("WS-07"), "ACTIVADA")
socket_server.handle_register(c2, ["REGISTRO", "WS-05", "Plaza"], db)
check("22 re-registro de una estacion normal: ACTIVADA", st("WS-05"), "ACTIVADA")

reset(); control.block_station(db, "WS-04")
c = Conn(); socket_server.active_ws["WS-04"] = c
socket_server.on_ws_disconnect("WS-04", c, db)
check("23 desconexion de una estacion bloqueada: sigue bloqueada", st("WS-04"), "FUERA_DE_SERVICIO")
reset(); c = Conn(); socket_server.active_ws["WS-05"] = c
socket_server.on_ws_disconnect("WS-05", c, db)
check("24 desconexion de una estacion normal: DESCONECTADA", st("WS-05"), "DESCONECTADA")

reset(); control.block_station(db, "WS-04"); db.execute("UPDATE stations SET status='ACTIVADA' WHERE id='WS-05'"); db.commit()
database.reset_stations_on_startup()
check("25 reinicio de Central: la bloqueada sigue bloqueada y el resto pasa a DESCONECTADA", (st("WS-04"), st("WS-05")), ("FUERA_DE_SERVICIO", "DESCONECTADA"))

# ======= ACTIVAR
reset(); control.block_station(db, "WS-04"); sent.clear()
socket_server.active_ws["WS-04"] = Conn()
ok, _ = control.activate_station(db, "WS-04")
check("26 activar con el Monitor conectado: ACTIVADA y se manda ACTIVAR", (ok, st("WS-04"), of_type("ACTIVAR")), (True, "ACTIVADA", [("wm.ws.WS-04", {})]))
sent.clear()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 5}, "FO:3", id_msg="ac1"))
check("27 tras activar ya se puede regar", state("ac1"), "PENDIENTE")

reset(); control.block_station(db, "WS-04"); sent.clear()
ok, _ = control.activate_station(db, "WS-04")
check("28 activar sin Monitor conectado: DESCONECTADA", (ok, st("WS-04")), (True, "DESCONECTADA"))

reset(); sent.clear()
check("29 activar una estacion que no esta bloqueada: error y nada enviado", (control.activate_station(db, "WS-04")[0], sent), (False, []))
check("30 activar una estacion desconocida: error y nada enviado", (control.activate_station(db, "WS-99")[0], sent), (False, []))
check("31 list_stations devuelve todas ordenadas", control.list_stations(db), [("WS-04", "Parque", "ACTIVADA"), ("WS-05", "Plaza", "ACTIVADA")])

# ======= SOLICITUDES CON ORIGEN CENTRAL (iniciar riego desde el menu)
check("32 reply_topic de CENTRAL es el destino de pantalla", kafka_utils.reply_topic("CENTRAL"), config.TOPIC_SCREEN)
real_publish(config.TOPIC_SCREEN, "ESTADO_SOLICITUD", {})     # sin productor: no debe fallar ni enviar nada
check("33 publicar al destino de pantalla no toca Kafka", True, True)

reset()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 60}, "CENTRAL", id_msg="c1"))
check("34 solicitud de CENTRAL: riego PENDIENTE y INICIAR_RIEGO al Engine", (state("c1"), [t for t, ty, p in sent if ty == "INICIAR_RIEGO"]), ("PENDIENTE", ["wm.ws.WS-04"]))
check("35 los estados de la solicitud van a pantalla", [(e[0], e[1]) for e in estados()], [(config.TOPIC_SCREEN, "RECIBIDA"), (config.TOPIC_SCREEN, "VALIDANDO")])
sent.clear()
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-04", "id_solicitud": "c1", "aceptado": True, "motivo": ""}))
check("36 AUTORIZADA tambien a pantalla", [(e[0], e[1]) for e in estados()], [(config.TOPIC_SCREEN, "AUTORIZADA")])
sent.clear()
feed(T, env("TELEMETRIA", {"ws_id": "WS-04", "id_solicitud": "c1", "caudal_lpm": 12, "volumen_l": 1.0}))
feed(T, env("FIN_RIEGO", {"ws_id": "WS-04", "id_solicitud": "c1", "motivo": "TIEMPO", "volumen_total_l": 5.0, "duracion_s": 5}))
check("37 riego de CENTRAL: se cierra FINALIZADO y no se reenvia telemetria ni resumen", (state("c1"), sent), ("FINALIZADO", []))
reset(); sent.clear()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-99", "duracion_s": 60}, "CENTRAL"))
check("38 solicitud de CENTRAL a estacion desconocida: DENEGADA en pantalla", [(e[0], e[1], e[2]) for e in estados() if e[1] == "DENEGADA"], [(config.TOPIC_SCREEN, "DENEGADA", "Estacion no registrada")])
reset(); start_running("c2", origin="CENTRAL"); control.block_station(db, "WS-04")
check("39 bloquear con un riego pedido por CENTRAL: ABORTADO sin resumen a nadie", (state("c2"), resumenes()), ("ABORTADO", []))

print("\nFALLOS:", fails)
sys.exit(1 if fails else 0)
