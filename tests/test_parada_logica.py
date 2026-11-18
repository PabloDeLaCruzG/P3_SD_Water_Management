"""Prueba de LOGICA de DETENER_RIEGO y averia durante un riego (50 casos).
No necesita broker ni Docker: sustituye el envio a Kafka por una lista y usa una BD temporal.
Uso, desde la raiz del repo:  python3 tests/test_parada_logica.py
"""
import json, os, sys, tempfile, time, uuid
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from WM_Central.central import config, database, irrigation, kafka_consumer, kafka_utils
from WM_Central.central import socket_server
config.DB_FILE = os.path.join(tempfile.mkdtemp(), "t.db")
database.init_db()
sent = []
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
def reset():
    db.execute("DELETE FROM riegos"); db.execute("DELETE FROM telemetry_latest")
    db.execute("DELETE FROM stations")
    db.execute("INSERT INTO stations VALUES ('WS-04','Parque','ACTIVADA')")
    db.execute("INSERT INTO stations VALUES ('WS-05','Plaza','ACTIVADA')")
    db.commit(); sent.clear()
def start_running(rid, origin="FO:3", ws="WS-04", volume=3.3):
    # lleva un riego hasta EN_CURSO con una medida
    feed(R, env("SOLICITUD_RIEGO", {"ws_id": ws, "duracion_s": 60}, origin, id_msg=rid))
    feed(T, env("RESPUESTA_INICIO", {"ws_id": ws, "id_solicitud": rid, "aceptado": True, "motivo": ""}, f"WS:{ws}"))
    feed(T, env("TELEMETRIA", {"ws_id": ws, "id_solicitud": rid, "caudal_lpm": 12.5, "volumen_l": volume}, f"WS:{ws}"))
    sent.clear()
def stop(rid, ws="WS-04", origin="FO:3", **kw): feed(R, env("DETENER_RIEGO", {"id_solicitud": rid, "ws_id": ws}, origin, **kw))
def stops(): return [(t, p["id_solicitud"], p["motivo"]) for t, ty, p in sent if ty == "DETENER_RIEGO"]
def resumenes(): return [(t, p["motivo_fin"], p["volumen_total_l"]) for t, ty, p in sent if ty == "RESUMEN_RIEGO"]
def estados(): return [(t, p["estado"]) for t, ty, p in sent if ty == "ESTADO_SOLICITUD"]

class Conn:
    def __init__(self): self.out = []
    def send(self, b): self.out.append(b.decode())

# ======= PARADA PEDIDA POR EL OPERARIO
reset(); start_running("r1")
stop("r1")
check("1 parada de riego EN_CURSO -> ABORTADO", state("r1"), "ABORTADO")
check("2 la estacion vuelve a ACTIVADA", st("WS-04"), "ACTIVADA")
check("3 el operario recibe RESUMEN con motivo ORDEN y ultimo volumen", resumenes(), [("wm.fo.3", "ORDEN", 3.3)])
check("4 se ordena parar al Engine, con id y motivo", stops(), [("wm.ws.WS-04", "r1", "ORDEN")])
check("5 el solicitante no recibe DETENIDA extra", estados(), [])
check("6 se borra la ultima medida", db.execute("SELECT COUNT(*) FROM telemetry_latest").fetchone()[0], 0)

sent.clear()
feed(T, env("FIN_RIEGO", {"ws_id": "WS-04", "id_solicitud": "r1", "motivo": "ORDEN", "volumen_total_l": 3.4, "duracion_s": 4}))
feed(T, env("TELEMETRIA", {"ws_id": "WS-04", "id_solicitud": "r1", "caudal_lpm": 12, "volumen_l": 3.5}))
check("7 FIN_RIEGO y telemetria tardios del Engine no duplican nada", sent, [])

sent.clear()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 10}, "FO:3", id_msg="r1b"))
check("8 la estacion queda libre: nueva solicitud pasa a PENDIENTE", state("r1b"), "PENDIENTE")

# parada desde OTRO operario
reset(); start_running("r2")
stop("r2", origin="FO:9")
check("9 otro operario para: el solicitante recibe su RESUMEN", resumenes(), [("wm.fo.3", "ORDEN", 3.3)])
check("10 quien paro recibe DETENIDA", [e for e in estados() if e[0] == "wm.fo.9"], [("wm.fo.9", "DETENIDA")])
check("11 solo una orden al Engine", len(stops()), 1)

# parada desde la propia estacion que hizo la solicitud (origen WS)
reset(); start_running("r3", origin="WS:WS-04")
stop("r3", origin="WS:WS-04")
check("12 riego pedido por la WS: queda ABORTADO y no hay resumen a nadie", (state("r3"), resumenes()), ("ABORTADO", []))

# parada de riego PENDIENTE
reset()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 60}, "FO:3", id_msg="r4")); sent.clear()
stop("r4")
check("13 parada de PENDIENTE -> ABORTADO", state("r4"), "ABORTADO")
check("14 el solicitante recibe DENEGADA con el motivo", [(t, p["estado"], p["motivo"]) for t, ty, p in sent if ty == "ESTADO_SOLICITUD"],
      [("wm.fo.3", "DENEGADA", "Detenido por orden de FO:3")])
check("15 tambien se manda DETENER_RIEGO (por si la estacion ya iba a arrancar)", stops(), [("wm.ws.WS-04", "r4", "ORDEN")])
sent.clear()
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-04", "id_solicitud": "r4", "aceptado": True, "motivo": ""}))
check("16 si luego acepta, se vuelve a ordenar parar y el riego sigue ABORTADO", (state("r4"), len(stops())), ("ABORTADO", 1))

# casos denegados
reset(); start_running("r5")
feed(T, env("FIN_RIEGO", {"ws_id": "WS-04", "id_solicitud": "r5", "motivo": "TIEMPO", "volumen_total_l": 5.0, "duracion_s": 5})); sent.clear()
stop("r5")
check("17 parar un riego ya FINALIZADO se deniega", [(p["estado"], p["motivo"]) for t, ty, p in sent], [("DENEGADA", "El riego ya habia terminado (FINALIZADO)")])
check("18 ...y no se manda orden al Engine", stops(), [])
sent.clear(); stop("no-existe")
check("19 riego inexistente -> DENEGADA", [(p["estado"], p["motivo"]) for t, ty, p in sent], [("DENEGADA", "No existe ese riego en esa estacion")])
reset(); start_running("r6")
stop("r6", ws="WS-05")
check("20 riego de OTRA estacion -> DENEGADA y sigue EN_CURSO", (state("r6"), [e[1] for e in estados()], stops()), ("EN_CURSO", ["DENEGADA"], []))
sent.clear()
feed(R, env("DETENER_RIEGO", {"ws_id": "WS-04"}, "FO:3"))
check("21 payload mal formado -> DENEGADA", [(p["estado"], p["motivo"]) for t, ty, p in sent], [("DENEGADA", "Orden de parada mal formada")])
sent.clear()
feed(R, env("DETENER_RIEGO", {"id_solicitud": "r6", "ws_id": "WS-04"}, "XX:1"))
check("22 origen no valido: sin respuesta ni accion", (sent, state("r6")), ([], "EN_CURSO"))
sent.clear()
feed(R, env("DETENER_RIEGO", {"id_solicitud": "r6", "ws_id": "WS-04"}, "FO:3", age=100))
check("23 parada caducada se descarta", (sent, state("r6")), ([], "EN_CURSO"))
sent.clear()
feed(R, env("DETENER_RIEGO", {"id_solicitud": "r6", "ws_id": "WS-04"}, "FO:3", id_msg="stop-dup"))
n = len(stops()); feed(R, env("DETENER_RIEGO", {"id_solicitud": "r6", "ws_id": "WS-04"}, "FO:3", id_msg="stop-dup"))
check("24 el mismo mensaje de parada dos veces solo actua una", (n, len(stops())), (1, 1))

# DETENER_RIEGO en el topic de telemetria no es valido
reset(); start_running("r7")
feed(T, env("DETENER_RIEGO", {"id_solicitud": "r7", "ws_id": "WS-04"}))
check("25 DETENER_RIEGO llegado por el topic de telemetria se ignora", (state("r7"), sent), ("EN_CURSO", []))

# ======= AVERIA
reset(); start_running("a1")
conn = Conn()
socket_server.handle_fault(conn, ["AVERIA", "WS-04", "DETECTADA"], db)
check("26 averia: la estacion queda en FUGA (no ACTIVADA)", st("WS-04"), "FUGA")
check("27 averia: el riego EN_CURSO queda ABORTADO", state("a1"), "ABORTADO")
check("28 averia: el operario recibe RESUMEN con motivo AVERIA", resumenes(), [("wm.fo.3", "AVERIA", 3.3)])
check("29 averia: se ordena parar al Engine con motivo AVERIA", stops(), [("wm.ws.WS-04", "a1", "AVERIA")])
check("30 averia: respuesta OK por el socket", conn.out, ["STATUS#OK#Averia detectada registrada"])
sent.clear()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 5}, "FO:3"))
check("31 en FUGA no se puede pedir riego", [p["motivo"] for t, ty, p in sent if p.get("estado") == "DENEGADA"], ["Estacion no disponible (FUGA)"])
sent.clear()
feed(T, env("FIN_RIEGO", {"ws_id": "WS-04", "id_solicitud": "a1", "motivo": "AVERIA", "volumen_total_l": 3.4, "duracion_s": 4}))
check("32 el FIN_RIEGO posterior del Engine se ignora", (sent, st("WS-04")), ([], "FUGA"))
socket_server.handle_fault(conn, ["AVERIA", "WS-04", "RESUELTA"], db)
check("33 averia RESUELTA -> ACTIVADA sin ordenes", (st("WS-04"), stops()), ("ACTIVADA", []))
sent.clear()
socket_server.handle_fault(conn, ["AVERIA", "WS-04", "DETECTADA"], db)
check("34 averia sin riego abierto: no se manda nada", (st("WS-04"), sent), ("FUGA", []))

reset()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 60}, "FO:3", id_msg="a2")); sent.clear()
socket_server.handle_fault(Conn(), ["AVERIA", "WS-04", "DETECTADA"], db)
check("35 averia con riego PENDIENTE: ABORTADO y DENEGADA al operario",
      (state("a2"), [(p["estado"], p["motivo"]) for t, ty, p in sent if ty == "ESTADO_SOLICITUD"]),
      ("ABORTADO", [("DENEGADA", "La estacion tiene una averia")]))
check("36 ...y se ordena parar con motivo AVERIA", stops(), [("wm.ws.WS-04", "a2", "AVERIA")])

reset(); start_running("a3"); start_running("a4", ws="WS-05")
socket_server.handle_fault(Conn(), ["AVERIA", "WS-04", "DETECTADA"], db)
check("37 la averia de una estacion no toca el riego de otra", (state("a3"), state("a4"), st("WS-05")), ("ABORTADO", "EN_CURSO", "REGANDO"))

# averia de estacion desconocida
sent.clear()
c = Conn(); socket_server.handle_fault(c, ["AVERIA", "WS-99", "DETECTADA"], db)
check("38 averia de estacion desconocida: error y nada que parar", (c.out, stops()), (["STATUS#ERROR#Estacion WS-99 no registrada"], []))

# ======= RESPUESTA TARDIA
reset()
feed(R, env("SOLICITUD_RIEGO", {"ws_id": "WS-04", "duracion_s": 60}, "FO:3", id_msg="t1")); sent.clear()
db.execute("UPDATE riegos SET creado_ts = ? WHERE id_solicitud='t1'", (int(time.time()) - 100,)); db.commit()
irrigation.expire_pending_irrigations(db)
check("39 timeout aborta el riego PENDIENTE", state("t1"), "ABORTADO")
check("40 el timeout no manda DETENER_RIEGO por si solo", stops(), [])
sent.clear()
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-04", "id_solicitud": "t1", "aceptado": True, "motivo": ""}))
check("41 respuesta tardia ACEPTADA: se ordena parar (RESPUESTA_TARDIA)", stops(), [("wm.ws.WS-04", "t1", "RESPUESTA_TARDIA")])
check("42 ...sin reabrir el riego ni tocar la estacion", (state("t1"), st("WS-04")), ("ABORTADO", "ACTIVADA"))
sent.clear()
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-04", "id_solicitud": "t1", "aceptado": False, "motivo": "x"}))
check("43 respuesta tardia RECHAZADA: no hace falta parar nada", stops(), [])
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-04", "id_solicitud": "no-existe", "aceptado": True, "motivo": ""}))
check("44 aceptacion de un riego desconocido: ignorada", stops(), [])
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-05", "id_solicitud": "t1", "aceptado": True, "motivo": ""}))
check("45 aceptacion desde OTRA estacion con ese id: ignorada", stops(), [])
reset(); start_running("t2")
feed(T, env("FIN_RIEGO", {"ws_id": "WS-04", "id_solicitud": "t2", "motivo": "TIEMPO", "volumen_total_l": 5.0, "duracion_s": 5})); sent.clear()
feed(T, env("RESPUESTA_INICIO", {"ws_id": "WS-04", "id_solicitud": "t2", "aceptado": True, "motivo": ""}))
check("46 aceptacion repetida de un riego FINALIZADO: no se manda parar", stops(), [])

# ======= DESCONEXION
reset(); start_running("d1")
c = Conn(); socket_server.active_ws["WS-04"] = c
socket_server.on_ws_disconnect("WS-04", c, db)
check("47 desconexion durante riego: ABORTADO, estacion DESCONECTADA", (state("d1"), st("WS-04")), ("ABORTADO", "DESCONECTADA"))
check("48 desconexion: resumen DESCONEXION y orden de parar al Engine", (resumenes(), stops()),
      ([("wm.fo.3", "DESCONEXION", 3.3)], [("wm.ws.WS-04", "d1", "DESCONEXION")]))
sent.clear()
socket_server.active_ws["WS-04"] = c
socket_server.on_ws_disconnect("WS-04", c, db)
check("49 segunda desconexion no repite nada", sent, [])

# averia y desconexion seguidas: una sola orden
reset(); start_running("d2")
socket_server.handle_fault(Conn(), ["AVERIA", "WS-04", "DETECTADA"], db)
c = Conn(); socket_server.active_ws["WS-04"] = c
socket_server.on_ws_disconnect("WS-04", c, db)
check("50 averia + desconexion: una sola orden y un solo resumen", (len(stops()), len(resumenes())), (1, 1))

print("\nFALLOS:", fails)
sys.exit(1 if fails else 0)
