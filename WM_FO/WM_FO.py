import argparse
import collections
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
 
from confluent_kafka import Consumer, Producer
 
TOPIC_REQ = "wm.central.solicitudes"
TOPIC_STATIONS = "wm.estaciones"      # lista de estaciones que publica Central
STATIONS_MAX_AGE_S = 10               # si la ultima lista es mas vieja, se considera que no hay datos
FORMAT = 'utf-8'
WAIT_BETWEEN_S = 4      # pausa entre servicios del fichero (lo pide el enunciado)
EXTRA_WAIT_S = 40       # margen sobre la duracion del riego (cubre el timeout de 30 s de Central)
 
operator_id = None
producer = None
assigned = threading.Event()        # el consumidor ya tiene su particion asignada
stop_listening = threading.Event()
 
# id_solicitud -> {"ws", "duracion_s", "done" (Event), "estado", "caudal", "volumen", "resumen"}
requests = {}
requests_lock = threading.Lock()
stations = []          # ultima lista recibida: [{id, ubicacion, estado}]
stations_ts = 0
events = collections.deque(maxlen=100)     # mensajes para la web (la telemetria no entra, va en la tabla)
 
 
def parse_args():
    parser = argparse.ArgumentParser(description="WM_FO - Operador de las maquinas de riego")
    parser.add_argument("broker", type=str, help="IP:puerto del broker de Kafka, p. ej. localhost:9092")
    parser.add_argument("ID_FO", type=str, help="Identificador del Operador")
    parser.add_argument("--fichero", type=str, default=None,
                        help="Fichero con las peticiones de riego a ejecutar de forma automatica")
    parser.add_argument("--web", type=int, default=None, metavar="PUERTO",
                        help="Abrir la interfaz web en ese puerto, p. ej. 8081")
    parser.add_argument("--host", type=str, default="127.0.0.1",
                        help="Direccion donde escucha la web (en Docker, 0.0.0.0)")
    return parser.parse_args()
 
 
def log_event(text):
    events.append(time.strftime("%H:%M:%S ") + text)
    print(f"[FO] {text}")
 
 
##################################################################
## ENVIO
 
def publish(msg_type, payload, id_msg=None):
    id_msg = id_msg or str(uuid.uuid4())
    envelope = {
        "tipo": msg_type,
        "id_msg": id_msg,
        "origen": f"FO:{operator_id}",
        "ts": int(time.time()),
        "payload": payload,
    }
    producer.produce(TOPIC_REQ, value=json.dumps(envelope).encode(FORMAT))
    if producer.flush(10) > 0:
        log_event("No se pudo entregar el mensaje: esta el broker accesible?")
    return id_msg
 
 
def request_irrigation(ws_id, duration_s):
    # id_msg = id_solicitud, asi Central y el operario hablan del mismo riego
    request_id = str(uuid.uuid4())
    with requests_lock:
        requests[request_id] = {"ws": ws_id, "duracion_s": duration_s, "done": threading.Event(),
                                "estado": "ENVIADA", "caudal": None, "volumen": None, "resumen": None}
    publish("SOLICITUD_RIEGO", {"ws_id": ws_id, "duracion_s": duration_s}, id_msg=request_id)
    log_event(f"Solicitud enviada: riego en {ws_id} durante {duration_s}s (id={request_id})")
    return request_id
 
 
def stop_irrigation(request_id):
    with requests_lock:
        req = requests.get(request_id)
    if req is None:
        log_event("Ese riego no es mio")
        return False
    publish("DETENER_RIEGO", {"id_solicitud": request_id, "ws_id": req["ws"]})
    log_event(f"DETENER_RIEGO enviado para {request_id}")
    return True
 
 
##################################################################
## RECEPCION (hilo propio: guarda y muestra todo lo que Central manda a este operario)
 
def describe(msg_type, payload):
    if msg_type == "ESTADO_SOLICITUD":
        text = payload.get("estado") or ""
        if payload.get("motivo"):
            text += f" ({payload['motivo']})"
        return text
    if msg_type == "TELEMETRIA":
        return f"{payload.get('caudal_lpm')} L/min | {payload.get('volumen_l')} L"
    if msg_type == "RESUMEN_RIEGO":
        return (f"{payload.get('volumen_total_l')} L en {payload.get('duracion_s')} s "
                f"(fin: {payload.get('motivo_fin')})")
    return ""
 
 
def update_stations(payload):
    global stations, stations_ts
    lst = payload.get("estaciones")
    if isinstance(lst, list):
        stations = [w for w in lst if isinstance(w, dict) and isinstance(w.get("id"), str)]
        stations_ts = time.time()
 
 
def current_stations():
    # Lista reciente de estaciones, o [] si Central lleva tiempo sin publicarla
    return stations if time.time() - stations_ts < STATIONS_MAX_AGE_S else []
 
 
def handle_incoming(raw):
    try:
        msg = json.loads(raw.decode(FORMAT))
    except ValueError:
        return
    if not isinstance(msg, dict) or not isinstance(msg.get("payload"), dict):
        return
    msg_type = msg.get("tipo")
    payload = msg["payload"]
    if msg_type == "ESTADO_ESTACIONES":
        update_stations(payload)
        return
    request_id = payload.get("id_solicitud")
    line = f"{msg_type} {payload.get('ws_id', '')} {describe(msg_type, payload)}".replace("  ", " ")
    if msg_type == "TELEMETRIA":
        print(f"[FO] {line}")
    else:
        log_event(line)
 
    with requests_lock:
        req = requests.get(request_id)
    if req is None:
        return      # mensaje de un riego de una ejecucion anterior
    if msg_type == "ESTADO_SOLICITUD":
        req["estado"] = payload.get("estado")
        if payload.get("estado") == "DENEGADA":
            req["resumen"] = payload.get("motivo") or "Denegada"
            req["done"].set()
    elif msg_type == "TELEMETRIA":
        req["caudal"] = payload.get("caudal_lpm")
        req["volumen"] = payload.get("volumen_l")
    elif msg_type == "RESUMEN_RIEGO":
        req["estado"] = "FINALIZADO"
        req["resumen"] = describe(msg_type, payload)
        req["done"].set()
 
 
def listen(consumer):
    while not stop_listening.is_set():
        m = consumer.poll(1.0)
        if m is None or m.error():
            continue
        try:
            handle_incoming(m.value())
        except Exception as e:
            print(f"[FO] Error tratando un mensaje: {e!r}")
    consumer.close()
 
 
##################################################################
## MODO FICHERO
 
def read_services(path):
    services = []
    with open(path, encoding=FORMAT) as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            try:
                if len(parts) != 2 or not parts[0]:
                    raise ValueError
                duration_s = int(parts[1])
                if duration_s <= 0:
                    raise ValueError
            except ValueError:
                print(f"[FO] Linea {n} ignorada (formato esperado 'WS-04,30'): {line}")
                continue
            services.append((parts[0], duration_s))
    return services
 
 
def run_file(path):
    try:
        services = read_services(path)
    except OSError as e:
        print(f"[FO] No se pudo leer {path}: {e}")
        return
    print(f"[FO] {len(services)} servicios en {path}")
    for i, (ws_id, duration_s) in enumerate(services, 1):
        print(f"\n[FO] --- Servicio {i}/{len(services)} ---")
        request_id = request_irrigation(ws_id, duration_s)
        with requests_lock:
            done = requests[request_id]["done"]
        if not done.wait(duration_s + EXTRA_WAIT_S):
            print("[FO] Se acabo el tiempo de espera sin resumen ni denegacion, sigo con el siguiente")
        if i < len(services):
            print(f"[FO] Esperando {WAIT_BETWEEN_S}s antes del siguiente servicio...")
            time.sleep(WAIT_BETWEEN_S)
    print("\n[FO] Fichero completado")
 
 
##################################################################
## MENU DE CONSOLA
 
MENU = """
===== WM_FO (operario {op}) =====
 1) Solicitar riego
 2) Detener un riego
 3) Ver mis riegos
 0) Salir
"""
 
 
def active_requests():
    with requests_lock:
        return [(rid, r) for rid, r in requests.items() if not r["done"].is_set()]
 
 
def choose_station():
    lst = current_stations()
    if not lst:
        return input("ID de la estacion (no llega la lista de Central): ").strip()
    for n, w in enumerate(lst, 1):
        print(f"  {n}) {w['id']} - {w.get('ubicacion', '')} - {w.get('estado', '')}")
    try:
        return lst[int(input("Numero de estacion: ").strip()) - 1]["id"]
    except (ValueError, IndexError):
        return None
 
 
def menu_loop():
    while True:
        print(MENU.format(op=operator_id))
        try:
            option = input("> ").strip()
        except EOFError:
            print("[FO] Sin teclado (stdin cerrado). En Docker usa 'docker run -it'")
            return
        if option == "1":
            ws_id = choose_station()
            try:
                duration_s = int(input("Duracion en segundos: ").strip())
            except ValueError:
                duration_s = 0
            if not ws_id or duration_s <= 0:
                print("[FO] Datos no validos")
                continue
            request_irrigation(ws_id, duration_s)
        elif option == "2":
            active = active_requests()
            if not active:
                print("[FO] No tienes ningun riego activo")
                continue
            for n, (rid, r) in enumerate(active, 1):
                print(f"  {n}) {r['ws']} - {r['estado']} ({rid})")
            try:
                choice = int(input("Cual detener: ").strip())
                stop_irrigation(active[choice - 1][0])
            except (ValueError, IndexError):
                print("[FO] Opcion no valida")
        elif option == "3":
            with requests_lock:
                if not requests:
                    print("[FO] Aun no has pedido ningun riego")
                for rid, r in requests.items():
                    print(f"  {r['ws']} | {r['estado']} | {r['duracion_s']}s | {rid}")
        elif option == "0":
            return
 
 
##################################################################
## MODO WEB
#   GET  /                -> pagina
#   GET  /api/state       -> mis riegos y mensajes
#   POST /api/solicitar   -> {"ws_id": "WS-04", "duracion_s": 30}
#   POST /api/detener     -> {"id_solicitud": "..."}
 
PAGE = """<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WM FO</title>
<style>
body{font-family:system-ui,sans-serif;margin:0;background:#f4f7f5;color:#1b2b24}
header{background:#2e7d32;color:#fff;padding:14px 24px}
h1{font-size:20px;margin:0}
main{max-width:900px;margin:0 auto;padding:16px}
section{background:#fff;border-radius:10px;padding:16px;margin-bottom:16px;box-shadow:0 1px 3px #0002}
h2{font-size:15px;margin:0 0 10px}
form{display:flex;gap:8px;flex-wrap:wrap}
input,select{padding:8px;border:1px solid #b6c4bb;border-radius:6px;font-size:15px}
button{padding:8px 14px;border:0;border-radius:6px;background:#2e7d32;color:#fff;font-size:15px;cursor:pointer}
button.stop{background:#c62828;padding:4px 10px;font-size:13px}
button:disabled{opacity:.5}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #e1e8e3}
.regando{background:#e8f5e9;animation:blink 1.2s infinite}
@keyframes blink{50%{background:#fff}}
pre{margin:0;padding:10px;background:#10201a;color:#cfe8d8;border-radius:8px;height:180px;overflow:auto;font-size:13px}
#msg{color:#c62828;margin-top:8px;min-height:1em}
</style></head><body>
<header><h1 id="title">WaterManagement · Operario</h1></header>
<main>
<section><h2>Solicitar riego</h2>
<form id="f"><select id="ws" required><option value="">Esperando lista de estaciones...</option></select>
<input id="dur" type="number" min="1" placeholder="Duracion (s)" required>
<button id="go">Solicitar</button></form><div id="msg"></div></section>
<section><h2>Mis riegos</h2>
<table><thead><tr><th>Estacion</th><th>Estado</th><th>Caudal</th><th>Volumen</th><th>Resultado</th><th></th></tr></thead>
<tbody id="rows"></tbody></table></section>
<section><h2>Mensajes</h2><pre id="log"></pre></section>
</main>
<script>
function el(tag,text,cls){const e=document.createElement(tag);if(text!==undefined&&text!==null)e.textContent=text;if(cls)e.className=cls;return e}
const msg=document.getElementById('msg');
async function post(url,body){
  const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const j=await r.json();if(!r.ok)throw new Error(j.error||'Error');return j;
}
document.getElementById('f').addEventListener('submit',async e=>{
  e.preventDefault();const b=document.getElementById('go');b.disabled=true;msg.textContent='';
  try{await post('/api/solicitar',{ws_id:document.getElementById('ws').value.trim(),duracion_s:parseInt(document.getElementById('dur').value,10)});refresh()}
  catch(err){msg.textContent=err.message}
  b.disabled=false;
});
async function stop(id){try{await post('/api/detener',{id_solicitud:id})}catch(err){msg.textContent=err.message}}
const NAMES={ACTIVADA:'Disponible',REGANDO:'Regando',FUGA:'Fuga',DESCONECTADA:'Desconectada',FUERA_DE_SERVICIO:'Fuera de Servicio'};
let lastStations='';
function renderStations(s){
  const key=JSON.stringify(s.estaciones);
  if(key===lastStations)return;lastStations=key;
  const sel=document.getElementById('ws');const prev=sel.value;sel.replaceChildren();
  if(!s.estaciones.length){sel.append(new Option('Sin lista de estaciones (esperando a Central)',''));return}
  sel.append(new Option('Elige estacion...',''));
  for(const w of s.estaciones){
    const ok=w.estado==='ACTIVADA';
    const o=new Option(w.id+' - '+w.ubicacion+' - '+(NAMES[w.estado]||w.estado),w.id);
    o.disabled=!ok;sel.append(o);
  }
  sel.value=prev;
}
function render(s){
  renderStations(s);
  document.getElementById('title').textContent='WaterManagement · Operario '+s.operario;
  const tb=document.getElementById('rows');tb.replaceChildren();
  for(const r of s.riegos){
    const tr=el('tr',null,r.estado==='AUTORIZADA'?'regando':'');
    tr.append(el('td',r.ws),el('td',r.estado),
      el('td',r.caudal===null?'-':r.caudal+' L/min'),el('td',r.volumen===null?'-':r.volumen+' L'),
      el('td',r.resumen||''));
    const td=el('td');
    if(r.activo){const b=el('button','Detener','stop');b.onclick=()=>stop(r.id);td.append(b)}
    tr.append(td);tb.append(tr);
  }
  if(!s.riegos.length){const tr=el('tr');const td=el('td','Aun no has pedido ningun riego');td.colSpan=6;tr.append(td);tb.append(tr)}
  const log=document.getElementById('log');const end=log.scrollTop+log.clientHeight>=log.scrollHeight-5;
  log.textContent=s.eventos.join('\\n');if(end)log.scrollTop=log.scrollHeight;
}
async function refresh(){try{const r=await fetch('/api/state');render(await r.json())}catch(e){msg.textContent='Sin conexion con WM_FO'}}
refresh();setInterval(refresh,1000);
</script></body></html>
"""
 
 
def web_state():
    with requests_lock:
        items = list(requests.items())
    riegos = [{
        "id": rid, "ws": r["ws"], "duracion_s": r["duracion_s"], "estado": r["estado"],
        "activo": not r["done"].is_set(), "caudal": r["caudal"], "volumen": r["volumen"],
        "resumen": r["resumen"],
    } for rid, r in reversed(items)]
    return {"operario": operator_id, "riegos": riegos, "eventos": list(events), "estaciones": current_stations()}
 
 
class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass
 
    def reply(self, code, body, ctype="application/json"):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode(FORMAT)
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
 
    def do_GET(self):
        if self.path == "/api/state":
            self.reply(200, web_state())
        elif self.path in ("/", "/index.html"):
            self.reply(200, PAGE.encode(FORMAT), "text/html; charset=utf-8")
        else:
            self.reply(404, {"error": "no encontrado"})
 
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length).decode(FORMAT))
            if not isinstance(data, dict):
                raise ValueError
        except ValueError:
            self.reply(400, {"error": "JSON no valido"})
            return
 
        if self.path == "/api/solicitar":
            ws_id, duration_s = data.get("ws_id"), data.get("duracion_s")
            if (not isinstance(ws_id, str) or not ws_id.strip() or isinstance(duration_s, bool)
                    or not isinstance(duration_s, int) or duration_s <= 0):
                self.reply(400, {"error": "Estacion o duracion no validas"})
                return
            self.reply(200, {"id": request_irrigation(ws_id.strip(), duration_s)})
        elif self.path == "/api/detener":
            request_id = data.get("id_solicitud")
            if not isinstance(request_id, str) or not stop_irrigation(request_id):
                self.reply(404, {"error": "Ese riego no es tuyo"})
                return
            self.reply(200, {"ok": True})
        else:
            self.reply(404, {"error": "no encontrado"})
 
 
def run_web(port, host="127.0.0.1"):
    # Solo en localhost: es la aplicacion de UN operario, no se expone a la red
    server = ThreadingHTTPServer((host, port), WebHandler)
    server.daemon_threads = True
    print(f"[FO] Interfaz web en http://localhost:{port}  (Ctrl+C para salir)")
    server.serve_forever()
 
 
##################################################################
## MAIN
 
if __name__ == "__main__":
    args = parse_args()
    operator_id = args.ID_FO
 
    producer = Producer({"bootstrap.servers": args.broker})
 
    # Escuchar ANTES de enviar. Con 'latest' un mensaje enviado antes de que el grupo tenga
    # particion asignada se perderia, por eso se espera a la asignacion
    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": f"fo-{operator_id}",
        "auto.offset.reset": "latest",
        "allow.auto.create.topics": True,
        "topic.metadata.refresh.interval.ms": 5000,   # si un topic aparece despues, que se entere pronto
    })
    consumer.subscribe([f"wm.fo.{operator_id}", TOPIC_STATIONS], on_assign=lambda c, parts: assigned.set())
    threading.Thread(target=listen, args=(consumer,), daemon=True).start()
    if not assigned.wait(30):
        print("[FO] AVISO: el consumidor no ha obtenido particion tras 30 s, se continua igualmente")
    print(f"[FO] Operario {operator_id} escuchando en wm.fo.{operator_id}")
 
    try:
        if args.fichero:
            run_file(args.fichero)
            time.sleep(1)
        elif args.web:
            run_web(args.web, args.host)
        else:
            menu_loop()
    except KeyboardInterrupt:
        pass
    finally:
        stop_listening.set()
        time.sleep(1.2)
        print("\n[FO] Cerrado")