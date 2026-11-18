import collections
import json
import os
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
 
from WM_Central.central import config
from WM_Central.central import menu
 
##################################################################
## MENSAJES DE LA APLICACION: copia de lo que Central imprime por consola
 
LOG = collections.deque(maxlen=60)
 
 
class _Tee:
    # Sustituye a sys.stdout: sigue escribiendo en la consola y ademas guarda cada linea
    def __init__(self, real):
        self.real = real
        self.buf = ""
        self.lock = threading.Lock()
 
    def write(self, text):
        self.real.write(text)
        with self.lock:
            self.buf += text
            while "\n" in self.buf:
                line, self.buf = self.buf.split("\n", 1)
                if line.strip():
                    LOG.append(time.strftime("%H:%M:%S ") + line)
        return len(text)
 
    def flush(self):
        self.real.flush()
 
    def __getattr__(self, name):
        return getattr(self.real, name)
 
 
def install_log_capture():
    if not isinstance(sys.stdout, _Tee):
        sys.stdout = _Tee(sys.stdout)
 
 
##################################################################
## DATOS
 
def get_state():
    conn = sqlite3.connect(config.DB_FILE, timeout=5)
    try:
        telemetry = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT ws_id, caudal_lpm, volumen_l FROM telemetry_latest")}
        running = {r[0]: r[1] for r in conn.execute(
            "SELECT ws_id, solicitante FROM riegos WHERE estado = 'EN_CURSO'")}
        stations = []
        for ws_id, location, status in conn.execute(
                "SELECT id, ubicacion, status FROM stations ORDER BY ubicacion, id"):
            flow, volume = telemetry.get(ws_id, (None, None))
            stations.append({
                "id": ws_id, "ubicacion": location, "estado": status,
                "caudal": flow, "volumen": volume, "operario": running.get(ws_id),
            })
        recent = [
            {"id": r[0], "ws": r[1], "solicitante": r[2], "duracion_s": r[3], "estado": r[4], "ts": r[5]}
            for r in conn.execute(
                "SELECT id_solicitud, ws_id, solicitante, duracion_s, estado, creado_ts "
                "FROM riegos ORDER BY creado_ts DESC LIMIT 10")
        ]
    finally:
        conn.close()
    return {"estaciones": stations, "riegos": recent, "log": list(LOG)}
 
 
##################################################################
## PAGINA
 
PAGE = """<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WM Central</title>
<style>
body{font-family:system-ui,sans-serif;margin:0;background:#0f1a20;color:#e6eef2}
header{padding:16px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}
h1{font-size:20px;margin:0}h1 small{font-weight:400;color:#8fa6b2}
h2{font-size:14px;color:#8fa6b2;margin:24px 24px 8px;text-transform:uppercase;letter-spacing:.05em}
.chip{display:inline-block;padding:4px 10px;margin-left:6px;border-radius:99px;font-size:13px;color:#10202a}
#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:12px;padding:0 24px}
.card{border-radius:10px;padding:14px;color:#10202a}
.card b{font-size:18px}.card div{margin-top:4px}
.ACTIVADA{background:#7bd88f}
.REGANDO{background:#7bd88f;animation:blink 1s infinite}
.FUGA{background:#ef5350;color:#fff}
.FUERA_DE_SERVICIO,.BLOQUEADA{background:#ffa726}
.DESCONECTADA{background:#9e9e9e}
.otro{background:#b0bec5}
@keyframes blink{50%{opacity:.45}}
table{border-collapse:collapse;margin:0 24px;width:calc(100% - 48px);font-size:14px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #22343e}
pre{margin:0 24px 24px;padding:12px;background:#0a1318;border-radius:8px;height:200px;overflow:auto;font-size:13px}
input,button,select{padding:8px;border-radius:6px;border:0;font-size:14px}
button{background:#7bd88f;color:#10202a;cursor:pointer}
button.warn{background:#ffa726}
#conn{color:#ef5350;padding:0 24px}
</style></head><body>
<header><h1>WaterManagement <small>· Panel de Central</small></h1><div id="chips"></div></header>
<div id="conn"></div>
<div id="grid"></div>
<h2>Acciones de Central</h2>
<div style="margin:0 24px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
<select id="target"><option value="TODAS">Todas las estaciones</option></select>
<input id="dur" type="number" min="1" value="30" style="width:110px" title="Duracion en segundos (solo al regar)">
<button id="regar">Regar</button>
<button id="bloquear" class="warn">Bloquear</button>
<button id="activar">Activar</button><span id="res"></span></div>
<h2>Riegos recientes</h2>
<table><thead><tr><th>Hora</th><th>Estacion</th><th>Solicitante</th><th>Duracion</th><th>Estado</th></tr></thead><tbody id="riegos"></tbody></table>
<h2>Mensajes de la aplicacion</h2>
<pre id="log"></pre>
<script>
const LABEL={ACTIVADA:'Disponible',REGANDO:'Regando',FUGA:'Fuga',FUERA_DE_SERVICIO:'Fuera de Servicio',BLOQUEADA:'Fuera de Servicio',DESCONECTADA:'Desconectada'};
const COLOR={Disponible:'#7bd88f',Regando:'#7bd88f',Fuga:'#ef5350','Fuera de Servicio':'#ffa726',Desconectada:'#9e9e9e'};
function el(tag,text,cls){const e=document.createElement(tag);if(text!==undefined&&text!==null)e.textContent=text;if(cls)e.className=cls;return e}
function label(s){return LABEL[s]||s}
function render(s){
  fillTargets(s.estaciones);
  const grid=document.getElementById('grid');grid.replaceChildren();
  const counts={};
  for(const w of s.estaciones){
    const l=label(w.estado);counts[l]=(counts[l]||0)+1;
    const c=el('div',null,'card '+(LABEL[w.estado]?w.estado:'otro'));
    c.append(el('b',w.id),el('div',w.ubicacion),el('div',l));
    if(w.estado==='FUERA_DE_SERVICIO'||w.estado==='BLOQUEADA'){}
    if(w.estado==='REGANDO'){
      c.append(el('div','Caudal: '+(w.caudal===null?'-':w.caudal)+' L/min'),
               el('div','Volumen: '+(w.volumen===null?'-':w.volumen)+' L'),
               el('div','Operario: '+(w.operario||'-')));
    }
    grid.append(c);
  }
  if(!s.estaciones.length)grid.append(el('div','No hay estaciones registradas'));
  const chips=document.getElementById('chips');chips.replaceChildren();
  for(const [l,n] of Object.entries(counts)){const c=el('span',n+' '+l,'chip');c.style.background=COLOR[l]||'#b0bec5';chips.append(c)}
  const tb=document.getElementById('riegos');tb.replaceChildren();
  for(const r of s.riegos){
    const tr=el('tr');
    for(const v of [new Date(r.ts*1000).toLocaleTimeString(),r.ws,r.solicitante,r.duracion_s+' s',r.estado])tr.append(el('td',v));
    tb.append(tr);
  }
  const log=document.getElementById('log');const atEnd=log.scrollTop+log.clientHeight>=log.scrollHeight-5;
  log.textContent=s.log.join('\\n');if(atEnd)log.scrollTop=log.scrollHeight;
}
async function refresh(){
  const c=document.getElementById('conn');
  try{const r=await fetch('/api/state');render(await r.json());c.textContent=''}
  catch(e){c.textContent='Sin conexion con Central'}
}
let lastIds='';
function fillTargets(list){
  const ids=list.map(w=>w.id).join(',');
  if(ids===lastIds)return;lastIds=ids;
  const sel=document.getElementById('target');const prev=sel.value;sel.replaceChildren();
  sel.append(new Option('Todas las estaciones','TODAS'));
  for(const w of list)sel.append(new Option(w.id+' - '+w.ubicacion,w.id));
  sel.value=[...sel.options].some(o=>o.value===prev)?prev:'TODAS';
}
async function orden(accion){
  const res=document.getElementById('res');res.textContent='';
  const target=document.getElementById('target').value;
  try{
    const r=await fetch('/api/orden',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({accion:accion,ws_id:target,duracion_s:parseInt(document.getElementById('dur').value,10)})});
    const j=await r.json();
    res.textContent=r.ok?(target==='TODAS'?j.resumen:j.mensajes.join(' ')):j.error;
  }catch(e){res.textContent='Sin conexion con Central'}
}
document.getElementById('regar').addEventListener('click',()=>orden('REGAR'));
document.getElementById('bloquear').addEventListener('click',()=>orden('BLOQUEAR'));
document.getElementById('activar').addEventListener('click',()=>orden('ACTIVAR'));
refresh();setInterval(refresh,1000);
</script></body></html>
"""
 
 
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass    # no llenar la consola de Central con cada peticion
 
    def send_body(self, code, body, ctype="application/json"):
        if not isinstance(body, bytes):
            body = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
 
    def do_GET(self):
        if self.path == "/api/state":
            try:
                self.send_body(200, get_state())
            except sqlite3.Error as e:
                self.send_body(500, {"error": str(e)})
        elif self.path in ("/", "/index.html"):
            self.send_body(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        else:
            self.send_body(404, {"error": "no encontrado"})
 
    # Ordenes de Central: regar / bloquear / activar, a una o a todas
    def do_POST(self):
        if self.path not in ("/api/orden", "/api/regar_todas"):
            self.send_body(404, {"error": "no encontrado"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 10000:
                raise ValueError
            data = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(data, dict):
                raise ValueError
        except ValueError:
            self.send_body(400, {"error": "JSON no valido"})
            return

        # El endpoint antiguo equivale a regar en todas
        if self.path == "/api/regar_todas":
            data["accion"], data["ws_id"] = "REGAR", "TODAS"
        action = data.get("accion")
        target = data.get("ws_id")
        duration_s = data.get("duracion_s")

        if action not in ("REGAR", "BLOQUEAR", "ACTIVAR"):
            self.send_body(400, {"error": "Orden no valida"})
            return
        if not isinstance(target, str) or not target:
            self.send_body(400, {"error": "Estacion no valida"})
            return
        # La duracion solo importa al regar
        if action == "REGAR" and (isinstance(duration_s, bool) or not isinstance(duration_s, int)
                                  or not 1 <= duration_s <= 3600):
            self.send_body(400, {"error": "Duracion no valida (1-3600 s)"})
            return
        try:
            result = menu.run_order(action, target, duration_s)
        except RuntimeError as e:
            self.send_body(503, {"error": str(e)})
            return
        except sqlite3.Error as e:
            self.send_body(500, {"error": str(e)})
            return
        self.send_body(200, result)
 
 
def run_panel(port=None):
    port = port or int(os.environ.get("PANEL_PORT") or os.environ.get("PORT") or 8080)
    install_log_capture()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    print(f"[PANEL] Panel web en http://localhost:{port}")
    server.serve_forever()