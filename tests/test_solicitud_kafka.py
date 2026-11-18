"""Cliente de PRUEBA (no forma parte de la entrega): hace de WM_FO.
Envia una SOLICITUD_RIEGO a Central y muestra todo lo que le llega: los estados de la
solicitud, la telemetria del riego y el resumen final. Termina al recibir el resumen,
una denegacion, o al pasar el tiempo maximo.

Uso: python3 test_solicitud_kafka.py <broker> <id_operador> <ws_id> [duracion_s] [parar_a_los_s]
  p. ej. python3 test_solicitud_kafka.py localhost:9092 3 WS-04 10
         python3 test_solicitud_kafka.py localhost:9092 3 WS-04 30 5    (pide parar el riego a los 5 s)

Lee wm.fo.<id_operador> (respuestas de Central) y wm.ws.<ws_id> (para ver la orden INICIAR_RIEGO).
"""

import json
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer

# Tiempo maximo de espera = duracion del riego + este margen (cubre el timeout de 30 s de Central)
EXTRA_WAIT_S = 40

if len(sys.argv) < 4:
    print(__doc__)
    sys.exit(1)

broker, operator_id, ws_id = sys.argv[1], sys.argv[2], sys.argv[3]
duration_s = int(sys.argv[4]) if len(sys.argv) > 4 else 30
stop_after_s = int(sys.argv[5]) if len(sys.argv) > 5 else None   # None = no pedir la parada

topic_fo = f"wm.fo.{operator_id}"
topic_ws = f"wm.ws.{ws_id}"


def describe(msg_type, payload):
    # Texto corto con lo importante de cada tipo de mensaje
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
    if msg_type == "INICIAR_RIEGO":
        return f"{payload.get('duracion_s')} s"
    if msg_type == "DETENER_RIEGO":
        return f"({payload.get('motivo')})"
    return ""


# 1) Escuchar ANTES de enviar, para no perder la respuesta
consumer = Consumer({
    'bootstrap.servers': broker,
    # group.id unico por ejecucion: es una prueba, no queremos retomar offsets de otra
    'group.id': f"test-fo-{operator_id}-{uuid.uuid4().hex[:8]}",
    'auto.offset.reset': 'earliest',
    'allow.auto.create.topics': True,
})
consumer.subscribe([topic_fo, topic_ws])

# 2) Enviar la solicitud
request_id = str(uuid.uuid4())
envelope = {
    "tipo": "SOLICITUD_RIEGO",
    "id_msg": request_id,
    "origen": f"FO:{operator_id}",
    "ts": int(time.time()),
    "payload": {"ws_id": ws_id, "duracion_s": duration_s},
}
producer = Producer({'bootstrap.servers': broker})
producer.produce("wm.central.solicitudes", value=json.dumps(envelope).encode("utf-8"))
if producer.flush(10) > 0:
    print(f"No se pudo entregar la solicitud: esta el broker en {broker}?")
    sys.exit(1)
max_wait_s = duration_s + EXTRA_WAIT_S
print(f"Solicitud enviada (id_solicitud={request_id}). Escuchando hasta {max_wait_s}s...\n")

# 3) Mostrar solo los mensajes que se refieren a ESTA solicitud
deadline = time.time() + max_wait_s
stop_at = time.time() + stop_after_s if stop_after_s is not None else None
seen = 0
finished = False
while time.time() < deadline and not finished:
    # Si se pidio, a la hora indicada se envia la orden de parar ESTE riego
    if stop_at is not None and time.time() >= stop_at:
        stop_at = None
        stop_envelope = {
            "tipo": "DETENER_RIEGO",
            "id_msg": str(uuid.uuid4()),
            "origen": f"FO:{operator_id}",
            "ts": int(time.time()),
            "payload": {"id_solicitud": request_id, "ws_id": ws_id},
        }
        producer.produce("wm.central.solicitudes", value=json.dumps(stop_envelope).encode("utf-8"))
        producer.flush(10)
        print(">> DETENER_RIEGO enviado")
    m = consumer.poll(1.0)
    if m is None or m.error():
        continue
    try:
        msg = json.loads(m.value().decode("utf-8"))
    except ValueError:
        continue
    payload = msg.get("payload", {})
    if payload.get("id_solicitud") != request_id:
        continue
    seen += 1
    msg_type = msg.get("tipo")
    print(f"[{m.topic()}] {msg_type} {describe(msg_type, payload)}".rstrip())

    # Fin de la historia: resumen del riego, o la solicitud fue denegada
    if msg_type == "RESUMEN_RIEGO" or (msg_type == "ESTADO_SOLICITUD" and payload.get("estado") == "DENEGADA"):
        finished = True

consumer.close()
if seen == 0:
    print("Sin respuesta de Central. Esta arrancado y conectado al mismo broker?")
elif not finished:
    print("\nSe acabo el tiempo de espera sin resumen ni denegacion.")
