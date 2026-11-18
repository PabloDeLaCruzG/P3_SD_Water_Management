# kafka_utils.py, Utilidades de Kafka que no dependen de la logica de negocio:
#   - el productor (enviar mensajes)
#   - el sobre JSON comun (crearlo al enviar, validarlo al recibir)
#   - averiguar a que topic hay que contestar segun el "origen"
#   - notify(): atajo para enviar ESTADO_SOLICITUD al solicitante

import json
import time
import uuid

from confluent_kafka import Producer

from WM_Central.central import config

# Productor unico de todo el programa, empieza en None y se crea al arrancar, con init_producer()
producer = None


def init_producer(broker):
    global producer
    producer = Producer({'bootstrap.servers': broker})


def delivery_report(err, msg):
    # Callback de Kafka, avisa si un mensaje no se pudo entregar
    if err is not None:
        print(f"[KAFKA] Delivery to {msg.topic()} failed: {err}")


def publish_json(topic, msg_type, payload):
    # Envuelve el payload en el json comun y lo publica en el topic indicado
    if topic == config.TOPIC_SCREEN:   # [NUEVO]
        return   # Es para la propia Central, no sale a Kafka, ya se muestra por pantalla
    
    envelope = {
        "tipo": msg_type,
        "id_msg": str(uuid.uuid4()),   # identificador unico, sirve para detectar duplicados
        "origen": "CENTRAL",
        "ts": int(time.time()),
        "payload": payload
    }
    producer.produce(topic, value=json.dumps(envelope).encode(config.FORMAT), callback=delivery_report)
    producer.poll(0)  # sirve los callbacks pendientes (si no, delivery_report nunca se ejecutaria)

# Traduce el campo "origen" del mensaje al topic donde hay que contestar
def reply_topic(origin):
    if origin == "CENTRAL":
        return config.TOPIC_SCREEN   # Las respuestas a Central se muestran por pantalla
    kind, _, ident = origin.partition(":")
    if not ident:
        return None
    if kind == "FO":
        return f"wm.fo.{ident}"
    if kind == "WS":
        return f"wm.ws.{ident}"
    return None


# Convierte los bytes recibidos en un diccionario y comprueba que cumple el json
def parse_envelope(raw):
    try:
        msg = json.loads(raw.decode(config.FORMAT))
    except ValueError:  # JSON invalido o bytes que no son utf-8
        return None

    # Tiene que ser un objeto JSON con todos los campos obligatorios
    if not isinstance(msg, dict) or any(f not in msg for f in config.REQUIRED_FIELDS):
        return None
    # tipo, id_msg y origen, texto no vacio
    if not all(isinstance(msg[f], str) and msg[f] for f in ("tipo", "id_msg", "origen")):
        return None
    # ts  un numero
    if isinstance(msg["ts"], bool) or not isinstance(msg["ts"], (int, float)):
        return None
    # payload un objeto
    if not isinstance(msg["payload"], dict):
        return None
    
    return msg


def notify(topic, request_id, ws_id, state, reason=""):
    # Informa al solicitante de como va su peticion.
    # RECIBIDA, VALIDANDO, AUTORIZADA o DENEGADA (con motivo)
    print(f"[ESTADO] {request_id[:8]} {ws_id}: {state} {reason}".rstrip())   # Tambien en la pantalla de Central
    publish_json(topic, "ESTADO_SOLICITUD", {
        "id_solicitud": request_id,
        "ws_id": ws_id,
        "estado": state,
        "motivo": reason,
    })