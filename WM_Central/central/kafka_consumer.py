# kafka_consumer.py, Hilo que lee los mensajes de Kafka dirigidos a Central
#
# Flujo de cada mensaje:
#   bytes -> parse_envelope (valido?) -> caducado? -> duplicado? -> segun topic/tipo -> handler
# Topics que lee:
#   wm.central.solicitudes -> SOLICITUD_RIEGO, DETENER_RIEGO             (operario o estacion)
#   wm.central.telemetria  -> RESPUESTA_INICIO, TELEMETRIA, FIN_RIEGO   (estacion)
# Ademas, cada 2 s cierra los riegos PENDIENTE que la estacion no ha confirmado a tiempo

import time
import traceback

from confluent_kafka import Consumer, KafkaError

from WM_Central.central import config
from WM_Central.central import database
from WM_Central.central import irrigation
from WM_Central.central import kafka_utils

# Con el broker vacio, el topic aun no existe al suscribirse, se crea solo y el consumidor se recupera
# Si el aviso persiste pasado el margen, si se muestra
def should_ignore_consumer_error(err, started_at):
    return (err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART
            and time.time() - started_at < config.STARTUP_GRACE_S)


# Filtros comunes a cualquier mensaje, si falla uno, se descarta y se sigue.
def process_kafka_message(topic, raw, db):

    # Tiene que cumplir el json comun
    msg = kafka_utils.parse_envelope(raw)
    if msg is None:
        print(f"[KAFKA] Mensaje mal formado en {topic}, descartado")
        return

    # No puede ser un mensaje antiguo
    if topic == config.TOPIC_REQ:
        age = time.time() - msg["ts"]
        if age > config.MAX_AGE_S:
            print(f"[KAFKA] Mensaje caducado descartado: {msg['tipo']} {msg['id_msg']} ({age:.0f}s)")
            return

    # No puede haberse procesado ya, para evitar repetidos
    # TELEMETRIA se salta este filtro porque llega cada segundo, repetirla no hace daño
    if msg["tipo"] != "TELEMETRIA" and database.already_processed(db, msg["id_msg"]):
        print(f"[KAFKA] Mensaje duplicado ignorado: {msg['id_msg']}")
        return

    # Enviar segun topic y tipo
    tipo = msg["tipo"]
    if topic == config.TOPIC_REQ and tipo == "SOLICITUD_RIEGO":
        irrigation.handle_irrigation_request(msg, db)
    elif topic == config.TOPIC_REQ and tipo == "DETENER_RIEGO":      
        irrigation.handle_stop_request(msg, db)                      
    elif topic == config.TOPIC_TELEMETRY and tipo == "RESPUESTA_INICIO":
        irrigation.handle_start_response(msg, db)
    elif topic == config.TOPIC_TELEMETRY and tipo == "TELEMETRIA":
        irrigation.handle_telemetry(msg, db)
    elif topic == config.TOPIC_TELEMETRY and tipo == "FIN_RIEGO":
        irrigation.handle_irrigation_end(msg, db)
    else:
        print(f"[KAFKA] Tipo no soportado en {topic}: {tipo}")


def kafka_loop(broker):
    # Cuerpo del hilo consumidor
    db = database.get_db_connection()  # conexion propia de este hilo
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': config.KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',    # si el grupo es nuevo, empezar por el principio
        'enable.auto.commit': True,         # Kafka apunta solo hasta donde se ha leido
        # Si Central arranca antes que nadie haya escrito, que el topic se cree al suscribirse en vez de quedarse esperando a que aparezca
        'allow.auto.create.topics': True,
    })
    consumer.subscribe([config.TOPIC_REQ, config.TOPIC_TELEMETRY])
    print(f"[KAFKA] Escuchando {config.TOPIC_REQ} y {config.TOPIC_TELEMETRY} (group.id={config.KAFKA_GROUP_ID})")

    started_at = time.time()
    last_expire_check = 0

    try:
        while True:
            # Cada 2s, cerrar los riegos que la estacion no ha confirmado a tiempo
            if time.time() - last_expire_check >= 2:
                last_expire_check = time.time()
                try:
                    irrigation.expire_pending_irrigations(db)
                except Exception:
                    print("[RIEGO] Error cerrando riegos caducados:")
                    traceback.print_exc()

            # Esperar hasta 1s a que llegue un mensaje
            m = consumer.poll(1.0)
            if m is None:
                continue  # no llegs nada
            if m.error():
                if should_ignore_consumer_error(m.error(), started_at):
                    continue
                print(f"[KAFKA] Error del consumidor: {m.error()}")
                continue

            try:
                process_kafka_message(m.topic(), m.value(), db)
            except Exception:
                # Central no debe dejar de leer por un mensaje que falle
                print("[KAFKA] Error tratando un mensaje:")
                traceback.print_exc()
    finally:
        consumer.close()
        db.close()