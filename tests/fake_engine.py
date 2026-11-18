"""Engine FALSO de prueba (no forma parte de la entrega): hace de WM_WS_E solo en la parte de Kafka.
Sirve para probar Central y WM_FO sin esperar al Engine real, y como ejemplo exacto de lo que
el Engine real tiene que leer y enviar.

Uso: python3 fake_engine.py <broker> <ws_id> [--reject] [--flow <litros_por_minuto>]
  p. ej. python3 fake_engine.py localhost:9092 WS-04
         python3 fake_engine.py localhost:9092 WS-04 --reject     (rechaza todos los riegos)

Lee    wm.ws.<ws_id>           -> INICIAR_RIEGO, y DETENER_RIEGO (para un riego en marcha)
Envia  wm.central.telemetria   -> RESPUESTA_INICIO, TELEMETRIA (cada segundo), FIN_RIEGO
"""

import argparse
import json
import random
import time
import uuid

from confluent_kafka import Consumer, Producer

MAX_AGE_S = 30   # una orden mas vieja que esto se ignora (lo pide el protocolo)
TOPIC_TELEMETRY = "wm.central.telemetria"


def publish(producer, ws_id, msg_type, payload):
    # Envuelve el payload en el sobre comun y lo envia al topic de telemetria de Central
    envelope = {
        "tipo": msg_type,
        "id_msg": str(uuid.uuid4()),
        "origen": f"WS:{ws_id}",
        "ts": int(time.time()),
        "payload": payload,
    }
    producer.produce(TOPIC_TELEMETRY, value=json.dumps(envelope).encode("utf-8"))
    producer.poll(0)


def stop_requested(consumer, ws_id, request_id, wait_s):
    # Espera wait_s mirando el buzon. Devuelve el motivo si llega un DETENER_RIEGO de ESTE riego, o None.
    # Cualquier otro mensaje que llegue durante el riego se descarta.
    m = consumer.poll(wait_s)
    if m is None or m.error():
        return None
    try:
        msg = json.loads(m.value().decode("utf-8"))
    except ValueError:
        return None
    if msg.get("tipo") != "DETENER_RIEGO":
        print(f"[ENGINE] mensaje ignorado durante el riego: {msg.get('tipo')}")
        return None
    # Solo vale si es de este riego: una orden de otro riego no se obedece
    payload = msg.get("payload", {})
    if payload.get("id_solicitud") != request_id:
        print(f"[ENGINE] DETENER_RIEGO de otro riego ({payload.get('id_solicitud')}), ignorado")
        return None
    return payload.get("motivo") or "ORDEN"


def run_irrigation(producer, ws_id, request_id, duration_s, reject=False, flow_lpm=12.5, tick_s=1.0,
                   consumer=None):
    # Simula un riego completo. tick_s es la pausa entre medidas (1 s de verdad; mas corta en tests).
    # Si se pasa consumer, entre medida y medida se mira el buzon por si llega un DETENER_RIEGO

    # 1) Contestar si se puede empezar
    if reject:
        publish(producer, ws_id, "RESPUESTA_INICIO", {
            "ws_id": ws_id, "id_solicitud": request_id,
            "aceptado": False, "motivo": "Estacion en mantenimiento (simulado)",
        })
        print(f"[ENGINE] {request_id}: riego rechazado")
        return
    publish(producer, ws_id, "RESPUESTA_INICIO", {
        "ws_id": ws_id, "id_solicitud": request_id, "aceptado": True, "motivo": "",
    })
    print(f"[ENGINE] {request_id}: riego aceptado, regando {duration_s}s")

    # 2) Una medida por segundo. Caudal en litros/minuto, volumen acumulado en litros
    volume_l = 0.0
    end_reason = "TIEMPO"
    elapsed_s = duration_s
    for second in range(1, duration_s + 1):
        if consumer is None:
            time.sleep(tick_s)
        else:
            # Esta espera de tick_s sirve a la vez de pausa y de escucha de ordenes
            stop_reason = stop_requested(consumer, ws_id, request_id, tick_s)
            if stop_reason:
                end_reason = stop_reason
                elapsed_s = second - 1
                print(f"[ENGINE] {request_id}: DETENER_RIEGO recibido ({stop_reason}), parando")
                break
        current_flow = round(flow_lpm + random.uniform(-0.5, 0.5), 1)
        volume_l += current_flow / 60   # litros/minuto -> litros en 1 segundo
        publish(producer, ws_id, "TELEMETRIA", {
            "ws_id": ws_id, "id_solicitud": request_id,
            "caudal_lpm": current_flow, "volumen_l": round(volume_l, 2),
        })

    # 3) Avisar del fin. Va en el mismo topic que la telemetria, asi llega despues de la ultima medida
    publish(producer, ws_id, "FIN_RIEGO", {
        "ws_id": ws_id, "id_solicitud": request_id, "motivo": end_reason,
        "volumen_total_l": round(volume_l, 2), "duracion_s": elapsed_s,
    })
    producer.flush(5)
    print(f"[ENGINE] {request_id}: riego terminado, {volume_l:.2f} L")


def handle_message(producer, ws_id, raw, reject, flow_lpm, tick_s=1.0, consumer=None):
    # Trata un mensaje de wm.ws.<ws_id>
    try:
        msg = json.loads(raw.decode("utf-8"))
    except ValueError:
        return

    if msg.get("tipo") == "DETENER_RIEGO":
        # Si llega aqui es que no hay ningun riego en marcha (los de un riego en marcha los lee run_irrigation)
        print("[ENGINE] DETENER_RIEGO recibido sin riego en marcha, ignorado")
        return
    if msg.get("tipo") != "INICIAR_RIEGO":
        print(f"[ENGINE] mensaje ignorado: {msg.get('tipo')}")
        return

    # Orden caducada: se escribio mientras la estacion estaba caida, no se ejecuta
    age = time.time() - msg.get("ts", 0)
    if age > MAX_AGE_S:
        print(f"[ENGINE] INICIAR_RIEGO caducado ({age:.0f}s), ignorado")
        return

    payload = msg["payload"]
    run_irrigation(producer, ws_id, payload["id_solicitud"], payload["duracion_s"],
                   reject, flow_lpm, tick_s, consumer)


def main():
    parser = argparse.ArgumentParser(description="Engine falso de prueba")
    parser.add_argument("broker", help="IP:puerto del broker, p. ej. localhost:9092")
    parser.add_argument("ws_id", help="ID de la estacion, p. ej. WS-04")
    parser.add_argument("--reject", action="store_true", help="rechazar todos los riegos")
    parser.add_argument("--flow", type=float, default=12.5, help="caudal en litros/minuto")
    args = parser.parse_args()

    topic = f"wm.ws.{args.ws_id}"
    producer = Producer({"bootstrap.servers": args.broker})
    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": f"ws-{args.ws_id}",          # un lector por buzon (ver protocolo)
        "auto.offset.reset": "earliest",
        "allow.auto.create.topics": True,
    })
    consumer.subscribe([topic])
    print(f"[ENGINE] {args.ws_id} esperando ordenes en {topic} (Ctrl+C para salir)")

    try:
        while True:
            m = consumer.poll(1.0)
            if m is None or m.error():
                continue
            handle_message(producer, args.ws_id, m.value(), args.reject, args.flow, consumer=consumer)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
