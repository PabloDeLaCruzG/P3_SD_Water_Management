# WM_Central - Servidor Concurrente

import threading
 
# Usamos argparse en vez de sys.argv manual porque valida tipos y numero de argumentos, con errores mas clasos
import argparse
 
from WM_Central.central import panel
from WM_Central.central import stations_feed
from WM_Central.central import database
from WM_Central.central import kafka_utils
from WM_Central.central import kafka_consumer
from WM_Central.central import socket_server
 
# Funcion que lee los argumentos de "sys.argv", y los valida
def parse_args():
    parser = argparse.ArgumentParser(
        description="WM_Central - Nucleo central del sistema Water Management"
    )
    parser.add_argument(
        "port",
        type=int,
        help="Puerto de escucha del servidor de sockets que ofrece a WM_WS_M"
    )
    parser.add_argument(
        "broker",
        type=str,
        help="IP:puerto del broker/bootstrap-server de Kafka, p. ej. localhost:9092"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
 
    # Socket de escucha
    server, addr = socket_server.create_server(args.port)
 
    # Crear tablas y dejar todas las estaciones como DESCONECTADAS
    database.init_db()
    database.reset_stations_on_startup()

    threading.Thread(target=panel.run_panel, daemon=True).start()
    threading.Thread(target=stations_feed.run_feed, daemon=True).start()
    
    # Kafka: productor y hilo consumidor en segundo plano
    kafka_utils.init_producer(args.broker)
    threading.Thread(target=kafka_consumer.kafka_loop, args=(args.broker,), daemon=True).start()
 
    print(f"Server ON - listening on {addr} | Kafka broker: {args.broker}")
 
    # Bucle principal: aceptar Monitores
    socket_server.run_server(server, addr)