# config.py, Constantes compartidas

import os

HEADER = 64          # tamano fijo de la cabecera que indica la longitud del mensaje
FORMAT = 'utf-8'     

DB_FILE = os.environ.get("DB_FILE", "wm_central.db") # config para railway

## KAFKA
TOPIC_REQ = "wm.central.solicitudes"   # topic donde Central recibe las peticiones de riego
TOPIC_TELEMETRY = "wm.central.telemetria"  # topic donde las estaciones envian respuestas, medidas y fin de riego
KAFKA_GROUP_ID = "central"             # config fija porque asi Kafka recuerda por donde iba tras un reinicio
MAX_AGE_S = 30                         # mensajes mas viejos que esto se descartan sin ejecutar
RESPONSE_TIMEOUT_S = 30                # si la estacion no confirma el inicio de un riego en este tiempo, se aborta
STARTUP_GRACE_S = 15                   # al arrancar, un topic que aun no existe no se muestra como error

# Destino ficticio: mensajes dirigidos a la propia Central (origen "CENTRAL")
# No salen a Kafka, solo se muestran por pantalla
TOPIC_SCREEN = "SCREEN"
# Campos obligatorios del sobre JSON
REQUIRED_FIELDS = {"tipo", "id_msg", "origen", "ts", "payload"}