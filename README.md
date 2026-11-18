# Water Management - Sistemas Distribuidos 26/27

Autores: ver Nombres.txt

## Arquitectura
- WM_Central (sockets 5050 + panel web 8080), Kafka y SQLite: AWS Learner Lab (EC2).
- WM_WS (Monitor + Engine) y WM_FO: PCs del laboratorio, siempre en Docker.
- Imagenes en Docker Hub (repo pablodlc/practica-aws):
  - `latest` = Central, `ws` = Monitor y Engine, `fo` = Field Operator.

## CI/CD
VS Code -> GitHub -> GitHub Actions (`.github/workflows/deploy.yml`) -> Docker Hub.
El despliegue en AWS es manual: `docker pull` + `docker run` en la instancia.

## Nube (AWS EC2 Ubuntu, grupo de seguridad con 22, 5050, 8080 y 9092 abiertos)
Kafka (instancia Broker-Kafka), sustituyendo IP_KAFKA por su IPv4 publica:
```
sudo docker run -d --name kafka -p 9092:9092 \
 -e KAFKA_HEAP_OPTS="-Xmx256m -Xms256m" \
 -e KAFKA_CFG_NODE_ID=0 -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
 -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
 -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
 -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
 -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://IP_KAFKA:9092 \
 bitnamilegacy/kafka:3.7.0
```
Central (instancia Central-WM):
```
sudo docker run -dit --name central -p 5050:5050 -p 8080:8080 \
 -e KAFKA_BROKER=IP_KAFKA:9092 -v central-data:/data pablodlc/practica-aws:latest
```
Panel: `http://IP_CENTRAL:8080`

## Laboratorio
```
docker network create wm
# PC LAB 2: estacion WS1 (Monitor y Engine, en dos terminales)
docker run -it --rm --name monitor --network wm pablodlc/practica-aws:ws python WM_WS_M.py 6000 5050 IP_CENTRAL WS1 Alicante
docker run -it --rm --name engine --network wm pablodlc/practica-aws:ws python WM_WS_E.py IP_KAFKA:9092 monitor:6000
# PC LAB 1: operador
docker run -it --rm pablodlc/practica-aws:fo python WM_FO.py IP_KAFKA:9092 FO1
```
Una segunda estacion usa otros nombres y otro id (monitor2, engine2, WS2).

## Notas
- La IP publica de las instancias cambia si se paran: hay que recrear Kafka con la
  nueva IP en ADVERTISED_LISTENERS y relanzar Central con la nueva IP de Kafka.
- Los relojes de las maquinas deben estar sincronizados (los mensajes caducan a 30 s).