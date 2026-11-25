
## Endpoints de `WM_Registry` (API REST sobre HTTPS)

### Tabla de endpoints

| Verbo y Ruta | Propósito | Auth | Entrada | Respuesta OK | Errores |
|---|---|---|---|---|---|
| `GET /health` | Comprobar servicio y acceso a BD | No | — | `200 OK`: `{"status": "ok", "service": "WM_Registry"}` | `500` |
| `POST /stations` | **Alta** de una WS y emisión de token | No | `{"id": "WS-01", "ubicacion": "Alicante"}` | `201 Created`: `{"id": "WS-01", "ubicacion": "Alicante", "token": "<secret>"}` | `400` (campos inválidos), `409` (ya existe), `500` |
| `DELETE /stations/<ws_id>` | **Baja** de una WS | `Authorization: Bearer <token>` | — | `200 OK`: `{"status": "unregistered", "id": "WS-01"}` | `401` (sin token o inválido), `404` (no existe), `500` |
| `GET /stations/<ws_id>` | Consultar si una WS está registrada (sin secretos) | No | — | `200 OK`: `{"id": "WS-01", "ubicacion": "Alicante", "status": "DESCONECTADA", "registered": true}` | `404` |
| `PUT /stations/<ws_id>` | Actualizar `ubicacion` de una WS ya registrada | `Authorization: Bearer <token>` | `{"ubicacion": "Madrid"}` | `200 OK`: `{"id": "WS-01", "ubicacion": "Madrid"}` | `400`, `401`, `404` |
| `POST /stations/<ws_id>/verify` | Verificar credencial (cubre la mención del enunciado pág. 3 de alta/baja/autenticación en Registry) | `Authorization: Bearer <token>` | — | `200 OK`: `{"valid": true, "id": "WS-01"}` | `401` (`{"valid": false}`), `404` |

### Decisiones de diseño importantes aquí (y cómo justificarlas en defensa)
1. **¿Por qué `POST /stations` devuelve `409 Conflict` si ya existe el `id`?** Si permitiera sobrescribir el token, cualquiera que conociera el ID `WS-01` podría secuestrar la estación. Para volver a registrarla, primero debe darse de baja acreditando su token.
2. **¿Por qué `DELETE` y `PUT` exigen `Authorization: Bearer <token>`?** Sin autenticación, cualquiera podría dar de baja estaciones ajenas (DoS). Al comparar `SHA-256(token)` con `token_hash` usando `hmac.compare_digest` (comparación en tiempo constante contra *timing attacks*), solo la estación dueña de la credencial puede modificarse o darse de baja.
3. **El token en claro solo existe en la respuesta `201` (cifrada por TLS).** `WM_Registry` genera `secrets.token_urlsafe(32)` (256 bits de entropía criptográfica, nunca `random`) y guarda únicamente su SHA-256. Si se filtra `wm_central.db`, el atacante no obtiene tokens útiles.
4. **Aviso en tiempo real a `WM_Central`:**
   Conviene acordar con Antonio un **único canal interno** en `WM_Central` (solo accesible desde la red interna de Docker, **sin** publicar el puerto en el host ni en AWS Security Group, p. ej. puerto `5051`):
   - `WM_Registry` (al hacer `DELETE`) → `POST http://wm_central:5051/internal/ws/<ws_id>/unregister` (Central cierra su socket TLS si estaba abierta y aborta riegos).
   - `API_Central` (cuando `WM_W` avisa) → `POST http://wm_central:5051/internal/ws/<ws_id>/weather` (Central aplica o levanta alerta climática en caliente).
   *(Si Central está caída en ese instante, `WM_Registry` captura el error de red, deja la baja hecha en SQLite y registra aviso en logs sin romperse.)*

---

## 3. Estrategia TLS con certificado autofirmado e IP dinámica de AWS EC2

### El problema real
En Python 3.12 (OpenSSL 3.x), `requests` con verificación real (`verify=...`, sin `verify=False`) y `ssl.create_default_context()` comprueban **dos cosas**:
1. **Cadena de confianza:** el certificado del servidor está firmado por una entidad en el *trust store* del cliente.
2. **Identidad del host (extensión X.509v3 `SAN` — *Subject Alternative Name*):** la IP o DNS al que te conectas debe aparecer en el SAN del certificado (OpenSSL 3 ignora `CN` para validar hosts).

Si el certificado lleva fija la IP pública de EC2 de hoy, en cuanto AWS Learner Lab reinicie la instancia y cambie la IP pública, todos los clientes fallarán con `SSLCertVerificationError: IP address mismatch`.

### Estrategia recomendada: Mini-PKI propia (CA fija + certificado de servidor dinámico al arrancar)

1. **Una sola vez (al preparar el proyecto): generáis vuestra propia CA** (`ca.key` + `ca.crt`):
   - `ca.crt` lleva `BasicConstraints(ca=True, path_length=0)` y `KeyUsage(key_cert_sign=True)` (obligatorio en OpenSSL 3.x).
   - **`ca.crt` (pública) es fija y nunca cambia.** Se distribuye a los PCs de laboratorio (`WM_WS_M`).
   - **`ca.key` (privada) NUNCA sale del servidor AWS** ni se distribuye a clientes (y se excluye en `.gitignore`).

2. **Cada vez que arranca AWS EC2 (con la nueva IP pública):**
   - Un script Python pequeño (`certs/gen_server_cert.py`, usando la librería `cryptography` que ya tenéis en [requirements.txt](file:///Users/pablodelacruz/Essentials/UNI/OBS_UNI/SISTEMAS%20DISTRIBUIDOS/PRA/Practica_3/P3_SD_Water_Management/requirements.txt#L6)) genera en <1 segundo un `server.key` y `server.crt` **firmados por `ca.key`/`ca.crt`**, metiendo en el **SAN**:
     - `IP:<IP_PUBLICA_EC2>` (pasada por parámetro/variable de entorno, igual que ya hacéis con Kafka)
     - `IP:127.0.0.1`
     - `DNS:localhost`
     - `DNS:wm_registry` y `DNS:wm_central`
   - Además incluye `BasicConstraints(ca=False)` y `ExtendedKeyUsage(SERVER_AUTH)`.
   - Ese mismo par (o uno por servicio, ambos firmados por `ca.crt`) lo usan `WM_Registry` (Gunicorn HTTPS) y `WM_Central` (socket TLS).

3. **En el cliente `WM_WS_M`:**
   - Recibe la ruta a `ca.crt` por parámetro CLI (ej. `--ca-cert certs/ca.crt`).
   - Contra `WM_Registry`: `requests.post(url, json=..., verify=ca_cert_path, timeout=5)`.
   - Contra `WM_Central`: `ssl.create_default_context(cafile=ca_cert_path)` + `wrap_socket(sock, server_hostname=ip_central)`.

### Por qué esta opción es mejor que las alternativas
- **Frente a certificado autofirmado sin CA:** no tenéis que copiar ningún fichero nuevo a los PCs del laboratorio cada vez que EC2 cambia de IP; en laboratorio `ca.crt` no se toca nunca.
- **Frente a DNS dinámico gratuito (DuckDNS, etc.):** cero dependencia de servicios externos ni problemas de caché DNS en la red de la UA durante la corrección.
- **Frente a `verify=False` o `check_hostname=False`:** validación criptográfica completa (firma + identidad del host) contra ataques MITM, y un argumento sólido de PKI/X.509 para la defensa.

---

## Puntos a confirmar (contigo y con Antonio) antes de pasar a código

1. **Idioma de claves JSON en APIs:** ¿Mantenemos `ubicacion` en JSON para ser coherentes con SQLite (`stations.ubicacion`) y Kafka de P2 (`duracion_s`, `caudal_lpm`), o preferís inglés (`location`) en las nuevas APIs REST?
2. **Persistencia del token en `WM_WS_M`:** cuando `WM_WS_M` se da de alta y recibe su `token`, ¿lo guardamos solo en memoria del proceso o también en un fichero local (p. ej. `.token_<WS_ID>`, permisos `0600`) para que si reinicias el Monitor en laboratorio pueda seguir autenticándose y darse de baja sin quedarse bloqueado por `409 Conflict`? *(Recomiendo fichero local por WS, y borrarlo al hacer baja.)*
3. **Confirmar con Antonio el canal interno `http://wm_central:5051/internal/...` y el esquema de `station_weather` y `audit_logs`** para que `WM_Registry` y `API_Central` hablen exactamente el mismo contrato desde el día 1.

Dime qué te parece este contrato y qué ajustes quieres hacer; en cuanto lo cerremos, pasamos al primer paso de implementación (script de generación de CA + certificado de servidor con `cryptography`).