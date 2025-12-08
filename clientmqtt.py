# ─────────────────────────────────────────────
# client.py → Gestió MQTT: rep missatges de l'ESP32 i crida funcions
# Descripció: subscriu al topic MQTT i processa els payloads (UIDs).
# ─────────────────────────────────────────────

import paho.mqtt.client as mqtt
import json
from datetime import datetime

import db
from utils import calcular_estat, determinar_assignatura, registrar_lectura, registrar_assistencia

# Configuració MQTT
MQTT_BROKER = "arcxnujtdwkhj-ats.iot.us-east-1.amazonaws.com"
MQTT_PORT = 8883
MQTT_TOPIC = "iticbcn/espnode01/pub"


# Quan es connecta al broker MQTT
def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"[ERROR] Connexió MQTT fallida, codi: {rc}")
    client.subscribe(MQTT_TOPIC)


# Quan arriba un missatge MQTT
def on_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode())
        uid = data.get("tag", None)
    except Exception as e:
        print(f"[ERROR] Payload invàlid: {e}")
        return

    if not uid:
        print("[ERROR] UID no rebut")
        return

    result = db.find_user_by_rfid(uid)
    if not result:
        print(f"[ERROR] UID no trobat a usuari: {uid}")
        return

    usuari_id, nom_usuari = result
    tipus = calcular_estat(usuari_id)

    if tipus is None:
        print(f"[AVÍS] Usuari {nom_usuari} ja ha passat la targeta fa poc.")
        return

    data_hora = datetime.now()
    lectura_id = registrar_lectura(usuari_id, tipus)
    assignatura_id = determinar_assignatura(usuari_id)
    registrar_assistencia(usuari_id, lectura_id, assignatura_id)

    if assignatura_id:
        nom_assignatura = db.get_assignatura_name(assignatura_id)
        print(f"Assistència registrada per {nom_usuari}, tipus={tipus}, assignatura: {nom_assignatura}")
    else:
        print(f"Assistència registrada per {nom_usuari}, tipus={tipus}, assignatura: No disponible")


# Configuració del client MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.tls_set(
    ca_certs="certs/AmazonRootCA1.pem",
    certfile="certs/certificate.pem.crt",
    keyfile="certs/private.pem.key"
)

try:
    client.connect(MQTT_BROKER, MQTT_PORT)
    print("Esperant missatges MQTT...")
    client.loop_forever()
except Exception as e:
    print(f"[ERROR] No s'ha pogut connectar a MQTT: {e}")
