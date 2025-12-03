import paho.mqtt.client as mqtt
import mysql.connector
import json
from datetime import datetime, timedelta

# -------------------------------
# Configuració MQTT
# -------------------------------
MQTT_BROKER = "arcxnujtdwkhj-ats.iot.us-east-1.amazonaws.com"  # Endpoint d'AWS
MQTT_PORT = 8883
MQTT_TOPIC = "iticbcn/espnode01/pub"

# -------------------------------
# Configuració MySQL
# -------------------------------
db = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin123",
    database="asistencia"
)
cursor = db.cursor()

# -------------------------------
# Configuració dispositiu (prova)
# -------------------------------
DISPOSITIU_ID = 1  # ID del dispositiu a la tabla dispositius

# -------------------------------
# Funcions auxiliars
# -------------------------------
def calcular_estat(usuari_id):
    """Calcula el tipus de lectura: entrada o sortida segons la última lectura"""
    cursor.execute("""
        SELECT tipus, data_hora
        FROM lecturas
        WHERE usuari_id = %s
        ORDER BY data_hora DESC
        LIMIT 1
    """, (usuari_id,))
    last_row = cursor.fetchone()
    now = datetime.now()

    if not last_row:
        return 'entrada'
    
    last_tipus, last_time = last_row
    # Evitar doble registre en menys de 5 minuts
    if (now - last_time).total_seconds() < 300:
        return None  # Indica que no registrar
    
    return 'entrada' if last_tipus == 'sortida' else 'sortida'

def registrar_lectura(usuari_id, tipus):
    """Inserta una nova lectura i retorna el seu id"""
    data_hora = datetime.now()
    sql = "INSERT INTO lecturas (usuari_id, dispositiu_id, tipus, data_hora) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (usuari_id, DISPOSITIU_ID, tipus, data_hora))
    db.commit()
    return cursor.lastrowid

def registrar_assistencia(usuari_id, lectura_id):
    """Inserta un registre en assistencies"""
    estat = 'present'  # Sol es registra present
    sql = "INSERT INTO assistencies (usuari_id, dispositiu_id, estat, lectura_id) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (usuari_id, DISPOSITIU_ID, estat, lectura_id))
    db.commit()
    return estat

# -------------------------------
# Conprovació de connexió MQTT
# -------------------------------
def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print(f"[ERROR] Connexió MQTT fallida, codi: {rc}")
    client.subscribe(MQTT_TOPIC)

# -------------------------------
# Funció al rebre missatge MQTT
# -------------------------------
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

    # Buscar usuari per UID
    cursor.execute("SELECT id, nom FROM usuaris WHERE rfid_uid=%s", (uid,))
    result = cursor.fetchone()

    if not result:
        print(f"[ERROR] UID no trobat a usuaris: {uid}")
        return

    usuari_id, nom_usuari = result

    # Determinar tipus (entrada/sortida) segons última lectura
    tipus = calcular_estat(usuari_id)
    if tipus is None:
        print(f"[AVÍS] Usuari {nom_usuari} ja ha passat la targeta fa poc.")
        return

    # Registrar lectura
    lectura_id = registrar_lectura(usuari_id, tipus)
    # Registrar assistència
    estat = registrar_assistencia(usuari_id, lectura_id)

    print(f"Assistència registrada per l'usuari: {nom_usuari} (id={usuari_id}), tipus={tipus}")

# -------------------------------
# Configurar client MQTT
# -------------------------------
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connexió TLS amb certificats
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
