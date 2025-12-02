import paho.mqtt.client as mqtt
import mysql.connector
import json

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
# Conprovació de connexió
# -------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connectat a MQTT correctament")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscrit al topic: {MQTT_TOPIC}")
    else:
        print(f"Error de connexió, codi: {rc}")

# -------------------------------
# Funció al rebre missatge MQTT
# -------------------------------
def on_message(client, userdata, message):
    payload = message.payload.decode()
    print(f"Missatge rebut: {payload}")

    try:
        data = json.loads(payload)
        uid = data.get("tag", None)
    except:
        uid = None

    if uid:
        # Buscar usuari per UID
        cursor.execute("SELECT id FROM usuaris WHERE rfid_uid=%s", (uid,))
        result = cursor.fetchone()

        if result:
            usuari_id = result[0]
            # Obtener el nom del usuari
            cursor.execute("SELECT nom FROM usuaris WHERE id=%s", (usuari_id,))
            nom_result = cursor.fetchone()
            nom_usuari = nom_result[0] if nom_result else "Desconegut"

            # Insertar a assistencies
            cursor.execute(
                "INSERT INTO assistencies (usuari_id, dispositiu_id, estat) VALUES (%s, %s, %s)",
                (usuari_id, DISPOSITIU_ID, 'present')
            )
            db.commit()
            #print(f"Assistència guardada per l'usuari_id={usuari_id}")
            print(f"Assistència guardada per l'usuari: {nom_usuari} (id={usuari_id})")
        else:
            print(f"UID no trobat a usuaris: {uid}")
    else:
        print("Error: UID no rebut")

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

client.connect(MQTT_BROKER, MQTT_PORT)
client.subscribe(MQTT_TOPIC)

print("Esperant missatges MQTT...")
client.loop_forever()
