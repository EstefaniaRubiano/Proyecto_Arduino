import paho.mqtt.client as mqtt
import mysql.connector
import json
from datetime import datetime, timedelta

# -------------------------------
# Configuració MQTT
# -------------------------------
MQTT_BROKER = "arcxnujtdwkhj-ats.iot.us-east-1.amazonaws.com"
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
DISPOSITIU_ID = 1

# -------------------------------
# Funcions auxiliars
# -------------------------------
def calcular_estat(usuari_id):
    """Determina entrada/salida según la última lectura"""
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
    if (now - last_time).total_seconds() < 300:  # Evita doble registro
        return None

    return 'entrada' if last_tipus == 'sortida' else 'sortida'

def determinar_assignatura(usuari_id):
    """Devuelve assignatura_id según hora, día y matrícula"""
    now = datetime.now()

    # Map inglés -> catalán
    mapa_dies = {
        'Monday': 'Dilluns',
        'Tuesday': 'Dimarts',
        'Wednesday': 'Dimecres',
        'Thursday': 'Dijous',
        'Friday': 'Divendres',
        'Saturday': 'Dissabte',
        'Sunday': 'Diumenge'
    }
    dia_setmana = mapa_dies[now.strftime('%A')]
    hora_actual = now.time()

    # Buscar asignaturas del usuario según matrícula
    cursor.execute("""
        SELECT h.assignatura_id, h.hora_inici, h.hora_fi
        FROM horari h
        JOIN matricula m ON h.assignatura_id = m.assignatura_id
        WHERE m.usuari_id = %s AND h.dia_setmana = %s
    """, (usuari_id, dia_setmana))
    rows = cursor.fetchall()

    for assignatura_id, hora_inici, hora_fi in rows:

        #  Convertir posibles valores tipo timedelta a time
        if isinstance(hora_inici, datetime):
            hora_inici = hora_inici.time()
        elif isinstance(hora_inici, timedelta):
            hora_inici = (datetime.min + hora_inici).time()

        if isinstance(hora_fi, datetime):
            hora_fi = hora_fi.time()
        elif isinstance(hora_fi, timedelta):
            hora_fi = (datetime.min + hora_fi).time()

        #  Comparación simple
        if hora_inici <= hora_actual <= hora_fi:
            return assignatura_id

    return None  # No hay asignatura en este horario

def registrar_lectura(usuari_id, tipus):
    data_hora = datetime.now()
    sql = "INSERT INTO lecturas (usuari_id, dispositiu_id, tipus, data_hora) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (usuari_id, DISPOSITIU_ID, tipus, data_hora))
    db.commit()
    return cursor.lastrowid

def registrar_assistencia(usuari_id, lectura_id, assignatura_id):
    estat = 'present'
    sql = "INSERT INTO assistencies (usuari_id, dispositiu_id, estat, lectura_id, assignatura_id) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (usuari_id, DISPOSITIU_ID, estat, lectura_id, assignatura_id))
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

    cursor.execute("SELECT id, nom FROM usuaris WHERE rfid_uid=%s", (uid,))
    result = cursor.fetchone()
    if not result:
        print(f"[ERROR] UID no trobat a usuaris: {uid}")
        return

    usuari_id, nom_usuari = result

    tipus = calcular_estat(usuari_id)
    if tipus is None:
        print(f"[AVÍS] Usuari {nom_usuari} ja ha passat la targeta fa poc.")
        return

    lectura_id = registrar_lectura(usuari_id, tipus)

    assignatura_id = determinar_assignatura(usuari_id)
    registrar_assistencia(usuari_id, lectura_id, assignatura_id)

    if assignatura_id:
        cursor.execute("SELECT nom FROM assignatures WHERE id=%s", (assignatura_id,))
        nom_assignatura = cursor.fetchone()[0]
        print(f"Assistència registrada per l'usuari: {nom_usuari} (id={usuari_id}), tipus={tipus}, assignatura: {nom_assignatura}")
    else:
        print(f"Assistència registrada per l'usuari: {nom_usuari} (id={usuari_id}), tipus={tipus}, assignatura: No disponible")

# -------------------------------
# Configurar client MQTT
# -------------------------------
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
