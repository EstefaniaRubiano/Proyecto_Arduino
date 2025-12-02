from fastapi import FastAPI
import mysql.connector
from fastapi.responses import JSONResponse
import json

app = FastAPI()

# ---------------------------------------------
# Funció per connectar amb MySQL
# ---------------------------------------------
def obtenir_connexio_bd():
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin123",
        database="asistencia"
    )

# ---------------------------------------------
# Endpoint 1: Llistar totes les assistències
# ---------------------------------------------
@app.get("/assistencies")
def llistar_assistencies():
    db = obtenir_connexio_bd()
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT a.id, u.nom as usuari, d.nom as dispositiu, a.data_hora, a.estat
        FROM assistencies a
        JOIN usuaris u ON a.usuari_id = u.id
        JOIN dispositius d ON a.dispositiu_id = d.id
        ORDER BY a.data_hora DESC
    """)
    resultats = cursor.fetchall()
    cursor.close()
    db.close()
    return JSONResponse(content=json.loads(json.dumps(resultats, default=str, indent=4)))

# ---------------------------------------------
# Endpoint 2: Llistar assistències per usuari
# ---------------------------------------------
@app.get("/assistencies/{usuari_id}")
def assistencies_usuari(usuari_id: int):
    db = obtenir_connexio_bd()
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT a.id, u.nom as usuari, d.nom as dispositiu, a.data_hora, a.estat
        FROM assistencies a
        JOIN usuaris u ON a.usuari_id = u.id
        JOIN dispositius d ON a.dispositiu_id = d.id
        WHERE u.id = %s
        ORDER BY a.data_hora DESC
    """, (usuari_id,))
    resultats = cursor.fetchall()
    cursor.close()
    db.close()
    return JSONResponse(content=json.loads(json.dumps(resultats, default=str, indent=4)))

# ---------------------------------------------
# Endpoint 3: Llistar tots els usuaris
# ---------------------------------------------
@app.get("/usuaris")
def llistar_usuaris():
    db = obtenir_connexio_bd()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT id, nom, email, rfid_uid, rol_id FROM usuaris")
    resultats = cursor.fetchall()
    cursor.close()
    db.close()
    return JSONResponse(content=json.loads(json.dumps(resultats, default=str, indent=4)))

# ---------------------------------------------
# Endpoint 4: Llistar dispositius
# ---------------------------------------------
@app.get("/dispositius")
def llistar_dispositius():
    db = obtenir_connexio_bd()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT id, nom, ubicacio FROM dispositius")
    resultats = cursor.fetchall()
    cursor.close()
    db.close()
    return JSONResponse(content=json.loads(json.dumps(resultats, default=str, indent=4)))
