# ─────────────────────────────────────────────
# db.py → Connexió a la base de dades MySQL i funcions SQL
# Descripció: centralitza totes les consultes a la BD.
# Conté una connexió global `db` i un cursor global `cursor`
# per compatibilitat amb el client original.
# Taules: usuari, lectura, assistencia, assignatura, horari, matricula
# ─────────────────────────────────────────────

import mysql.connector
from datetime import datetime

# Configuració MySQL 
db = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="admin123",
    database="asistencia"
)

# Cursor global (compatibilitat amb el codi original)
cursor = db.cursor()

# Identificador del dispositiu (si el vols centralitzar aquí)
DISPOSITIU_ID = 1

def get_cursor():
    """Retorna el cursor global"""
    return cursor

def find_user_by_rfid(uid):
    """Retorna (id, nom) de l'usuari amb aquest rfid_uid o None si no existeix."""
    cursor.execute("SELECT id, nom FROM usuari WHERE rfid_uid=%s", (uid,))
    row = cursor.fetchone()
    return row  # None o (id, nom)

def get_last_lectura(usuari_id):
    """Retorna l'última lectura (tipus, data_hora) per un usuari o None."""
    cursor.execute("""
        SELECT tipus, data_hora
        FROM lectura
        WHERE usuari_id = %s
        ORDER BY data_hora DESC
        LIMIT 1
    """, (usuari_id,))
    row = cursor.fetchone()
    return row  # None o (tipus, data_hora)

def get_horari_for_user(usuari_id, dia_setmana):
    """Retorna les files d'horari (assignatura_id, hora_inici, hora_fi) per usuari i dia."""
    cursor.execute("""
        SELECT h.assignatura_id, h.hora_inici, h.hora_fi
        FROM horari h
        JOIN matricula m ON h.assignatura_id = m.assignatura_id
        WHERE m.usuari_id = %s AND h.dia_setmana = %s
    """, (usuari_id, dia_setmana))
    rows = cursor.fetchall()
    return rows  # llista de tuples

def insert_lectura(usuari_id, dispositiu_id, tipus, data_hora):
    """Insereix una fila a 'lectura' i retorna l'id de la lectura inserida."""
    sql = "INSERT INTO lectura (usuari_id, dispositiu_id, tipus, data_hora) VALUES (%s, %s, %s, %s)"
    cursor.execute(sql, (usuari_id, dispositiu_id, tipus, data_hora))
    db.commit()
    return cursor.lastrowid

def insert_assistencia(usuari_id, dispositiu_id, estat, lectura_id, assignatura_id):
    """Insereix una fila a 'assistencia' (no retorna res)."""
    sql = "INSERT INTO assistencia (usuari_id, dispositiu_id, estat, lectura_id, assignatura_id) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(sql, (usuari_id, dispositiu_id, estat, lectura_id, assignatura_id))
    db.commit()

def get_assignatura_name(assignatura_id):
    """Retorna el nom de l'assignatura si existeix, o None."""
    if assignatura_id is None:
        return None
    cursor.execute("SELECT nom FROM assignatura WHERE id=%s", (assignatura_id,))
    row = cursor.fetchone()
    return row[0] if row else None
