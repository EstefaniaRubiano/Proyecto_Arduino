# ─────────────────────────────────────────────
# utils.py → Funcions auxiliars de validació i inserció a la DB
# Descripció: lògica de negoci (calcular estat, determinar assignatura, registrar lectura/assistència)
# ─────────────────────────────────────────────

from datetime import datetime, timedelta
from db import cursor, db, DISPOSITIU_ID, insert_lectura, insert_assistencia

# Determina si és entrada o sortida segons l'últim registre
def calcular_estat(usuari_id):
    cursor.execute("""
        SELECT tipus, data_hora
        FROM lectura
        WHERE usuari_id = %s
        ORDER BY data_hora DESC
        LIMIT 1
    """, (usuari_id,))
    last_row = cursor.fetchone()
    now = datetime.now()

    if not last_row:
        return 'entrada'

    last_tipus, last_time = last_row
    # Si last_time arriba com a string, possible conversió hauria d'afegir-se aquí si fos necessari
    if (now - last_time).total_seconds() < 300:
        return None

    return 'entrada' if last_tipus == 'sortida' else 'sortida'


# Detecta quina assignatura correspon segons dia i hora
def determinar_assignatura(usuari_id):
    now = datetime.now()
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

    cursor.execute("""
        SELECT h.assignatura_id, h.hora_inici, h.hora_fi
        FROM horari h
        JOIN matricula m ON h.assignatura_id = m.assignatura_id
        WHERE m.usuari_id = %s AND h.dia_setmana = %s
    """, (usuari_id, dia_setmana))
    rows = cursor.fetchall()

    for assignatura_id, hora_inici, hora_fi in rows:
        if isinstance(hora_inici, datetime):
            hora_inici = hora_inici.time()
        elif isinstance(hora_inici, timedelta):
            hora_inici = (datetime.min + hora_inici).time()
        if isinstance(hora_fi, datetime):
            hora_fi = hora_fi.time()
        elif isinstance(hora_fi, timedelta):
            hora_fi = (datetime.min + hora_fi).time()

        if hora_inici <= hora_actual <= hora_fi:
            return assignatura_id

    return None


# Registra lectura en la taula "lectura"
def registrar_lectura(usuari_id, tipus):
    data_hora = datetime.now()
    return insert_lectura(usuari_id, DISPOSITIU_ID, tipus, data_hora)


# Registra assistència en la taula "assistencia"
def registrar_assistencia(usuari_id, lectura_id, assignatura_id):
    estat = 'present'
    insert_assistencia(usuari_id, DISPOSITIU_ID, estat, lectura_id, assignatura_id)
    return estat
