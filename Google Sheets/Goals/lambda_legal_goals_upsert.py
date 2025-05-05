import os
import json
import gspread
from google.oauth2 import service_account
import psycopg2
import datetime
import pytz

REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1nfw8Xu7sUynkIIlGdkE0-7ZZDw90BGEglAPq2EVcKus"
SHEET_NAME = "Targets"
NY_TZ = pytz.timezone("America/New_York")

def log(msg):
    """Imprime un mensaje con timestamp."""
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_google_sheets_client():
    """Authenticate using the service account credentials stored in Lambda."""
    credentials_info = json.loads(os.environ['GOOGLE_SHEET_CREDENTIALS'])  

    # Authenticate using the service account
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=SCOPES)

    # Create a client to interact with Google Sheets
    client = gspread.authorize(credentials)
    
    return client


def insert_or_update_in_redshift(data):
    """Inserta o actualiza los datos de Google Sheets en Redshift."""
    try:
        # Conexión a Redshift
        conn = psycopg2.connect(
            dbname=REDSHIFT_CONFIG['dbname'],
            user=REDSHIFT_CONFIG['user'],
            password=REDSHIFT_CONFIG['password'],
            host=REDSHIFT_CONFIG['host'],
            port=REDSHIFT_CONFIG['port']
        )
        cursor = conn.cursor()

        # Consulta para insertar nuevos datos o actualizar existentes si el 'goal' es diferente
        upsert_query = """
        INSERT INTO legal.goals (team, week_first_day, week_number, employee, employee_email, goal)
        SELECT %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM legal.goals g
            WHERE g.employee_email = %s AND g.week_first_day = %s
        );

        UPDATE legal.goals
        SET goal = %s
        WHERE employee_email = %s AND week_first_day = %s AND goal != %s;
        """

        # Insertar o actualizar datos en Redshift
        for row in data:
            values = (
                row['Team'],
                row['1st Day Week'],
                row['week number'],
                row['Name'],
                row['Email'],
                row['Goal Productivity'],
                row['Email'],  
                row['1st Day Week'],  
                row['Goal Productivity'], 
                row['Email'],  
                row['1st Day Week'], 
                row['Goal Productivity']  
            )
            cursor.execute(upsert_query, values)

        # Confirmar y cerrar la conexión
        conn.commit()
        cursor.close()
        conn.close()
        log("Datos insertados o actualizados correctamente en Redshift.")
    except Exception as e:
        log(f"Error al insertar o actualizar en Redshift: {e}")


def lambda_handler(event, context):
    log("Iniciando proceso de extracción de datos...")
    
    # Obtener el cliente de Google Sheets utilizando la autenticación con la cuenta de servicio
    client = get_google_sheets_client()
    
    # Acceder a Google Sheets
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(SHEET_NAME)
    
    # Obtener todos los registros de la hoja de Google
    data = worksheet.get_all_records()
    
    # Insertar o actualizar los datos en Redshift
    insert_or_update_in_redshift(data)
    log("Proceso completado!")