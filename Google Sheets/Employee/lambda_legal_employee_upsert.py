import os
import json
import gspread
from google.oauth2 import service_account
import psycopg2
from datetime import datetime 
import pytz

REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])

# Google Sheets Config
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1nfw8Xu7sUynkIIlGdkE0-7ZZDw90BGEglAPq2EVcKus"
SHEET_NAME = "Staffing"
NY_TZ = pytz.timezone("America/New_York")

def get_local_time_iso():
    ny_tz = pytz.timezone('America/New_York')
    return datetime.now(ny_tz).isoformat()

def log(msg):
    """Print a message with timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_google_sheets_client():
    """Authenticate using the service account credentials stored in Lambda."""  
    credentials_json = os.environ.get('GOOGLE_SHEET_CREDENTIALS')

    if not credentials_json:
        raise ValueError("Google Sheets credentials not found in environment variables.")

    credentials_dict = json.loads(credentials_json)
    
    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict, scopes=SCOPES
    )
    
    client = gspread.authorize(credentials)
    return client

def convert_date(date_str):
    """Convert the date from DD/MM/YYYY to YYYY-MM-DD format."""
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, '%d/%m/%Y')
            return date_obj.strftime('%Y-%m-%d')  
        except ValueError:
            return None
    return None

def handle_empty(value):
    """Convert empty strings or None to NULL (None)."""
    if value == "" or value is None:
        return None
    return value.strip() if isinstance(value, str) else value

def insert_into_employee(data):
    """Inserts the data into the legal.employee table in Redshift."""
    try:
        # Hardcoded Redshift connection
        conn = psycopg2.connect(
            dbname=REDSHIFT_DBNAME,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT
        )
        cursor = conn.cursor()

        # SQL query to insert data into the employee table
        insert_query = """
        INSERT INTO legal.employee_staging (
        email, 
        name, 
        position, 
        tower, 
        team, 
        supervisor, 
        manager, 
        hire, 
        fire, 
        country, 
        birth, 
        phone, 
        work_phone, 
        schedule_daylight, 
        schedule_standard,
        lastmodifieddate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        current_timestamp = get_local_time_iso()


        for row in data:
            # Ensure the date fields are handled properly
            hire_date = convert_date(row.get('Hiring date', None))
            fire_date = convert_date(row.get('Last working day', None))
            birth_date = convert_date(row.get('Date of birth', None))

            # Handle empty or None values for other fields
            email = handle_empty(row.get('Email', ''))
            name = handle_empty(row.get('Employee name', ''))
            position = handle_empty(row.get('Position', ''))
            tower = handle_empty(row.get('Tower', ''))
            team = handle_empty(row.get('Team', ''))
            supervisor = handle_empty(row.get('Supervisor', ''))
            manager = handle_empty(row.get('Manager', ''))
            country = handle_empty(row.get('Country', ''))
            phone = handle_empty(row.get('Personal Phone Number', ''))
            work_phone = handle_empty(row.get('Company Phone Number', ''))
            schedule_daylight = handle_empty(row.get('Schedule Daylight (EST)', ''))
            schedule_standard = handle_empty(row.get('Schedule (EST)', ''))

            values = (
                email,
                name,
                position,
                tower,
                team,
                supervisor,
                manager,
                hire_date,
                fire_date,
                country,
                birth_date,
                phone,
                work_phone,
                schedule_daylight,
                schedule_standard,
                current_timestamp
            )

            cursor.execute(insert_query, values)

        
        conn.commit()

        cursor.execute("CALL legal.update_employee();")
        conn.commit()  
        
        cursor.close()
        conn.close()

        log("Datos insertados correctamente en la tabla legal.employee.")
    except Exception as e:
        log(f"Error al insertar en la tabla legal.employee: {e}")

def lambda_handler(event, context):
    log("Iniciando proceso de extracción de datos...")

    client = get_google_sheets_client()
    sheet = client.open_by_key(SHEET_ID)
    worksheet = sheet.worksheet(SHEET_NAME)
    data = worksheet.get_all_records()
    insert_into_employee(data)
    log("Proceso completado!")