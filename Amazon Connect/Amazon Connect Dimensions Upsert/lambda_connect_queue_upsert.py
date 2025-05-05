import boto3
import psycopg2
import json
from datetime import datetime
import pytz
import os

# AWS Configuration
instance_id = os.getenv("INSTANCE_ID")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("REGION")

# Redshift Configuration
REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN")

# AWS clients
connect_client = boto3.client(
    'connect',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# Timezone for New York
ny_tz = pytz.timezone('America/New_York')

# Function to convert UTC time to New York time
def convert_to_ny_time(utc_time):
    if utc_time:
        utc_time = utc_time.replace(tzinfo=pytz.utc)  
        return utc_time.astimezone(ny_tz)  
    return None

# Function to get all queues from Amazon Connect
def get_all_queues(instance_id):
    queues = []
    next_token = None

    while True:
        if next_token:
            response = connect_client.list_queues(
                InstanceId=instance_id,
                NextToken=next_token
            )
        else:
            response = connect_client.list_queues(
                InstanceId=instance_id
            )

        # Extract and store the required queue information
        for queue in response['QueueSummaryList']:
            queue_id = queue.get('Id', None)
            queue_name = queue.get('Name', None)
            last_modified_time = queue.get('LastModifiedTime', None)

            # Convert the LastModifiedTime to New York time if available
            if last_modified_time is not None:
                last_modified_time = convert_to_ny_time(last_modified_time)
                if last_modified_time:
                    last_modified_time = last_modified_time.strftime('%Y-%m-%d %H:%M:%S')  

            queues.append({
                'queue_id': queue_id,  
                'queue_name': queue_name,  
                'last_modified': last_modified_time  
            })

        # Check if there are more queues to fetch
        next_token = response.get('NextToken', None)
        if not next_token:
            break

    return queues

# Function to insert or update queues in Redshift
def upsert_queues_in_redshift(queues):
    try:
        conn = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = conn.cursor()

        for queue in queues:
            # Check if the queue already exists in the database
            cursor.execute("SELECT COUNT(*) FROM connect.dim_queues WHERE queue_id = %s", (queue['queue_id'],))
            queue_exists = cursor.fetchone()[0]

            # If the queue exists, perform an UPDATE; otherwise, perform an INSERT
            if queue_exists > 0:
                cursor.execute("""
                    UPDATE connect.dim_queues
                    SET queue_name = %s, last_modified = %s
                    WHERE queue_id = %s
                """, (queue['queue_name'], queue['last_modified'], queue['queue_id']))
                print(f"Updated queue {queue['queue_id']}")
            else:
                cursor.execute("""
                    INSERT INTO connect.dim_queues (queue_id, queue_name, last_modified)
                    VALUES (%s, %s, %s)
                """, (queue['queue_id'], queue['queue_name'], queue['last_modified']))
                print(f"Inserted queue {queue['queue_id']}")

        # Commit the changes to the database
        conn.commit()

        # Optionally, execute a stored procedure in Redshift to update any other necessary data
        #cursor.execute("CALL update_dim_queues_data()")
        #print("Procedure executed")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error during upsert operation: {e}")

# Lambda Handler
def lambda_handler(event, context):
    # Fetch queues from Amazon Connect
    queues = get_all_queues(instance_id)

    # Insert/Update queues in Redshift
    upsert_queues_in_redshift(queues)

    return {'status': 'ok', 'message': 'Queues have been updated in Redshift'}