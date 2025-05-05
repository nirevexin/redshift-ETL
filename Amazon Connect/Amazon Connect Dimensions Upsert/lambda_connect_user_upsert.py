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

# Function to get all users from Amazon Connect
def get_all_users(instance_id):
    users = []
    next_token = None
    print("Starting to fetch users from Amazon Connect...")

    while True:
        # Fetch users with pagination
        if next_token:
            response = connect_client.list_users(
                InstanceId=instance_id,
                NextToken=next_token
            )
        else:
            response = connect_client.list_users(
                InstanceId=instance_id
            )

        print(f"Fetched {len(response['UserSummaryList'])} users...")

        # Extract user information
        for user in response['UserSummaryList']:
            user_id = user['Id']
            user_name = user['Username']
            
            # Get first and last name, and last modified time using describe_user API
            first_name, last_name, last_modified_time = get_user_details(user_id)
            print(f"Processing user: {user_name} ({user_id})")

            # Add the user details with lowercase keys as required
            users.append({
                'user_id': user_id,
                'user_email': user_name,
                'user_name': first_name,
                'user_lastname': last_name,
                'last_modified': last_modified_time
            })

        # Check if there are more users to fetch
        next_token = response.get('NextToken', None)
        if not next_token:
            break

    print(f"Total users fetched: {len(users)}")
    return users

def get_user_details(user_id):
    """Fetch detailed information about a user including first and last name and last modified time."""
    print(f"Fetching details for user {user_id}...")
    try:
        response = connect_client.describe_user(
            InstanceId=instance_id,
            UserId=user_id
        )
        # Get user first and last name from the IdentityInfo
        user_data = response['User']
        first_name = user_data['IdentityInfo'].get('FirstName', None)
        last_name = user_data['IdentityInfo'].get('LastName', None)
        last_modified_time = user_data.get('LastModifiedTime', None)

        # Format the LastModifiedTime to string if it's available
        if last_modified_time is not None:
            last_modified_time = last_modified_time.strftime('%Y-%m-%d %H:%M:%S')

        print(f"User {user_id} details fetched: {first_name} {last_name} | Last Modified: {last_modified_time}")
        return first_name, last_name, last_modified_time
    except Exception as e:
        print(f"Error fetching details for user {user_id}: {e}")
        return None, None, None

# Function to insert or update users in Redshift
def upsert_users_in_redshift(users):
    print("Starting upsert operation in Redshift...")
    try:
        conn = psycopg2.connect(**REDSHIFT_CONFIG)
        cursor = conn.cursor()

        for user in users:
            print(f"Checking if user {user['user_id']} exists in Redshift...")
            # Check if the user already exists in the database
            cursor.execute("SELECT COUNT(*) FROM connect.dim_users WHERE user_id = %s", (user['user_id'],))
            user_exists = cursor.fetchone()[0]

            # If the user exists, perform an UPDATE; otherwise, perform an INSERT
            if user_exists > 0:
                print(f"User {user['user_id']} exists. Updating record...")
                cursor.execute("""
                    UPDATE connect.dim_users
                    SET user_name = %s, user_lastname = %s, last_modified = %s
                    WHERE user_id = %s
                """, (user['user_name'], user['user_lastname'], user['last_modified'], user['user_id']))
                print(f"Updated user {user['user_id']}")
            else:
                print(f"User {user['user_id']} does not exist. Inserting new record...")
                cursor.execute("""
                    INSERT INTO connect.dim_users (user_id, user_email, user_name, user_lastname, last_modified)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user['user_id'], user['user_email'], user['user_name'], user['user_lastname'], user['last_modified']))
                print(f"Inserted user {user['user_id']}")

        # Commit the changes to the database
        conn.commit()
        print("Upsert operation completed successfully.")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error during upsert operation: {e}")

# Lambda Handler
def lambda_handler(event, context):
    print("Starting script execution...")

    # Fetch users from Amazon Connect
    users = get_all_users(instance_id)

    # Insert/Update users in Redshift
    upsert_users_in_redshift(users)

    print('Users have been updated in Redshift')

    return {
        'status': 'ok',
        'message': 'Users have been updated in Redshift'
    }