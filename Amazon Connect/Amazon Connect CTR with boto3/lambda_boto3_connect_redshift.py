import boto3
import json 
import os
import pytz
import psycopg2
from psycopg2.extras import execute_values
import datetime
import time as time_module
from datetime import timedelta

# AWS Connect Configuration
INSTANCE_ID = os.getenv("INSTANCE_ID")
AWS_REGION = os.getenv("REGION")
NY_TZ = pytz.timezone("America/New_York")

# Redshift connection config
REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])

# Yesterday
"""
today_ny = datetime.now(NY_TZ).date()
yesterday_ny = today_ny - timedelta(days=1)
START_DATE_NY = NY_TZ.localize(datetime.combine(yesterday_ny, datetime.min.time()))
END_DATE_NY = NY_TZ.localize(datetime.combine(yesterday_ny, datetime.max.time()))
START_DATE_UTC = START_DATE_NY.astimezone(pytz.utc)
END_DATE_UTC = END_DATE_NY.astimezone(pytz.utc)
"""

# By Hour Periods 
now_ny = datetime.datetime.now(NY_TZ) # time right now in NY

def get_previous_interval_bounds(now_ny, tz):
    current_time = now_ny.replace(minute=0, second=0, microsecond=0)
    hour = current_time.hour

    if hour == 0:
        start_local = tz.localize(datetime.datetime.combine(now_ny.date() - timedelta(days=1), datetime.time(22, 0)))
        end_local = tz.localize(datetime.datetime.combine(now_ny.date(), datetime.time(0, 0)))
        interval_label = "22-00"
    else:
        start_hour = hour - 2
        start_local = tz.localize(datetime.datetime.combine(now_ny.date(), datetime.time(start_hour, 0)))
        end_local = tz.localize(datetime.datetime.combine(now_ny.date(), datetime.time(hour, 0)))
        interval_label = f"{start_hour:02d}-{hour:02d}"

    end_local += timedelta(seconds=1)

    # Convert to UTC
    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)

    return start_utc, end_utc, interval_label


START_DATE_UTC, END_DATE_UTC, interval_label = get_previous_interval_bounds(now_ny, NY_TZ)

client = boto3.client("connect", region_name=AWS_REGION)

def parse_datetime(timestamp):
    eastern_tz = pytz.timezone("America/New_York")
    if not timestamp:
        return None
    try:
        if isinstance(timestamp, datetime.datetime):
            dt_utc = timestamp.replace(tzinfo=pytz.utc)
        else:
            dt_utc = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        return dt_utc.astimezone(eastern_tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Failed to parse {timestamp}: {e}")
        return None


def get_contact_details(contact_id):
    try:
        response = client.describe_contact(InstanceId=INSTANCE_ID, ContactId=contact_id)
        contact = response.get('Contact', {})
        return (
            contact.get('CustomerEndpoint', {}).get('Address', None),
            contact.get('TotalPauseCount', 0),
            contact.get('TotalPauseDurationInSeconds', 0),
            parse_datetime(contact.get('LastUpdateTimestamp', None)),
            parse_datetime(contact.get('QueueInfo', {}).get('EnqueueTimestamp', None)),
            contact.get('QueueTimeAdjustmentSeconds', 0),
            parse_datetime(contact.get('ConnectedToSystemTimestamp', None))
        )
    except Exception as e:
        print(f"Error fetching contact {contact_id}: {e}")
        return (None, 0, 0, None, None, None, None)

def fetch_completed_calls(start_time, end_time):
    print(f"Fetching completed calls from {start_time} to {end_time} (UTC)...")
    rows = []
    next_token = None
    total_fetched = 0

    while True:
        params = {
            "InstanceId": INSTANCE_ID,
            "MaxResults": 100,
            "TimeRange": {
                "Type": "INITIATION_TIMESTAMP",
                "StartTime": start_time,
                "EndTime": end_time
            }
        }
        if next_token:
            params["NextToken"] = next_token

        try:
            response = client.search_contacts(**params)
        except client.exceptions.TooManyRequestsException:
            print("Too many requests, retrying...")
            time_module.sleep(2)
            continue
        except Exception as e:
            print(f"Error: {e}")
            break

        contacts_batch = response.get("Contacts", [])
        total_fetched += len(contacts_batch)
        print(f"🔹 Retrieved {len(contacts_batch)} contacts (total: {total_fetched})")
        
        for idx, contact in enumerate(contacts_batch):
            contact_id = contact.get("Id")
            if not contact_id:
                continue
            print(f"Processing contact [{idx + 1}] → {contact_id}")
            
            init_contact_id = contact.get('InitialContactId', None)
            prev_contact_id = contact.get('PreviousContactId', None)
            next_contact_id = None
            channel = contact.get("Channel", None)
            init_method = contact.get('InitiationMethod')

            raw_disconn = contact.get("DisconnectTimestamp")
            if not raw_disconn:
                continue
            raw_agent_conn = contact.get('AgentInfo', {}).get('ConnectedToAgentTimestamp')

            contact_duration = ((raw_disconn - raw_agent_conn).total_seconds() if raw_disconn and raw_agent_conn else None)
            init_time = parse_datetime(contact.get("InitiationTimestamp"))
            disconn_time = parse_datetime(raw_disconn)
            disconn_reason = None
            agent_username = None
            agent_conn_att = None
            agent_afw_start = None
            agent_afw_end = None
            agent_afw_duration = None
            agent_interact_duration = None
            agent_longest_hold = None
            queue_name = None
            out_queue_time = None
            customer_voice = None
            sys_phone = None
            agent_conn = parse_datetime(contact.get('AgentInfo', {}).get('ConnectedToAgentTimestamp', None))
            agent_id = contact.get("AgentInfo", {}).get("Id", None)
            queue_id = contact.get("QueueInfo", {}).get("Id", None)
            

            (
                customer_phone,
                agent_holds,
                customer_hold_duration,
                last_update_time,
                in_queue_time,
                queue_duration,
                conn_to_sys
            ) = get_contact_details(contact_id)

            rows.append((
                init_contact_id, prev_contact_id, contact_id, next_contact_id,
                channel, init_method, init_time, disconn_time, disconn_reason,
                last_update_time, agent_conn, agent_id, agent_username,
                agent_conn_att, agent_afw_start, agent_afw_end, agent_afw_duration,
                agent_interact_duration, agent_holds, agent_longest_hold,
                queue_id, queue_name, in_queue_time, out_queue_time, queue_duration,
                customer_voice, customer_hold_duration, contact_duration,
                sys_phone, conn_to_sys, customer_phone
            ))

            time_module.sleep(0.05)

        next_token = response.get("NextToken")
        if not next_token:
            break

    print(f"Total fetched: {total_fetched}")
    return rows


def insert_into_redshift(rows):
    if not rows:
        print("No rows to insert.")
        return

    insert_sql = """
        INSERT INTO connect.f_calls_staging (
            init_contact_id, prev_contact_id, contact_id, next_contact_id,
            channel, init_method, init_time, disconn_time, disconn_reason,
            last_update_time, agent_conn, agent_id, agent_username,
            agent_conn_att, agent_afw_start, agent_afw_end, agent_afw_duration,
            agent_interact_duration, agent_holds, agent_longest_hold,
            queue_id, queue_name, in_queue_time, out_queue_time, queue_duration,
            customer_voice, customer_hold_duration, contact_duration,
            sys_phone, conn_to_sys, customer_phone
        ) VALUES %s
    """

    procedure_sql = "CALL connect.insert_new_f_calls();"

    try:
        conn = psycopg2.connect(**REDSHIFT_CONFIG)
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows)
                print(f"Inserted {len(rows)} rows into connect.f_calls_staging.")
                
                # Execute stored procedure
                cur.execute(procedure_sql)
                print("Stored procedure 'connect.insert_new_f_calls()' executed.")
    except Exception as e:
        print(f"Redshift error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# Lambda entry point
def lambda_handler(event, context):
    start_time = time_module.time()

    calls = fetch_completed_calls(START_DATE_UTC, END_DATE_UTC)
    insert_into_redshift(calls)

    end_time = time_module.time()
    minutes = int((end_time - start_time) // 60)
    seconds = int((end_time - start_time) % 60)
    print(f"Execution time: {minutes} min {seconds} sec")

    return {
        "statusCode": 200,
        "body": f"Loaded {len(calls)} calls into Redshift staging."
    }