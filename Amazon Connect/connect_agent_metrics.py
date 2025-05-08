import psycopg2
from psycopg2.extras import execute_values
import boto3
import pytz
import json 
import os 
from datetime import datetime, timedelta, time

# Constants
REDSHIFT_CONFIG = json.loads(os.environ['REDSHIFT_CONFIG'])
INSTANCE_ID = os.environ['INSTANCE_ID']
TIMEZONE = os.environ['TIMEZONE']
ny_tz = pytz.timezone(TIMEZONE)
REGION = os.environ['REGION']



def get_all_agent_ids(client,instance_id):
    agent_ids = []
    next_token = None

    print("Fetching agent IDs from Amazon Connect...")

    while True:
        if next_token:
            response = client.list_users(InstanceId=INSTANCE_ID, NextToken=next_token)
        else:
            response = client.list_users(InstanceId=INSTANCE_ID)

        for user_summary in response["UserSummaryList"]:
            agent_ids.append(user_summary["Id"])

        next_token = response.get("NextToken")
        if not next_token:
            break

    print(f" Total agent IDs fetched: {len(agent_ids)}")
    return agent_ids


def chunk_list(lst, chunk_size=100):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def get_time_range():
    today_ny = datetime.now(ny_tz).date()
    yesterday_ny = today_ny - timedelta(days=1)
    start = ny_tz.localize(datetime.combine(yesterday_ny, datetime.min.time())).astimezone(pytz.utc)
    end = ny_tz.localize(datetime.combine(yesterday_ny, datetime.max.time())).astimezone(pytz.utc)
    return start, end

def get_all_metrics_paginated(client, **params):
    all_results = []
    next_token = None
    while True:
        if next_token:
            params['NextToken'] = next_token
        else:
            params.pop('NextToken', None)
        response = client.get_metric_data_v2(**params)
        all_results.extend(response.get('MetricResults', []))
        next_token = response.get('NextToken')
        if not next_token:
            break
    return all_results

def parse_metrics_to_json(metric_results):
    expected_metrics = [
        'AGENT_ANSWER_RATE', 'AGENT_NON_RESPONSE', 'AGENT_OCCUPANCY',
        'AVG_DIALS_PER_MINUTE', 'SUM_CONNECTING_TIME_AGENT', 'SUM_RETRY_CALLBACK_ATTEMPTS',
        'PERCENT_TALK_TIME_CUSTOMER', 'AVG_TALK_TIME_CUSTOMER', 'PERCENT_TALK_TIME_AGENT',
        'AVG_TALK_TIME_AGENT', 'PERCENT_TALK_TIME', 'AVG_TALK_TIME',
        'CONTACTS_QUEUED', 'CONTACTS_QUEUED_BY_ENQUEUE', 'MAX_QUEUED_TIME',
        'CONTACTS_TRANSFERRED_OUT_FROM_QUEUE', 'AVG_QUEUE_ANSWER_TIME', 'CONTACTS_CREATED',
        'SUM_CONTACTS_DISCONNECTED', 'AVG_ACTIVE_TIME', 'ABANDONMENT_RATE', 'AVG_NON_TALK_TIME',
        'AVG_INTERRUPTION_TIME_AGENT', 'DELIVERY_ATTEMPTS', 'CONTACTS_TRANSFERRED_OUT',
        'CONTACTS_TRANSFERRED_OUT_INTERNAL', 'CONTACTS_TRANSFERRED_OUT_EXTERNAL',
        'CONTACTS_PUT_ON_HOLD', 'AVG_HOLDS', 'SUM_HOLD_TIME', 'CONTACTS_HOLD_ABANDONS',
        'CONTACTS_ON_HOLD_AGENT_DISCONNECT', 'CONTACTS_ON_HOLD_CUSTOMER_DISCONNECT',
        'CONTACTS_HANDLED', 'AVG_HANDLE_TIME', 'SUM_HANDLE_TIME', 'AVG_INTERACTION_TIME',
        'SUM_INTERACTION_TIME', 'AVG_CONTACT_DURATION', 'SUM_INTERACTION_AND_HOLD_TIME',
        'AVG_AFTER_CONTACT_WORK_TIME', 'SUM_AFTER_CONTACT_WORK_TIME', 'SUM_ONLINE_TIME_AGENT',
        'SUM_NON_PRODUCTIVE_TIME_AGENT', 'SUM_IDLE_TIME_AGENT', 'SUM_ERROR_STATUS_TIME_AGENT',
        'SUM_CONTACT_TIME_AGENT', 'AGENT_NON_RESPONSE_WITHOUT_CUSTOMER_ABANDONS',
        'AGENT_NON_ADHERENT_TIME', 'AGENT_ADHERENT_TIME', 'AGENT_SCHEDULED_TIME',
        'AGENT_SCHEDULE_ADHERENCE'
    ]

    rows = []
    for entry in metric_results:
        row = {
            'agent_id': entry['Dimensions']['AGENT'],
            'start_time': entry['MetricInterval']['StartTime'].astimezone(ny_tz).replace(tzinfo=None),
            'end_time': entry['MetricInterval']['EndTime'].astimezone(ny_tz).replace(tzinfo=None),
        }
        for metric in expected_metrics:
            row[metric.lower()] = None

        for metric in entry['Collections']:
            name = metric['Metric']['Name'].lower()
            value = metric.get('Value')
            row[name] = round(value, 2) if value is not None else None

        rows.append(row)
    return rows

def insert_json_rows_to_redshift(json_rows, redshift_config):
    if not json_rows:
        print("No rows to insert.")
        return
    column_names = list(json_rows[0].keys())
    values = [tuple(row.get(col) for col in column_names) for row in json_rows]

    insert_query = f"""
        INSERT INTO connect.f_agent_metrics (
            {', '.join(column_names)}
        ) VALUES %s
    """

    try:
        conn = psycopg2.connect(**redshift_config)
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, values)
                print(f"Inserted {len(values)} rows into Redshift.")
    except Exception as e:
        print(f"Redshift insert failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def lambda_handler(event, context):
    start_time, end_time = get_time_range()
    client = boto3.client("connect", region_name=REGION)
    agent_ids = get_all_agent_ids(client,INSTANCE_ID)
    all_metric_results = []

    metric_names = [
        'SUM_ONLINE_TIME_AGENT', 'SUM_NON_PRODUCTIVE_TIME_AGENT', 'AGENT_ADHERENT_TIME',
        'AGENT_NON_ADHERENT_TIME', 'AGENT_ANSWER_RATE', 'AGENT_NON_RESPONSE',
        'AGENT_NON_RESPONSE_WITHOUT_CUSTOMER_ABANDONS', 'AGENT_OCCUPANCY',
        'AGENT_SCHEDULED_TIME', 'AGENT_SCHEDULE_ADHERENCE', 'AVG_DIALS_PER_MINUTE',
        'SUM_IDLE_TIME_AGENT', 'SUM_ERROR_STATUS_TIME_AGENT', 'SUM_CONTACT_TIME_AGENT',
        'SUM_CONNECTING_TIME_AGENT', 'SUM_RETRY_CALLBACK_ATTEMPTS',
        'PERCENT_TALK_TIME_CUSTOMER', 'AVG_TALK_TIME_CUSTOMER', 'PERCENT_TALK_TIME_AGENT',
        'AVG_TALK_TIME_AGENT', 'PERCENT_TALK_TIME', 'AVG_TALK_TIME', 'CONTACTS_QUEUED',
        'CONTACTS_QUEUED_BY_ENQUEUE', 'MAX_QUEUED_TIME', 'CONTACTS_TRANSFERRED_OUT_FROM_QUEUE',
        'AVG_QUEUE_ANSWER_TIME', 'CONTACTS_CREATED', 'SUM_CONTACTS_DISCONNECTED',
        'AVG_ACTIVE_TIME', 'ABANDONMENT_RATE', 'AVG_NON_TALK_TIME',
        'AVG_INTERRUPTION_TIME_AGENT', 'DELIVERY_ATTEMPTS', 'CONTACTS_TRANSFERRED_OUT',
        'CONTACTS_TRANSFERRED_OUT_INTERNAL', 'CONTACTS_TRANSFERRED_OUT_EXTERNAL',
        'CONTACTS_PUT_ON_HOLD', 'AVG_HOLDS', 'SUM_HOLD_TIME', 'CONTACTS_HOLD_ABANDONS',
        'CONTACTS_ON_HOLD_AGENT_DISCONNECT', 'CONTACTS_ON_HOLD_CUSTOMER_DISCONNECT',
        'CONTACTS_HANDLED', 'AVG_HANDLE_TIME', 'SUM_HANDLE_TIME', 'AVG_INTERACTION_TIME',
        'SUM_INTERACTION_TIME', 'AVG_CONTACT_DURATION', 'SUM_INTERACTION_AND_HOLD_TIME',
        'AVG_AFTER_CONTACT_WORK_TIME', 'SUM_AFTER_CONTACT_WORK_TIME'
    ]

    for agent_chunk in chunk_list(agent_ids, 100):
        params = {
            'ResourceArn': f"arn:aws:connect:{REGION}:555988031712:instance/{INSTANCE_ID}",
            'StartTime': start_time,
            'EndTime': end_time,
            'Interval': {'TimeZone': TIMEZONE, 'IntervalPeriod': 'HOUR'},
            'Filters': [{'FilterKey': 'AGENT', 'FilterValues': agent_chunk}],
            'Groupings': ['AGENT'],
            'Metrics': [{'Name': name} for name in metric_names],
            'MaxResults': 100
        }

        results = get_all_metrics_paginated(client, **params)
        all_metric_results.extend(results)

    json_rows = parse_metrics_to_json(all_metric_results)
    insert_json_rows_to_redshift(json_rows, REDSHIFT_CONFIG)
