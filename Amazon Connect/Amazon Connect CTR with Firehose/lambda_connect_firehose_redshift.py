import json
import pytz
import boto3
import base64
from datetime import datetime

dynamo = boto3.client('dynamodb', region_name='us-east-1')

def is_duplicate(contact_id):
    """
    Try to insert contact_id into DynamoDB.
    If contact_id exists, it throws an exceptions and returns True (duplicated).
    """
    eastern_tz = pytz.timezone("America/New_York")
    processed_at = datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(eastern_tz).isoformat()

    try:
        dynamo.put_item(
            TableName='ProcessedCTR',  
            Item={
                'ContactId': {'S': contact_id},
                'ProcessedAt': {'S': processed_at}
            },
            ConditionExpression='attribute_not_exists(ContactId)'
        )
        return False  
    except dynamo.exceptions.ConditionalCheckFailedException:
        return True

def parse_datetime(timestamp):
    eastern_tz = pytz.timezone("America/New_York")
    if timestamp:
        try:
            dt_utc = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
            dt_utc = dt_utc.replace(tzinfo=pytz.utc)
            return dt_utc.astimezone(eastern_tz).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return None

def lambda_handler(event, context):
    output = []

    for record in event['records']:

        b64_data = record.get('data', '')
        
        if not b64_data.strip():
            # If it's empty it's marked as "Dropped" and it continues
            output.append({
                'recordId': record['recordId'],
                'result': 'Dropped',
                'data': record['data']
            })
            continue

        try:
            data_str = base64.b64decode(b64_data).decode('utf-8')
            payload = json.loads(data_str)

        except json.JSONDecodeError as e:
            print(f"JSON decode error in record {record['recordId']}: {e}")
            output.append({
                'recordId': record['recordId'],
                'result': 'Dropped',
                'data': record['data']
            })
            continue

        # Extract ContactId
        contact_id = payload.get('ContactId', '')
        if not contact_id:
            # In not ContacId, then Dropped
            output.append({
                'recordId': record['recordId'],
                'result': 'Dropped',
                'data': record['data']
            })
            continue

        # Verify if this ContactId was processed in DynamoDB
        if is_duplicate(contact_id):
            print(f"Duplicate found for ContactId: {contact_id}")
            output.append({
                'recordId': record['recordId'],
                'result': 'Dropped',
                'data': record['data']
            })
            continue
        
        agent_data = payload.get('Agent', {}) or {}
        queue_data = payload.get('Queue', {}) or {}

        # Transform the CTR into a flat structure
        transformed = {
            'init_contact_id': payload.get('InitialContactId', ''), 
            'prev_contact_id': payload.get('PreviousContactId', ''),
            'contact_id': payload.get('ContactId', ''),
            'next_contact_id': payload.get('NextContactId', ''),
            'channel': payload.get('Channel', ''),
            'init_method': payload.get('InitiationMethod', ''),
            'init_time': parse_datetime(payload.get('InitiationTimestamp', '')),
            'disconn_time': parse_datetime(payload.get('DisconnectTimestamp', '')),
            'disconn_reason': payload.get('DisconnectReason', ''),
            'last_update_time': parse_datetime(payload.get('LastUpdateTimestamp', '')),
            'agent_conn': parse_datetime(agent_data.get('ConnectedToAgentTimestamp', '')),
            'agent_id': agent_data.get('ARN', '').split("/agent/")[-1] if agent_data.get('ARN') else None,
            'agent_username': agent_data.get('Username', ''),
            'agent_conn_att': payload.get('AgentConnectionAttempts', 0),
            'agent_afw_start': parse_datetime(agent_data.get('AfterContactWorkStartTimestamp', '')),
            'agent_afw_end': parse_datetime(agent_data.get('AfterContactWorkEndTimestamp', '')),
            'agent_afw_duration': agent_data.get('AfterContactWorkDuration', 0),
            'agent_interact_duration': agent_data.get('AgentInteractionDuration', 0),
            'agent_holds': agent_data.get('NumberOfHolds', 0),
            'agent_longest_hold': agent_data.get('LongestHoldDuration', 0),
            'queue_id': queue_data.get('ARN', '').split("/queue/")[-1] if queue_data.get('ARN') else None,
            'queue_name': queue_data.get('Name', ''),
            'in_queue_time': parse_datetime(queue_data.get('EnqueueTimestamp', '')),
            'out_queue_time': parse_datetime(queue_data.get('DequeueTimestamp', '')),
            'queue_duration': queue_data.get('Duration', 0),
            'customer_phone': payload.get('CustomerEndpoint', {}).get('Address', ''),
            'customer_voice': payload.get('CustomerEndpoint', {}).get('Voice', ''),
            'customer_hold_duration': agent_data.get('CustomerHoldDuration', 0),
            'sys_phone': payload.get('SystemEndpoint', {}).get('Address', ''),
            'conn_to_sys': parse_datetime(payload.get('ConnectedToSystemTimestamp', '')),
        }

        # Convert to JSON and encode for Firehose
        transformed_json = json.dumps(transformed)
        encoded_data = base64.b64encode(transformed_json.encode('utf-8')).decode('utf-8')
        output.append({
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': encoded_data
        })
    
    return {'records': output}