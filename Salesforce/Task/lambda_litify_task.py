import boto3
import pandas as pd
import io
import json 
from datetime import datetime
import pytz
import psycopg2
import os

# Configuration
REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN")

S3_TARGET_BUCKET = os.getenv("S3_TARGET_BUCKET")

# AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ProcessedTaskFolders')

bucket_name = 'sfdatabackup-gfproduction'
prefix_base = 'backup/'

# Helper function to get the local time in ISO format
def get_local_time_iso():
    ny_tz = pytz.timezone('America/New_York')
    return datetime.now(ny_tz).isoformat()

# Function to upload dataframe to S3
def upload_df_to_s3(df, s3_bucket, s3_key):
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', lines=True, date_format='iso')
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=json_buffer.getvalue())

# Function to copy data to Redshift and update
def copy_to_redshift_and_update(s3_temp_key):
    S3_TEMP_PATH = f's3://{S3_TARGET_BUCKET}/{s3_temp_key}'
    with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                COPY litify.task_staging
                FROM '{S3_TEMP_PATH}'
                IAM_ROLE '{IAM_ROLE_ARN}'
                FORMAT AS JSON 'auto'
                TIMEFORMAT 'auto'
                BLANKSASNULL
                EMPTYASNULL;
            """)
            print("COPY completado")
            cur.execute("CALL litify.update_litify_task();")
            print("Procedure ejecutada")
        conn.commit()

# Function to extract folder key from folder name
def extract_folder_key(folder_name: str) -> str:
    return folder_name.split('/')[-2].split('_Differential')[0] + "_"

# Function to transform data
def transform_data(df):
    columns_to_keep = [

    'WhatId', 
    'Subject', 
    'ActivityDate', 
    'Status', 
    'Priority', 
    'IsHighPriority', 
    'OwnerId', 
    'Description',
    'IsClosed', 
    'CreatedDate', 
    'CreatedById', 
    'LastModifiedDate', 
    'LastModifiedById', 
    'SystemModstamp',
    'ReminderDateTime', 
    'IsReminderSet', 
    'IsRecurrence', 
    'In_Progress_Date__c', 
    'TaskSubtype', 
    'CompletedDateTime',
    'litify_ext__Status__c', 
    'litify_pm__Default_Matter_Task__c', 
    'litify_pm__Matter_Stage_Activity__c',
    'litify_pm__AssociatedObjectName__c', 
    'litify_pm__Completed_Date__c', 
    'litify_pm__AssigneeName__c',
    'litify_pm__MatterStage__c', 
    'litify_pm__UserRoleRelatedJunction__c', 
    'Show_On_Calendar__c',
    'Completed_Date__c', 
    'Id', 
    'Completed_By__c']

    df = df[columns_to_keep]

    datetime_fields = ['ActivityDate', 'Completed_Date__c', 'In_Progress_Date__c','CreatedDate', 'LastModifiedDate', 'CompletedDateTime',
                       'litify_pm__Completed_Date__c', 'SystemModstamp', 'ReminderDateTime']
    for field in datetime_fields:
        df[field] = pd.to_datetime(df[field], errors='coerce')

    boolean_fields = ['IsHighPriority', 'IsClosed', 'IsReminderSet', 'IsRecurrence', 'Show_On_Calendar__c']
    for field in boolean_fields:
        df[field] = df[field].fillna(0).astype(bool).astype(int)

    string_fields = ["WhatId", "Subject", "Status", "Priority", "OwnerId", "Description", "CreatedById", "LastModifiedById",
                     "TaskSubtype", "litify_pm__Default_Matter_Task__c", "litify_pm__Matter_Stage_Activity__c",
                     "litify_pm__AssociatedObjectName__c", "litify_pm__AssigneeName__c", "litify_pm__MatterStage__c",
                     "litify_pm__UserRoleRelatedJunction__c", "litify_ext__Status__c", "Id", 'Completed_By__c']
    for field in string_fields:
        df[field] = df[field].fillna('').astype(str)

    df.columns = df.columns.str.lower()
    return df

# Function to get processed keys from DynamoDB
def get_processed_keys():
    response = table.scan()
    return {item['folder_key'] for item in response.get('Items', [])}

# Function to mark folder as processed in DynamoDB
def mark_key_as_processed(folder_key):
    table.put_item(Item={
        'folder_key': folder_key,
        'processed_at': get_local_time_iso()
    })

# Function to list differential folders in S3
def list_differential_folders(bucket, base_prefix):
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter='/')
    folders = []
    for page in result:
        folders += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return folders

# Function to process Task CSVs
def process_task_csvs(bucket, differential_folder):
    ny_tz = pytz.timezone('America/New_York')
    timestamp = datetime.now(ny_tz).strftime('%Y%m%d%H%M%S')
    s3_temp_key = f'staging/task_staging/task_staging_{timestamp}.json'
    task_prefix = differential_folder + 'Task/'
    result = s3.list_objects_v2(Bucket=bucket, Prefix=task_prefix)

    # If no files found, return False to indicate this folder is empty
    if 'Contents' not in result:
        print(f"No se encontró carpeta 'Task/' en {differential_folder}")
        return False

    csv_found = False  # Flag to track if any CSV is found and processed

    # Process the files in the 'Task' folder
    for obj in result['Contents']:
        key = obj['Key']
        if key.endswith('.csv'):
            csv_found = True  # Mark as found once we detect a .csv file
            print(f"Procesando CSV: {key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(response['Body'])

            try:
                df = transform_data(df)
                upload_df_to_s3(df, S3_TARGET_BUCKET, s3_temp_key)
                copy_to_redshift_and_update(s3_temp_key)
            except Exception as e:
                print(f"Error al transformar {key}: {e}")

    return csv_found  # Return whether CSVs were found and processed

# Lambda Handler
def lambda_handler(event, context):
    # Get processed keys and list differential folders
    processed_keys = get_processed_keys()
    all_diff_folders = list_differential_folders(bucket_name, prefix_base)

    # Iterate over all the differential folders
    for i, full_diff_folder in enumerate(all_diff_folders):
        if not full_diff_folder.endswith('_Differential/'):
            continue

        folder_key = extract_folder_key(full_diff_folder)

        # Skip processing if the folder has already been processed
        if folder_key in processed_keys:
            print(f"Ya procesado: {folder_key}")
            continue

        print(f"Procesando folder: {folder_key}")

        # Call process_task_csvs and check if CSV files were found and processed
        csv_processed = process_task_csvs(bucket_name, full_diff_folder)

        # If the current folder has no CSV files
        if not csv_processed:
            # If this is not the last folder (i.e., there is a next folder to process), mark as processed
            if i < len(all_diff_folders) - 1:  # Check if there is a next folder
                mark_key_as_processed(folder_key)  # Only mark as processed if there is a next folder
                print(f"Carpeta vacía marcada como procesada: {folder_key}")
            else:
                print(f"No se encontraron archivos CSV en {folder_key} y es la última carpeta, no se marcará como procesada.")
        else:
            # If CSV files were found and processed, mark as processed
            mark_key_as_processed(folder_key)
            print(f"Completado: {folder_key}")

    return {'status': 'ok', 'message': 'Todos los folders nuevos fueron procesados'}