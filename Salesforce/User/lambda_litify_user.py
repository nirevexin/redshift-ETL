import boto3
import pandas as pd
import io
from datetime import datetime
import pytz
import psycopg2
import os 
import json 

# Configuration
REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN")

S3_TARGET_BUCKET = os.getenv("S3_TARGET_BUCKET")

# AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ProcessedUserFolders')

bucket_name = 'sfdatabackup-gfproduction'
prefix_base = 'backup/'

def get_local_time_iso():
    ny_tz = pytz.timezone('America/New_York')
    return datetime.now(ny_tz).isoformat()

def upload_df_to_s3(df, s3_bucket, s3_key):
    json_buffer = io.StringIO()
    df.to_json(json_buffer, orient='records', lines=True, date_format='iso')
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=json_buffer.getvalue())

def copy_to_redshift_and_update(s3_temp_key):
    S3_TEMP_PATH = f's3://{S3_TARGET_BUCKET}/{s3_temp_key}'
    with psycopg2.connect(**REDSHIFT_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                COPY litify.dim_users_staging
                FROM '{S3_TEMP_PATH}'
                IAM_ROLE '{IAM_ROLE_ARN}'
                FORMAT AS JSON 'auto'
                TIMEFORMAT 'auto'
                BLANKSASNULL
                EMPTYASNULL;
            """)
            print("COPY completado")
            cur.execute("CALL litify.update_litify_user();")
            print("Procedure ejecutada")
        conn.commit()

def extract_folder_key(folder_name: str) -> str:
    return folder_name.split('/')[-2].split('_Differential')[0] + "_"

def get_processed_keys():
    response = table.scan()
    return {item['folder_key'] for item in response.get('Items', [])}

def mark_key_as_processed(folder_key):
    table.put_item(Item={
        'folder_key': folder_key,
        'processed_at': get_local_time_iso()
    })

def list_differential_folders(bucket, base_prefix):
    paginator = s3.get_paginator('list_objects_v2')
    result = paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter='/')
    folders = []
    for page in result:
        folders += [cp['Prefix'] for cp in page.get('CommonPrefixes', [])]
    return folders

def transform_user_data(df):
    df.columns = df.columns.str.lower()
    col_list = ['id',
                'username',
                'alias',
                'communitynickname',
                'firstname',
                'lastname',
                'title',
                'cm_job_title__c',
                'cm_job_title_multi__c',
                'department__c',
                'isactive',
                'startday',
                'endday',
                'companyname',
                'timezonesidkey',
                'localesidkey',
                'usertype',
                'passwordexpirationdate',
                'systemmodstamp',
                'lastpasswordchangedate',
                'createddate',
                'createdbyid',
                'lastmodifieddate',
                'lastmodifiedbyid',
                'lastlogindate',
                'receivesinfoemails',
                'receivesadmininfoemails',
                'numberoffailedlogins',
                'dfsle__username__c',
                'dfsle__status__c',
                'dfsle__provisioned__c',
                'dfsle__canmanageaccount__c',
                'aboutme',
                'federationidentifier',
                'attorneys_per_page__c',
                'lastreferenceddate',
                'lastvieweddate',
                'defaultgroupnotificationfrequency',
                'digestfrequency',
                'profileid']
    
    bool_fields = ['isactive', 
                'receivesinfoemails', 
                'receivesadmininfoemails', 
                'dfsle__canmanageaccount__c']

    date_fields = ['lastvieweddate', 
                'lastreferenceddate', 
                'lastlogindate', 
                'lastmodifieddate', 
                'createddate', 
                'lastpasswordchangedate', 
                'systemmodstamp', 
                'passwordexpirationdate', 
                'dfsle__provisioned__c']

    float_fields = ['startday',	
                    'endday', 
                    'numberoffailedlogins']

    string_fields = [col for col in df.columns if col not in date_fields + bool_fields + float_fields]

    df = df[col_list]

    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')

    for field in bool_fields:
        if field in df.columns:
            df[field] = df[field].fillna(0).astype(bool).astype(int)

    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].fillna('').astype(str)

    return df

def process_user_csvs(bucket, differential_folder):
    timestamp = datetime.now(pytz.timezone('America/New_York')).strftime('%Y%m%d%H%M%S')
    s3_temp_key = f'staging/user_staging/user_staging_{timestamp}.json'
    user_prefix = differential_folder + 'User/'
    result = s3.list_objects_v2(Bucket=bucket, Prefix=user_prefix)

    if 'Contents' not in result:
        print(f"No se encontró carpeta 'User/' en {differential_folder}")
        return False

    csv_found = False
    for obj in result['Contents']:
        key = obj['Key']
        if key.endswith('.csv'):
            csv_found = True
            print(f"Procesando CSV: {key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(response['Body'])
            try:
                df = transform_user_data(df)
                upload_df_to_s3(df, S3_TARGET_BUCKET, s3_temp_key)
                copy_to_redshift_and_update(s3_temp_key)
            except Exception as e:
                print(f"Error al transformar {key}: {e}")
    return csv_found

def lambda_handler(event, context):
    processed_keys = get_processed_keys()
    all_diff_folders = list_differential_folders(bucket_name, prefix_base)

    for i, full_diff_folder in enumerate(all_diff_folders):
        if not full_diff_folder.endswith('_Differential/'):
            continue

        folder_key = extract_folder_key(full_diff_folder)

        if folder_key in processed_keys:
            print(f"Ya procesado: {folder_key}")
            continue

        print(f"Procesando folder: {folder_key}")
        csv_processed = process_user_csvs(bucket_name, full_diff_folder)

        if not csv_processed:
            if i < len(all_diff_folders) - 1:
                mark_key_as_processed(folder_key)
                print(f"Carpeta vacía marcada como procesada: {folder_key}")
            else:
                print(f"Última carpeta sin CSVs, no se marcará como procesada: {folder_key}")
        else:
            mark_key_as_processed(folder_key)
            print(f"Completado: {folder_key}")

    return {'status': 'ok', 'message': 'Todos los folders nuevos fueron procesados'}