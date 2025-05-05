import boto3
import pandas as pd
from datetime import datetime
import pytz
import io
import psycopg2

# Configuration
REDSHIFT_CONFIG = json.loads(os.environ["REDSHIFT_CONFIG"])
IAM_ROLE_ARN = os.getenv("IAM_ROLE_ARN")

S3_TARGET_BUCKET = os.getenv("S3_TARGET_BUCKET")


s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ProcessedMatterFolders')

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
                COPY litify.matter_staging
                FROM '{S3_TEMP_PATH}'
                IAM_ROLE '{IAM_ROLE_ARN}'
                FORMAT AS JSON 'auto'
                TIMEFORMAT 'auto'
                BLANKSASNULL
                EMPTYASNULL;
            """)
            print("COPY completado")
            cur.execute("CALL litify.update_litify_matter();")
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

def transform_data(df):


    df.columns = df.columns.str.lower()
    
    datetime_fields = [

        "createddate",
        "lastmodifieddate",
        "systemmodstamp",
        "lastactivitydate",
        "litify_pm__open_date__c",
        "litify_pm__last_called_at__c",
        "litify_pm__last_emailed_at__c",
        "litify_pm__closed_date__c",
        "litify_pm__filed_date__c",
        "rfe_deadline__c",
        "emergency_deadline_date__c",
        "approved_denied_date__c",
        "psych_eval_date__c",
        "submitted_to_uscis__c",
        "reviewed_with_cl__c",
        "ff_paid_on__c",
        "receipt_notices_received__c",
        "fingerprint_appointment__c",
        "psych_eval_completed__c",
        "psych_eval_submitted_to_uscis__c",
        "rfe_received__c",
        "rfe_submission__c",
        "received_prima_facie__c",
        "received_work_permit__c",
        "checkboxf__c",
        "foia_request__c",
        "fbi_submission__c",
        "appeal_deadline__c",
        "approval_received__c",
        "denial_received__c",
        "client_notified__c",
        "uscis_receipt_cl_notified__c",
        "fingerprint_cl_notified__c",
        "rfe_received_cl_notified__c",
        "work_permit_cl_notified__c",
        "approval_received_cl_notified__c",
        "denial_received_cl_notified__c",
        "received_work_permit2__c",
        "work_permit_cl_notified2__c",
        "docs_collected__c",
        "accurint_report_completed__c",
        "sign_up_day__c",
        "cl_interview__c",
        "delivered_on__c",
        "intreview_completed__c",
        "forms_completed__c",
        "rejection_received__c",
        "refiling_date__c",
        "prima_facie_cl_notified__c",
        "early_aos_requested__c",
        "early_aos_requested_cl_notified__c",
        "early_aos_approved_cl_notified__c",
        "aos_approval_received__c",
        "referred_out_for_pe__c",
        "latest_case_update__c",
        "rfe_delivery__c",
        "qc_completed__c",
        "follow_up_date__c",
        "date_ff_paid_on__c",
        "noid_received__c",
        "noid_responded__c",
        "pre_rfe_date__c",
        "latest_docs_fu__c",
        "i_485_interview_360__c",
        "i_485_interview_aos__c",
        "asc_appointment_date__c",
        "welcome_email_sent__c",
        "last_auto_txt_communication__c",
        "pif2__c",
        "bonafide_received__c",
        "status_changed_date_time__c",
        "concern_raised__c",
        "concern_resolved__c",
        "dec_forms_sent_for_review__c"
    ]

    boolean_fields = [

        "isdeleted",
        "litify_pm__billable_matter__c",
        "litify_pm__ignore_default_plan__c",
        "litify_pm__limitations_date_satisfied__c",
        "litify_pm__matter_has_budget__c",
        "litify_pm__matter_team_modified__c",
        "litify_pm__manual_statute_of_limitations__c",
        "run_triggers__c",
        "litify_ext__isteammember__c",
        "litify_ext__private__c",
        "isdeceased__c",
        "serious_injury__c",
        "isminor__c",
        "conflict_check__c",
        "payment_overdue__c",
        "payments_criteria_2months__c",
        "is_synced__c",
        "urgent__c",
        "not_financial_user__c",
        "filling_fees_paid__c",
        "attorney_or_paralegal__c",
        "is_cl_specialist__c",
        "automatic_form_errors__c",
        "checkboxdate__c",
        "priority__c",
        "case_submitted__c",
        "pif__c",
        "foia_eoir__c",
        "filled_fee_is_filled_automation__c",
        "case_delivered__c",
        "attorney_approval__c",
        "consent_for_mts__c",
        "official_records__c",
        "early_aos_request__c",
        "mtt__c",
        "pro_bono__c",
        "marked_for_rfe_tagging__c",
        "ff_confirmed__c",
        "submission_qc__c",
        "removal__c",
        "original_docs_at_the_office__c",
        "i_765_filled__c",
        "cl_detained__c",
        "supervisor_call__c",
        "supervisor_call_resolved__c",
        "flagged_for_issues__c",
        "template_needed__c",
        "cases_sold_with__c",
        "money_back_guarantee__c",
        "archived__c",
        "unresponsive_client__c",
        "sensitive_case__c",
        "criminal_offense__c",
        "monitor_delivery__c",
        "post_dec_forms_review_edits__c",
        "attorney_call_needed__c",
        "case_monitoring__c",
        "open_warrant__c",
        "i_131__c",
        "claim_issue_found__c",
        "signature__c",
        "full_translation__c",
        "form_update__c"
        ]

    int_fields = [

        "live_saved__c",
        "lives_saved__c",
        "no_of_days__c",
        "turnaround_time__c",
        "count_role_records__c",
        "case_count__c",
        "live_associated__c",
        "litify_pm__matter__c",
        "litify_pm__total_calls__c",
        "successful_calls__c",
        "litify_pm__total_emails__c"
        ]


    float_fields = [

        "litify_pm__total_damages__c",
        "scheduled_amount__c",
        "litify_pm__total_hours__c",
        "litify_pm__total_amount_billable__c",
        "litify_pm__total_amount_due__c",
        "litify_pm__total_matter_value__c",
        "litify_pm__total_matter_cost__c",
        "litify_pm__total_amount_paid__c",
        "litify_pm__total_amount_billed__c",
        "litify_pm__total_amount_expensed_due__c",
        "litify_pm__total_amount_expensed__c",
        "litify_pm__total_amount_retained__c",
        "litify_pm__total_amount_unbilled_expenses__c",
        "litify_pm__total_amount_time_entries__c",
        "litify_pm__total_amount_time_entries_billed__c",
        "litify_pm__total_amount_time_entries_due__c",
        "litify_pm__total_amount_time_entries_unpaid__c",
        "litify_pm__lit_lien_total_currency__c",
        "litify_pm__lit_total_client_payout__c",
        "litify_pm__lit_damage_total__c",
        "litify_pm__lit_expense_total__c",
        "litify_pm__lit_lien_total__c",
        "total_billable_expenses__c",
        "total_unbilled_expenses__c",
        "total_billable_te__c",
        "total_unbilled_time_entries__c",
        "total_invoiced_amount__c",
        "total_payments_received__c",
        "total_expenses__c",
        "total_billed_expenses__c",
        "total_time_entries__c",
        "total_billed_time_entries__c",
        "total_payments_due__c",
        "total_uninvoiced_amount__c",
        "payment__c",
        "total_filing_fee__c",
        "total_overdue_amount__c",
        "urgentoverdue__c"
        ]

    string_fields = [col for col in df.columns if col not in datetime_fields + boolean_fields + int_fields + float_fields]

    
    for field in datetime_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], errors='coerce')

    for field in boolean_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: 1 if x in ['t', 'T', 'True', 'true', 1] else 0)
            #df[field] = df[field].map({'t': 1, 'f': 0}).fillna(0).astype(int)

    for field in int_fields:
        if field in df.columns:
            df[field] = df[field].fillna(0).astype(int)

    for field in float_fields:
        if field in df.columns:
            df[field] = df[field].fillna(0).astype(float)

    for field in string_fields:
        if field in df.columns:
            df[field] = df[field].fillna('').astype(str)
    
    return df

def process_matter_csvs(bucket, differential_folder):
    ny_tz = pytz.timezone('America/New_York')
    timestamp = datetime.now(ny_tz).strftime('%Y%m%d%H%M%S')
    s3_temp_key = f'staging/matter_staging/matter_staging_{timestamp}.json'
    matter_prefix = differential_folder + 'litify_pm__Matter__c/'
    result = s3.list_objects_v2(Bucket=bucket, Prefix=matter_prefix)

    if 'Contents' not in result:
        print(f"No se encontró carpeta 'litify_pm__Matter__c/' en {differential_folder}")
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
                df = transform_data(df)
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
        csv_processed = process_matter_csvs(bucket_name, full_diff_folder)

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