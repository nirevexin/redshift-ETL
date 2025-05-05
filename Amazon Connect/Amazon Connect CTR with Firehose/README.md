# 🔁 AWS Lambda + Redshift Serverless ETL Pipeline 

This repository contains the source code and configuration for an ETL pipeline designed to ingest and process Amazon Connect **Contact Trace Records (CTRs)** using AWS **Kinesis Firehose**, an **AWS Lambda transformer**, and a **Redshift Serverless destination**.

The processed data is stored in the Redshift table `connect.f_calls`.

---

## 🚀 Architecture Overview

### 🔄 Kinesis Firehose Stream

| Setting                       | Value                                                        |
|------------------------------|--------------------------------------------------------------|
| Source                       | Direct PUT                                                   |
| Destination                  | Amazon Redshift (Serverless Workgroup: `***`)             |
| Data transformation          | Enabled                                                      |
| Transformation Lambda        | Python 3.9 (15 min timeout)                                  |
| Buffer size                  | 2 MiB                                                        |
| Buffer interval              | 900 seconds                                                  |
| Retry duration               | 300 seconds                                                  |
| Intermediate S3 bucket       | `***-connect-firehose-s3-redshift`                           |
| Redshift Database            | `core_db`                                                    |
| Redshift Table               | `connect.f_calls`                                            |
| IAM Role                     | `KinesisFirehoseServiceRole-connect-redsh-***-****-*-********` |
| Logging                      | Enabled (Amazon CloudWatch)                                  |
| Compression / Encryption     | Disabled                                                     |

---

## 🧠 Why This Approach?

- **Real-time ingestion** using Kinesis Firehose
- **Transformation-on-the-fly** via AWS Lambda (flattening, enrichment)
- **Idempotent processing** with DynamoDB to avoid duplicates (`ProcessedCTR` table)
- **Low-maintenance** and **serverless architecture**

---

## 🐍 Lambda Function (`lambda_handler.py`)

This Python 3.9 Lambda function:
- Decodes base64 CTR records from Firehose
- Transforms nested JSON into a flat structure
- Filters duplicates using DynamoDB (`ProcessedCTR`)
- Converts timestamps to `America/New_York` timezone
- Encodes the result back to base64 for Firehose ingestion

### ➕ Key Features

- Uses `DynamoDB` conditional writes to detect duplicates via `ContactId`
- Ensures time-based fields are normalized and formatted
- Gracefully drops empty or malformed records

---

## 🗃️ Redshift COPY Command

The Redshift destination expects JSON-formatted records. The table `connect.f_calls` is populated using the following COPY configuration:

```sql
COPY connect.f_calls (
    init_contact_id,
    prev_contact_id,
    contact_id,
    next_contact_id,
    channel,
    init_method,
    init_time,
    disconn_time,
    disconn_reason,
    last_update_time,
    agent_conn,
    agent_id,
    agent_username,
    agent_conn_att,
    agent_afw_start,
    agent_afw_end,
    agent_afw_duration,
    agent_interact_duration,
    agent_holds,
    gent_longest_hold,
    queue_id,
    queue_name,
    in_queue_time,
    out_queue_time,
    queue_duration,
    customer_phone,
    customer_voice,
    customer_hold_duration,
    sys_phone,
    conn_to_sys
)
FROM 's3://***-connect-firehose-s3-redshift/<manifest>'
CREDENTIALS 'aws_iam_role=arn:aws:iam::<aws-account-id>:role/<role-name>'
MANIFEST
FORMAT AS JSON 'auto';
