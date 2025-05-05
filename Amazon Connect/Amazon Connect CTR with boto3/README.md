# ☎️ AWS Connect to Redshift ETL (2-Hour Window Collector)

This Lambda function powers a **temporary ETL solution** that extracts **completed call records from AWS Connect**, processes them, and loads them into **Amazon Redshift** (table: `connect.f_calls_staging`). 

It was created in response to a **production issue** caused by a new feature integration in the `connect` microservice, disrupting the normal flow of **CTR (Contact Trace Record)** data.

---

## ⚠️ Background & Motivation

We recently deployed a new functionality in `connect` to link AWS Connect call records with **Salesforce cases**, which introduced changes in the structure and delivery of CTRs. As a result:

- CTR ingestion via Kinesis became unstable.
- Immediate access to call data for reporting and analytics was impacted.

**This Lambda was designed as a workaround** to **periodically fetch call data directly from AWS Connect APIs**, bypassing the broken CTR pipeline.

---

## ⏱️ Behavior

- **Runs every 2 hours** via scheduler (triggered by EventBridge **40**(minutes)- **0/2**(hours)- **\***(dom) - **\***(month) - **?**(dow) - **\***(year).
- Dynamically computes the previous 2-hour window using New York time (EST/EDT).
- Fetches all **completed calls** in that time frame using `search_contacts`.
- Extracts additional details via `describe_contact`.
- Loads transformed records into Redshift (`connect.f_calls_staging`).
- Triggers a stored procedure `connect.insert_new_f_calls()` to apply SCD Type 1 logic.

---

## 🗃️ Redshift Table

Target table:  
`connect.f_calls_staging`

A stored procedure then moves/merges the staging data into the core facts table `connect.f_calls`.

---

## 🛠️ Core Components

### 🔍 1. Time Window Handling

```python
START_DATE_UTC, END_DATE_UTC, interval_label = get_previous_interval_bounds(now_ny, NY_TZ)
```
## 📁 Files

- **`boto3_connect_redshift.py`**  
  Contains the AWS Lambda function that:
  - Fetches completed call records from AWS Connect
  - Enriches and flattens them
  - Loads them into Redshift staging (`connect.f_calls_staging`)
  - Triggers a stored procedure for final SCD Type 1 processing

- **`boto3_connect_redshift_SP.sql`**  
  Contains the Redshift **stored procedure** (`connect.insert_new_f_calls`) that:
  - Merges staging data into the main `connect.f_calls` table
  - Overwrites records based on `contact_id` (SCD Type 1 logic)

## 🧠 Notes

- This Lambda was implemented as an interim patch, not a permanent replacement for the CTR Firehose.

- Intended to ensure continuity in call data availability during system degradation.

- Can be enhanced to support retries, better logging, and dynamic intervals if needed.
