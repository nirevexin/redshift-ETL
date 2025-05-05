# 🔄 Salesforce S3 to Redshift ETL Lambda

This Lambda function powers a **serverless ETL pipeline** designed to ingest Salesforce object backups stored in **S3**, transform them using **pandas**, and load them into **Amazon Redshift**. It supports a wide range of Salesforce objects, including but not limited to:

- Tasks  
- Users  
- Matters  
- Custom Litify objects  

---

## 📦 Workflow Overview

1. **Scan S3** for new differential folders containing Salesforce object CSVs.
2. **Check DynamoDB** to avoid reprocessing previously handled folders.
3. **Transform and clean** data using `pandas`:
   - Normalize datetime fields
   - Cast boolean and string fields
   - Standardize column names
4. **Export to JSON** and upload to a temporary S3 staging path.
5. **Load into Redshift staging table** using the `COPY` command.
6. **Trigger a stored procedure** to merge/update the main table using **SCD Type 1 logic**.

---

## 🔧 Technologies Used

| Tool             | Purpose                                      |
|------------------|----------------------------------------------|
| AWS Lambda       | Serverless ETL execution                     |
| AWS S3           | Data source and intermediate storage         |
| AWS DynamoDB     | Track processed folders to prevent duplication |
| AWS Redshift     | Final data warehouse destination             |
| psycopg2         | Redshift connectivity                        |
| pandas           | Data transformation and cleaning             |

---

## 🗂️ Key Components

### 📁 S3 Structure

s3://sfdatabackup-gfproduction/
└── backup/
├── 20240501_Differential/
│ └── Task/
│ ├── task_1.csv
│ ├── task_2.csv
├── 20240502_Differential/
└── Task/


### 🔄 DynamoDB Table

- **Name:** `ProcessedTaskFolders`
- **Purpose:** Track processed folder keys (`folder_key`) with a timestamp (`processed_at`) to prevent duplicates.

---

## 🧪 Object-Specific Logic

Although the example focuses on the **Task** object, this function is designed to **generalize** across multiple Salesforce objects by:
- Using flexible CSV reading logic
- Dynamic transformation rules
- Modular COPY and stored procedure commands (per object)

---

## 🧼 Data Transformation Logic

- **Datetime fields**: Parsed with `pd.to_datetime`, stored in ISO format
- **Boolean fields**: Coerced to `int` (0 or 1)
- **String fields**: Filled and cast to `str`
- **Column names**: Converted to lowercase for Redshift compatibility

---

## 📥 Redshift Loading

- **Target S3 bucket:** `litify-staging`
- **IAM Role:** Used to authorize COPY:  
  `arn:aws:iam::xxxxxxxxxxxx:role/service-role/AmazonRedshift-CommandsAccessRole-YYYYMMDDTHHMMSS`
- **Staging Table:**  
  `litify.task_staging`
- **Stored Procedure:**  
  `CALL litify.update_litify_task();`  
  Applies **SCD Type 1** update logic to insert or overwrite records in the main table.

---

## 🧠 Notes

- Empty folders are marked as processed **only if** they are **not the last** available folder, ensuring late-arriving files aren’t ignored.
- Designed for periodic execution (e.g. via EventBridge) to process new Salesforce backups every few hours or daily.

---

## 🧑‍💻 Author

ETL Pipeline developed by Alexey Vershinin  
@ Abogada Julia – Data Engineering Team  
2025


