# 📦 ETL Automation: Lambdas + Redshift SCD Type 1

This folder contains AWS Lambda functions and Amazon Redshift stored procedures used to automate an end-to-end ETL pipeline. The system extracts data from multiple sources (S3, Firehose, Google Sheets, Amazon Connect, etc.), transforms it, and loads it into Redshift using a **Slowly Changing Dimension (SCD) Type 1** strategy.

## 🔧 Components

- **AWS Lambda Functions**  
  Handle data extraction, transformation, and orchestration of Redshift loading.
  
- **Amazon Redshift Stored Procedures**  
  Perform deduplication, cleansing, and SCD Type 1 logic to overwrite existing records based on unique identifiers.

## 🚀 Features

- Multi-source data ingestion (S3, APIs, etc.)
- Scalable and automated ETL
- Real-time and batch update support
- SCD Type 1 logic (overwrite on key match)
- Optimized for analytics and reporting



