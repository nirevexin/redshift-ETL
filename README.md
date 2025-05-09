# 📦 ETL Automation: Lambdas + Redshift SCD Type 1

This folder contains AWS Lambda functions and Amazon Redshift stored procedures used to automate a modular, low-cost, and highly maintainable ETL pipeline. The system extracts data from multiple sources (S3, Salesforce, Firehose, Google Sheets, Amazon Connect), transforms it, and loads it into Redshift using a **Slowly Changing Dimension (SCD) Type 1** strategy.

---

## 🔁 ETL Workflow (Step-by-Step)

At my company, we implemented an ETL pipeline orchestrated entirely with **AWS Step Functions**, where each stage is handled by a purpose-built **Lambda function**. This design offers transparency, flexibility, and clear traceability of the process:

1. **Step Functions**, **triggered by EventBridge**, orchestrate the sequence of Lambda executions for each data source or object (e.g., `task`, `matter`, `user`, etc.).

2. Each **Lambda** performs:
   - Extraction from systems like **S3, APIs, Firehose, Salesforce, other SQL or NoSQL databases or Google Sheets**
   - **Cleaning and transformation** using `pandas`
   - Depending on the use case:
     - Either exports the transformed data to **S3** in JSON format for batch loading, or  
     - Directly inserts the data into **Redshift** using `psycopg2` for lightweight insert operations

3. If staged in S3, the Lambda triggers a **Redshift `COPY` command** to load the data into a staging table.

4. Finally, a **Redshift stored procedure** is invoked to apply **Slowly Changing Dimension (SCD) Type 1 logic**, updating or inserting records based on the primary key.

> 🔄 This pipeline runs periodically for each object, ensuring modularity and consistency across our data warehouse.

---

## 💡 Why We Use AWS Lambda Instead of AWS Glue

We **intentionally chose Lambda** over Glue for the following reasons:

- 🪙 **Cost-efficiency**: Lambda has zero idle cost and no per-job minimum billing.
- ⚡ **Fast startup and deploy cycles** (no Spark overhead).
- 🧩 **Simplicity**: Each Lambda is a self-contained Python function.
- 📊 **We do not process Big Data volumes**. Our datasets are transactional in nature (e.g., Salesforce tasks, users, matters) and well-suited for Lambda limits.
- 📚 **ETL follows a traditional Kimball-style architecture** with clearly defined fact and dimension tables in Redshift.

---

## 🔧 Components

- **AWS Lambda Functions**  
  Stateless functions that extract, transform, and export data to S3 → Redshift.

- **Amazon Redshift Stored Procedures**  
  Handle deduplication and apply **SCD Type 1 logic** to update records in place.

- **AWS Step Functions**  
  Orchestrate complex workflows, chaining together Lambda executions and error handling logic per object and source.

---
## 📦 Technologies Used in the ETL Pipeline

This ETL pipeline leverages a set of AWS services that together form a modular, cost-efficient, and event-driven data integration architecture:

| Service              | Purpose                                                                 |
|----------------------|-------------------------------------------------------------------------|
| **Amazon S3**         | Receives Salesforce object backups and serves as staging for transformed data  |
| **AWS Lambda**        | Stateless functions for extraction, transformation, and Redshift loading |
| **Amazon Redshift**   | Data warehouse with SCD Type 1 logic applied via stored procedures     |
| **AWS Step Functions**| Orchestrates and sequences Lambda executions per object and workflow   |
| **Amazon DynamoDB**   | Tracks processed folders, keys, or identifiers to ensure idempotency   |
| **Amazon EventBridge**| Schedules Step Function executions at defined intervals                |
| **Amazon CloudWatch** | Captures logs, metrics, and operational visibility for Lambdas and flows |

## 🚀 Key Features

- 🔗 Ingestion from multiple sources: **S3, Firehose, Google Sheets, AWS Connect**
- 🔄 Automated, low-maintenance workflow using **Step Functions**
- 🧠 SCD Type 1 updates with Redshift stored procedures
- 🪶 Lightweight, serverless architecture (no Glue, no Spark)
- 📐 Modeled with Kimball dimensional principles

---

## 📌 Example Objects Processed

- `task`
- `user`
- `matter`
- `case`
- `queue`
- `agent`
- `contact`

Each object has its own Lambda flow, S3 intermediate staging, and Redshift procedure for final merge.

---

## 📈 Architecture Diagram (Mermaid)

```mermaid
graph TD
    A[EventBridge Schedule] --> B[Step Function Execution]

    subgraph ETL Workflow
        B --> C1[Lambda: Extract & Transform Data]
        C1 --> C2{Use S3?}
        C2 -- Yes --> D1[Upload to S3]
        D1 --> D2[Redshift COPY into staging table]
        C2 -- No --> E1[Insert directly into Redshift via psycopg2]
    end

    D2 --> F[Call Redshift Stored Procedure]
    E1 --> F

    F --> G[Target Table with SCD Type 1 logic]


