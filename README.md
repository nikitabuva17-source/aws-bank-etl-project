# AWS Bank Customer ETL Pipeline

## 📌 Project Overview
This project demonstrates an end-to-end ETL pipeline using AWS services.
Raw banking customer data (CSV) is processed using AWS Glue and PySpark,
converted into optimized Parquet format, and queried using Amazon Athena.

---

## 🛠️ Technologies Used
- Amazon S3
- AWS Glue
- PySpark
- AWS Glue Data Catalog
- Amazon Athena
- AWS CloudWatch

---

## 🔄 ETL Workflow
1. Upload raw CSV data to Amazon S3.
2. Create Glue Data Catalog table.
3. Build AWS Glue ETL job using PySpark.
4. Clean and transform data.
5. Convert to Parquet format.
6. Store processed data in S3.
7. Query processed data using Athena.

---

## 📊 Monitoring
- Glue Job Run status monitoring
- CloudWatch logs for debugging
- Performance metrics tracking

---

## 🚀 Outcome
Built a scalable and cost-effective ETL solution for banking customer data.
