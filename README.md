# NYC Taxi Data Engineering Project 🚕

This repository documents my journey through the Data Engineering Zoomcamp. The primary goal of this project is to build a robust, production-grade data pipeline for the NYC Taxi and Limousine Commission (TLC) dataset, moving data from raw web sources into a scalable cloud data warehouse.



## 🎯 Project Intent
The intent of this project is to apply Data Engineering best practices to handle large-scale datasets. This includes:
* **Infrastructure as Code (IaC)** to manage cloud resources.
* **Orchestration** to automate data movement and handle backfills.
* **Data Lake & Warehousing** to optimize storage and query performance.
* **Workflow Automation** to ensure data quality and system reliability.

---

## 🛠️ Tech Stack
* **Cloud Provider:** Google Cloud Platform (GCP)
* **Infrastructure:** Terraform
* **Orchestration:** Airflow (running on Docker)
* **Data Lake:** Google Cloud Storage (GCS)
* **Data Warehouse:** BigQuery
* **Languages:** Python (Pandas/Requests), SQL
* **Data Format:** Parquet (Optimized Columnar Storage)

---

## 📂 Repository Structure

The project is organized by modules, each representing a different layer of the data engineering lifecycle:

| Folder | Focus | Key Tools |
| :--- | :--- | :--- |
| **`01-docker-terraform`** | Foundation | Docker, Postgres, Terraform (IaC) |
| **`02-workflow-orchestration`** | Pipeline Automation | Airflow |
| **`03-gcp`** | Cloud Fundamentals | GCS, BigQuery, IAM Roles |
| **`04-data-warehousing-bq`** | Analytics & Optimization | BigQuery (Partitioning & Clustering) |

---

## 🚀 Featured Pipeline: 2024 Yellow Taxi Ingestion
Currently, the project is focused on the **January 2024 - June 2024** subset of the Yellow Taxi dataset.

### Current Implementation:
1.  **Orchestration:** Airflow DAGs automate the monthly extraction of Parquet files from the TLC source.
2.  **Storage:** Data is streamed directly to a GCS bucket, organized by year and month.
3.  **Data Modeling:** Implementation of External and Native tables in BigQuery with specialized partitioning to minimize query costs.

---

## 📈 Key Learnings
* **Idempotency:** Designing DAGs that can be rerun multiple times without duplicating data.
* **Storage Efficiency:** Utilizing Parquet and GCS lifecycle rules to manage costs.
* **Query Optimization:** Using Clustering and Partitioning to reduce the amount of data scanned by BigQuery.