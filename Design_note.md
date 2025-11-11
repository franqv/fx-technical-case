# Design Note: FX Rates ETL Pipeline

## 1. Architecture and Cloud Provider Choice (GCP)

The solution was implemented using Google Cloud Platform (GCP) due to its suite of serverless services and excellent integration for Data Warehousing workloads.

| Decision | Justification |
| :--- | :--- |
| **Cloud Functions** | Chosen for the ETL logic due to its pay-per-use serverless model. It enables fast, scalable execution, perfect for periodic, short-duration ETL tasks. It eliminates the need to manage servers (VMs). |
| **BigQuery (DW)** | Selected as the destination for its nature as a modern Data Warehouse, designed for rapid analysis of large data volumes. It is ideal for the ad-hoc and complex queries (like YTD) required. |
| **Cloud Scheduler + Pub/Sub** | This combination decouples the orchestration (Scheduler) from the execution trigger (Pub/Sub), making the daily data flow more robust and resilient. Pub/Sub acts as a buffer in case of temporary function failures. |

### Justification of Cloud Functions vs. Orchestrators (Cloud Composer/Azure Data Factory)

Cloud Functions was chosen over complex orchestrators like Cloud Composer (Apache Airflow) or Azure Data Factory for the following key reasons:

* **Simplicity of the Use Case:** The task is a **direct and linear extract, transform, and load (E -> T -> L)** from a single source. There is no complex graph of dependencies (DAG), branching, or need to manage multiple data sources with complex fan-out/fan-in sequences. An orchestrator introduces unnecessary overhead for this task.

* **Operational Cost:** Cloud Functions operates under a **pay-per-use** model that is extremely economical for short, periodic executions. Conversely, Cloud Composer requires an Airflow cluster to run 24/7 (even in its smallest version), which is significantly more expensive for such a simple pipeline.

* **Maintenance and Overhead:** Cloud Functions is a *fully managed* service that does not require infrastructure administration, Airflow updates, or worker management. Composer and Data Factory demand greater knowledge and maintenance effort.

* **Development Speed (Time to Market):** The development and deployment of a Cloud Function is fast, allowing this pipeline to be put into production in minutes, whereas configuring and deploying a DAG in Airflow is a much longer process.

In summary, Cloud Functions is the **most suitable, cost-effective, and lowest-maintenance tool** for a simple ETL pipeline like the daily update of exchange rates.

## 2. Ingestion and ETL Strategy (Dual Function)

A dual Cloud Function strategy was chosen to efficiently manage the initial historical load and the daily update:

| Function | Purpose and Justification |
| :--- | :--- |
| **`etl_fx_load_all_year_data_function`** (HTTP) | **Initial Historical Load:** Triggered manually via HTTP. Its goal is to load the entire date range (Jan 1st - Today) in a single run. HTTP is used because it facilitates immediate status verification and ad-hoc execution management. |
| **`update_today_ebc_data_function`** (Pub/Sub) | **Daily Update:** Triggered by the Pub/Sub event (scheduled by Cloud Scheduler). It only processes today's data, ensuring **idempotency** and minimizing daily execution cost/time. |

## 3. Data Source and Transformation

### A. Extraction (E)

* **Source Chosen:** The **European Central Bank (ECB) API** was selected.

* **Justification:** It is a public, free, and highly reliable source. As one of the required currencies is EUR, the ECB provides the ideal base for consistently calculating all other cross-pairs.

### B. Transformation (T)

* **Cross-Pair Calculation:** The key functionality is generating **all required cross-pairs** (NOK, EUR, SEK, PLN, RON, DKK, CZK). This is done in the transformation layer (implicit in `etl_fx_function`), creating a unique row for each currency pair and date, which greatly simplifies subsequent querying in the DWH.

## 4. Data Modeling (BigQuery DWH)

The schema design is optimized for efficiency in analytical queries, meeting the objective of making it easy to relate to other DWH tables.

| Element | DDL | Justification |
| :--- | :--- | :--- |
| **Schema** | `fact_exchange_rates` | A fact table model is used to store metrics at a daily granularity per currency pair. This is a DWH standard and facilitates JOINs with dimension tables (e.g., `dim_transactions`). |
| **Partitioning** | `PARTITION BY exchange_date` | **Crucial for efficiency.** By partitioning by date, queries involving time ranges (like YTD) only scan necessary partitions, reducing costs and execution time. |
| **Clustering** | `CLUSTER BY base_currency, quote_currency` | Optimizes storage and query speed for common filters (e.g., `WHERE base_currency = 'NOK'`) and improves performance when JOINing with other dimension tables. |
| **Data Type** | `rate BIGNUMERIC` | Chosen to ensure maximum precision in exchange rates, avoiding floating-point errors that can be critical in financial analysis. |

## 5. Year-to-Date (YTD) Definition

### Analytical Definition

**YTD** (Year-to-Date) is defined as the **percentage change** in the value of a specific exchange rate between:

1. The value recorded at the **start of the calendar year** (January 1st).

2. The **latest available value** loaded into the Data Warehouse.

### Implementation

The YTD query implementation uses Common Table Expressions (`WITH LatestRate AS (...)`) in BigQuery to calculate this difference efficiently, which is only possible thanks to the table structure (Partitioning by date).