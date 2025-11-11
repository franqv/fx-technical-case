# 🚀 FX Rates ETL Pipeline (GCP)

This project implements a comprehensive ETL (Extract, Transform, Load) pipeline using Python and Google Cloud Platform (GCP) services to ingest, transform, and store daily foreign exchange (FX) rates in BigQuery.

The pipeline is designed to be fully reproducible, scalable, and optimized for analytical querying, particularly **Year-to-Date (YTD) calculations**.

**You can have a look at the design note in this file: [Design Note and Justification](Design_note.md)**
---

## 🧭 Table of Contents

* [1. Architecture and Components](#1-architecture-and-components)
* [2. Prerequisites and Local Setup](#2-prerequisites-and-local-setup)
* [3. Setup: BigQuery DWH Creation (DDL)](#3-setup-bigquery-dwh-creation-ddl)
* [4. Deployment and Execution Steps](#4-deployment-and-execution-steps)
    * [4.1 Deployment of Cloud Functions](#41-deployment-of-cloud-functions)
    * [4.2 Initial Historical Load](#42-initial-historical-load)
    * [4.3 Daily Scheduled Pipeline Setup](#43-daily-scheduled-pipeline-setup)
* [5. Data Validation and Example Queries](#5-data-validation-and-example-queries)

---

## 1. Architecture and Components

The pipeline leverages two distinct Google Cloud Functions to handle different execution scenarios:

| Component | Role | Trigger Type | Function Name |
| :--- | :--- | :--- | :--- |
| **Initial Load** | Fetches and loads **all data from Jan 1st** to current date. | **HTTP** | `etl_fx_load_all_year_data_function` |
| **Daily Update** | Fetches and loads **only the most recent day's** data. | **Pub/Sub (Scheduled)** | `update_today_ebc_data_function` |
| **Data Warehouse** | Google BigQuery | - | - |
| **Orchestration** | Google Cloud Scheduler | - | - |

---

## 2. Prerequisites and Local Setup

### A. System Requirements

1.  **GCP Project:** A Google Cloud Project with Billing enabled.
2.  **GCP CLI:** Google Cloud CLI (`gcloud`) installed and authenticated.
3.  **Poetry:** The Poetry package manager must be installed globally.

### B. Local Environment Configuration (Poetry)

To set up the project locally for testing:

1.  **Install Dependencies:** Creates the virtual environment and installs all packages.

    ```bash
    poetry install
    ```

2.  **Activate Shell:** Activates the virtual environment.

    ```bash
    poetry shell
    ```

3.  **GCP Authentication (ADC):** Authenticate your user for Application Default Credentials (ADC) to permit local BigQuery operations.

    ```bash
    gcloud auth application-default login
    ```

### C. Environment Variables (`.env`)

Create the following file to define your configuration. These variables **must** be set in your Cloud Function's environment settings upon deployment.

```
# --- Google Cloud Configuration ---
# Your GCP Project ID
GCP_PROJECT_ID="your-gcp-project-id"

# BigQuery Dataset and Table Names
BIGQUERY_DATASET_ID="fx_data"
BIGQUERY_TABLE_ID="fact_exchange_rates"
```

---

## 3. Setup: BigQuery DWH Creation (DDL)

The first step is to create the BigQuery Dataset and the destination table.

### A. Create BigQuery Dataset

```bash
gcloud bigquery datasets create ${BIGQUERY_DATASET_ID} --project=${GCP_PROJECT_ID}
```

### B. Create Table DDL

Execute the following BigQuery Standard SQL to create the destination table. It is **Partitioned by Date** and **Clustered by Currency Pair** for efficient querying.

```sql
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}`
(
    exchange_date DATE NOT NULL,
    base_currency STRING NOT NULL,
    quote_currency STRING NOT NULL,
    rate BIGNUMERIC NOT NULL,
    rate_inverse BIGNUMERIC,
    data_source STRING,
    load_timestamp TIMESTAMP NOT NULL
)
-- KEY: Partition by date for cost-effective YTD/date-range queries
PARTITION BY
    exchange_date
-- KEY: Cluster by currency pair for fast JOINs/lookups
CLUSTER BY
    base_currency, quote_currency;
```

---

## 4. Deployment and Execution Steps

### 4.1 Deployment of Cloud Functions

Both functions (`etl_fx_load_all_year_data_function` and `update_today_ebc_data_function`) must be deployed to Google Cloud Functions (V2 recommended), ensuring that the appropriate trigger (`HTTP` or `Pub/Sub`) and the environment variables (`GCP_PROJECT_ID`, etc.) are configured.

### 4.2 Initial Historical Load

Once the **HTTP** function (`etl_fx_load_all_year_data_function`) is deployed, it must be triggered once to perform the initial, full-year data load into BigQuery.

1.  **Action:** Invoke the HTTP endpoint of the deployed function (via `curl` or browser).
2.  **Result:** The function extracts historical data from January 1st to the current date and loads it into the target BigQuery table.

### 4.3 Daily Scheduled Pipeline Setup

Once the table contains historical data, the daily update pipeline is set up.

1.  **Action:** Configure a **Cloud Scheduler Job** to publish a message to a **Pub/Sub Topic** daily (e.g., at 08:00 AM CET).
2.  **Result:** The **Pub/Sub** function (`update_today_ebc_data_function`), which is subscribed to this topic, is activated daily to fetch and append only the latest exchange rates.

---

## 5. Data Validation and Example Queries

These queries demonstrate that the dataset is usable for analytical tasks once the table is populated.

### A. Lookup by Date and Currency Pair

Retrieves the rate for a specific cross-pair ($\text{NOK/SEK}$) on a given date.

```sql
SELECT
    exchange_date,
    base_currency,
    quote_currency,
    rate
FROM
    `${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}`
WHERE
    exchange_date = DATE '2025-11-05'
    AND base_currency = 'NOK'
    AND quote_currency = 'SEK'
LIMIT 1;
```

### B. Year-to-Date (YTD) Calculation

**YTD Definition:** The percentage change in the exchange rate between the start of the current calendar year (January 1st) and the most recent rate available in the DWH.

```sql
WITH LatestRate AS (
    -- 1. Get the latest rate loaded into the DWH
    SELECT
        base_currency,
        quote_currency,
        rate AS latest_rate
    FROM
        `${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}`
    WHERE
        exchange_date = (SELECT MAX(exchange_date) FROM `${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}`)
),
StartOfYearRate AS (
    -- 2. Get the rate from January 1st of the current year
    SELECT
        base_currency,
        quote_currency,
        rate AS soy_rate
    FROM
        `${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${BIGQUERY_TABLE_ID}`
    WHERE
        exchange_date = DATE '2025-01-01'
)
-- 3. Join and calculate the YTD change: ((Rate_Today / Rate_Jan1) - 1) * 100
SELECT
    lr.base_currency,
    lr.quote_currency,
    lr.latest_rate,
    syr.soy_rate,
    ((lr.latest_rate / syr.soy_rate) - 1) * 100 AS ytd_change_percent
FROM
    LatestRate lr
JOIN
    StartOfYearRate syr
ON
    lr.base_currency = syr.base_currency
    AND lr.quote_currency = syr.quote_currency
ORDER BY
    ytd_change_percent DESC;