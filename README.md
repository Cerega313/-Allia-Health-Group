# Allia Health Group — Provider Churn / LifeFile Ingestion (Test Project)

This repository contains a small end-to-end data pipeline around **LifeFile payment / provider data** for Allia Health Group:

- Ingestion from **LifeFile SFTP** into **Google Cloud Storage (GCS)**.
- Loading **raw CSV** into **BigQuery raw** tables.
- Technical **ingestion log** with idempotency.
- Downstream modeling in **dbt** towards BI marts and an **AI-ready churn dataset**.

The focus is on **pipeline robustness, idempotency, and clear modeling** for provider churn analysis.


## 1. High-level architecture

### End-to-end data flow (LifeFile → GCS → BigQuery → BI/AI)

```mermaid
flowchart LR

  %% --- Source system ---
  subgraph LifeFile
    LF_SFTP["LifeFile SFTP<br/>(payments*.csv, providers*.csv)<br/>daily 23:30"]
  end

  %% --- Orchestration (Cloud Scheduler) ---
  subgraph GCP_Scheduler["Orchestration"]
    CS["Cloud Scheduler<br/>(HTTP trigger 23:45 daily)"]
  end

  %% --- GCP: Ingestion layer ---
  subgraph GCP_Ingestion["GCP Ingestion"]
    CF_SFTP["Cloud Function #1<br/>cf_lifefile_sftp_to_gcs<br/>"]
    GCS_IN["GCS landing<br/>gs://lifefile/raw/incoming/..."]
    GCS_PROC["GCS processed<br/>gs://lifefile/raw/processed/..."]
    GCS_POLICIES["Object Versioning + Lifecycle<br/>"]
    CF_LOAD["Cloud Function #2<br/>cf_gcs_to_bq_raw<br/>"]
  end

  %% --- BigQuery raw + ingestion_log ---
  subgraph BQ_Raw["BigQuery: raw & ingestion log"]
    BQ_RAW_PAY["raw_lifefile.payments_raw"]
    BQ_RAW_PROV["raw_lifefile.providers_raw"]
    BQ_ING_LOG["raw_technical.ingestion_log"]
  end

  %% --- Reference tables (program / cycles) ---
  subgraph BQ_Ref["BigQuery: reference"]
    BQ_PROGRAMS["crm.order_types<br/>"]
  end

  %% --- Transformations via dbt ---
  subgraph BQ_Transform["BigQuery: dbt layers"]
    DBT_STG["Staging<br/>"]
    DBT_DV["Data Vault<br/>"]
    DBT_MARTS["Marts<br/>"]
    AI_CHURN["ai_churn.provider_cycle_training"]
  end

  %% --- BI / reporting ---
  subgraph BI["Analytics & BI"]
    LOOKER["Looker dashboards<br/>"]
  end

  %% --- Flows ---

  %% Scheduler → CF #1
  CS --> CF_SFTP

  %% LifeFile SFTP → CF #1
  LF_SFTP -- "SFTP pull (Paramiko)" --> CF_SFTP

  %% CF #1: SFTP → GCS landing
  CF_SFTP -- "download<br/>rename with timestamp + batch" --> GCS_IN

  %% CF #1: ingest log (UPLOADED/FAILED)
  CF_SFTP -- "INSERT ingestion_log<br/>status = 'UPLOADED' / 'FAILED'" --> BQ_ING_LOG

  %% Bucket policies (versioning / lifecycle)
  GCS_IN --- GCS_POLICIES
  GCS_PROC --- GCS_POLICIES

  %% CF #2: GCS → BQ raw (triggered by object finalize)
  GCS_IN -- "GCS object finalize<br/>" --> CF_LOAD
  CF_LOAD -- "LOAD payments" --> BQ_RAW_PAY
  CF_LOAD -- "LOAD providers" --> BQ_RAW_PROV

  %% CF #2: ingestion_log update after load
  CF_LOAD -- "UPDATE ingestion_log<br/>status = 'SUCCESS' / 'FAILED',<br/>" --> BQ_ING_LOG

  %% CF #2: move successfully loaded files
  CF_LOAD -- "move incoming/ → processed/" --> GCS_PROC

  %% Reference tables join into staging
  BQ_PROGRAMS --> DBT_STG

  %% Raw → Staging
  BQ_RAW_PAY --> DBT_STG
  BQ_RAW_PROV --> DBT_STG

  %% dbt layers: staging → DV → marts
  DBT_STG --> DBT_DV
  DBT_DV --> DBT_MARTS

  %% BI from marts
  DBT_MARTS --> LOOKER

  %% AI training dataset from marts
  DBT_MARTS --> AI_CHURN

``` 
## 2. Ingestion layer: SFTP → GCS → BigQuery raw

### 2.1 Agreements with the LifeFile team

- LifeFile drops export files to SFTP **every day at 23:30 UTC**.
  If the export fails, they perform a **second attempt at 23:35 UTC**.
- Technical point of contact: **[X@email.com](mailto:X@email.com)**.
- A manual export can be triggered by LifeFile following the documented procedure (**{instructions}**).
- Each daily export contains **only new and updated records** for the current day, plus a **30-minute overlap from the previous day** to capture late updates.
- Files are kept on the LifeFile side for **1 month**.
- Files are delivered in **CSV** format, with names such as
  `provider_452271717.csv` and `payment_656463262.csv`.
- If there is no data, the file is downloaded empty.

### 2.2 Files structure
The file structure is described in **[dbt/models/staging/_src__aliia_health.yml](https://github.com/Cerega313/-Allia-Health-Group/blob/main/dbt/models/staging/_src__aliia_health.yml)**.
[Uploading lifefile_providers_raw_2025_10_11.csv…]()


### 2.3 Cloud Function #1 — cf_lifefile_sftp_to_gcs

#### Purpose

 - Pull batch files from LifeFile SFTP once per day.

 - Store them in GCS landing as compressed CSV.

 - Write a row into raw_technical.ingestion_log for each SFTP file.

 - Guarantee idempotency on SFTP file level.

#### Trigger

- Cloud Scheduler (23:45 daily). LifeFile is responsible for producing SFTP exports daily around 23:30.

### 2.4 Cloud Function #2 — cf_gcs_to_bq_raw

#### Purpose

- Load CSV files from GCS landing into BigQuery raw tables

- Update raw_technical.ingestion_log with final load status (SUCCESS / FAILED).

- Move successfully loaded files into GCS processed.

#### Trigger

- GCS event: google.storage.object.finalize for objects in gs://<GCS_BUCKET_NAME>/raw/incoming/....

## 3. Ingestion log — raw_technical.ingestion_log

### DDL

```sql
CREATE TABLE IF NOT EXISTS raw_technical.ingestion_log (
  file_type       STRING,  -- 'payments', 'providers', etc.
  file_date       DATE,    -- logical file date (UTC) based on ingestion time or filename
  source_system   STRING,  -- 'lifefile'
  source_path     STRING,  -- full SFTP path, e.g. '/outgoing/payments_2025-11-15.csv'

  gcs_uri         STRING,  -- gs://bucket/raw/incoming/.../.csv.gz
  gcs_generation  INT64,   -- GCS object generation
  gcs_md5         STRING,  -- MD5 hash of object content (base64)
  gcs_size_bytes  INT64,   -- size of object in bytes

  target_dataset  STRING,  -- BigQuery dataset loaded ('raw_lifefile')
  target_table    STRING,  -- BigQuery table loaded ('payments_raw' / 'providers_raw')
  load_job_id     STRING,  -- BigQuery job id for load

  started_at      TIMESTAMP,  -- when SFTP→GCS ingestion started
  finished_at     TIMESTAMP,  -- when GCS→BQ load finished (or failed)

  status          STRING,  -- 'UPLOADED' | 'SUCCESS' | 'FAILED'
  error_message   STRING   -- error message in case of failure
);
```

## 4. Trade-off: Postgres vs BigQuery

#### Workload and data profile
Postgres is a great fit for transactional and mixed workloads, but as you accumulate years of payment and provider history and start running complex analytical queries (year-level scans, multi-table joins), it tends to hit instance resource limits. BigQuery is designed as a columnar analytical warehouse and handles growing volumes and heavy analytics much more comfortably.

#### Infrastructure, operations and DevOps
Postgres requires instance management (sizing, HA, upgrades, tuning, reporting replicas). BigQuery is a serverless warehouse: scale, resilience and maintenance are handled by GCP, and the data team can focus on models, pipelines and data quality instead of database operations.

#### Cost
In a production scenario, Postgres costs grow in steps: as data and workload increase, you have to move to larger (and more expensive) instances, often with extra headroom “for the future”. BigQuery’s model is based on storage plus bytes actually scanned: for analytical workloads with peaks (reporting periods, ad-hoc analysis), this gives a more flexible and controllable TCO. With proper partitioning and clustering (e.g. by `payment_date` and `provider_id`), you can keep query costs under control without over-provisioning a large database cluster.

#### Analytics, BI and AI
BigQuery integrates natively with Looker: marts built in dbt can be exposed directly as BI-ready models for segmenting providers by cycle, revenue, cost-to-serve and profitability. On top of the same tables it’s easy to build the AI dataset (`ai_churn` schema) and feed it into BigQuery ML / Vertex AI / notebooks without extra copies or data movement. With Postgres, both BI and ML always live “outside” and require additional glue code and infrastructure.

#### Why BigQuery is the better choice for this project
For the provider churn use case in the SFTP → GCS → Cloud Functions → DWH → Looker → `ai_churn` stack, BigQuery provides a more cohesive and scalable solution:

- as data volumes grow, columnar storage and distributed execution in BigQuery allow much faster heavy aggregations and joins; at the current sample size performance is similar, but in production BigQuery delivers a real latency advantage;
- there is no database cluster to administer, and the system scales with the history of payments, providers and models;
- it fits naturally with dbt, Looker and the AI layer, enabling a single analytical environment in GCP without extra intermediary systems.

Therefore, for LifeFile + CRM analytics, BI dashboards and the churn training dataset, BigQuery is the natural choice for the main DWH, while Postgres can still be used (if needed) for transactional systems and operational microservices.




## 5. Downstream modeling (dbt) — overview

The full implementation lives under /dbt
Below is a conceptual overview aligned with the architecture diagram.

## dbt model lineage (CRM & LifeFile → Raw Vault → Business Vault → Marts → ai_churn)

```mermaid
flowchart LR

  %% ---------- Sources ----------
  subgraph SRC_CRM["Sources: CRM"]
    CRM_PROV["crm.provider"]
    CRM_ORDT["crm.order_types"]
  end

  subgraph SRC_LF["Sources: LifeFile"]
    LF_PROV["raw_lifefile.providers_raw"]
    LF_PAY["raw_lifefile.payments_raw"]
  end

  %% ---------- Staging ----------
  subgraph STG["dbt staging"]
    STG_CRM_PROV["stg_crm__providers"]
    STG_CRM_ORDT["stg_crm__order_types"]
    STG_LF_PAY["stg_lifefile__payments"]
    STG_LF_PROV["stg_lifefile__providers"]
    STG_PROV_CONF["int_provider_conformed\n(join CRM + LifeFile by NPI)"]
  end

  %% ---------- Raw Vault ----------
  subgraph DV_RAW["dbt raw_vault (Data Vault)"]
    HUB_PROV["hub_provider"]
    HUB_ORDER["hub_order"]
    HUB_PAY["hub_payment"]

    LINK_PAY_ORDER["link_payment_order_l"]
    LINK_PAY_PROV["link_payment_provider_l"]
    LINK_PROV_ORDER["link_provider_order_l"]

    SAT_PAY_FIN_RAW["sat_payment_financials_v0"]
    SAT_PAY_CTX["sat_payment_context_v0"]
    SAT_PAY_TECH["sat_payment_technical_v0"]

    SAT_PROV_PROFILE_RAW["sat_provider_profile_v0"]
    SAT_PROV_SUB_RAW["sat_provider_subscription_v0\n(raw subscription events)"]
  end

  %% ---------- Business Vault ----------
  subgraph DV_BV["dbt bv (Business Vault)"]
    SAT_PAY_FIN_SCD["sat_payment_financials_scd2"]
    SAT_PROV_SUB_SCD["sat_provider_subscription_scd2\n(SCD2 subscription history)"]
  end

  %% ---------- Marts & AI ----------
  subgraph MARTS["dbt marts & ai"]
    FACT_PAY["fact_payment_events"]
    FACT_PROV_SUB["fact_provider_subscription_history"]
    DIM_PROV["dim_provider"]
    DIM_PROGRAM["dim_program_cycle"]
    AI_CHURN["ai_churn.provider_cycle_training"]
  end

  %% ---------- Sources → Staging ----------
  CRM_PROV --> STG_CRM_PROV
  CRM_ORDT --> STG_CRM_ORDT

  LF_PAY --> STG_LF_PAY
  LF_PROV --> STG_LF_PROV

  %% Provider conformance by NPI
  STG_CRM_PROV --> STG_PROV_CONF
  STG_LF_PROV --> STG_PROV_CONF

  %% ---------- Staging → Raw Vault ----------
  %% Payments → hubs + satellites
  STG_LF_PAY --> HUB_PAY
  STG_LF_PAY --> SAT_PAY_FIN_RAW
  STG_LF_PAY --> SAT_PAY_CTX
  STG_LF_PAY --> SAT_PAY_TECH

  %% Orders / programs from CRM
  STG_CRM_ORDT --> HUB_ORDER

  %% Providers (conformed CRM + LifeFile)
  STG_PROV_CONF --> HUB_PROV
  STG_PROV_CONF --> SAT_PROV_PROFILE_RAW

  %% Provider–Order relationship (subscription link)
  STG_CRM_PROV --> LINK_PROV_ORDER
  STG_CRM_ORDT --> LINK_PROV_ORDER

  %% Raw subscription events (open / pause / resume / cancel)
  STG_CRM_PROV --> SAT_PROV_SUB_RAW
  STG_CRM_ORDT --> SAT_PROV_SUB_RAW

  %% Payment links (from LifeFile)
  HUB_PAY --> LINK_PAY_ORDER
  HUB_ORDER --> LINK_PAY_ORDER

  HUB_PAY --> LINK_PAY_PROV
  HUB_PROV --> LINK_PAY_PROV

  %% ---------- Raw Vault → Business Vault ----------
  SAT_PAY_FIN_RAW --> SAT_PAY_FIN_SCD
  SAT_PROV_SUB_RAW --> SAT_PROV_SUB_SCD

  %% ---------- Business Vault → Marts ----------
  %% Payment facts
  HUB_PAY --> FACT_PAY
  HUB_ORDER --> FACT_PAY
  HUB_PROV --> FACT_PAY
  LINK_PAY_ORDER --> FACT_PAY
  LINK_PAY_PROV --> FACT_PAY
  SAT_PAY_CTX --> FACT_PAY
  SAT_PAY_FIN_SCD --> FACT_PAY

  %% Provider dimension
  HUB_PROV --> DIM_PROV
  SAT_PROV_PROFILE_RAW --> DIM_PROV

  %% Program dimension (3 / 6 / 9-month cycles)
  STG_CRM_ORDT --> DIM_PROGRAM
  SAT_PROV_SUB_SCD --> DIM_PROGRAM

  %% Provider subscription history mart (SCD2-style)
  HUB_PROV --> FACT_PROV_SUB
  HUB_ORDER --> FACT_PROV_SUB
  LINK_PROV_ORDER --> FACT_PROV_SUB
  SAT_PROV_SUB_SCD --> FACT_PROV_SUB

  %% AI churn dataset from marts / dims
  FACT_PAY --> AI_CHURN
  FACT_PROV_SUB --> AI_CHURN
  DIM_PROV --> AI_CHURN
  DIM_PROGRAM --> AI_CHURN
```
