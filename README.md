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
