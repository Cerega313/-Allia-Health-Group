# -Allia-Health-Group
Test Project

## Architecture

## End-to-end data flow (LifeFile → GCS → BigQuery → BI/AI)

```mermaid
flowchart LR

  %% --- Источник ---
  subgraph LifeFile
    LF_SFTP["LifeFile SFTP<br/>(payments.csv, providers.csv)"]
  end

  %% --- GCP: Ingestion слой ---
  subgraph GCP_Ingestion["GCP Ingestion"]
    CF_SFTP["Cloud Function #1<br/>cf_lifefile_sftp_to_gcs<br/>(Paramiko, 23:45)"]
    GCS_IN["GCS landing<br/>gs://lifefile/raw/incoming/..."]
    GCS_PROC["GCS processed<br/>gs://lifefile/raw/processed/..."]
    CF_LOAD["Cloud Function #2<br/>cf_gcs_to_bq_raw"]
    GCS_POLICIES["Object Versioning + Lifecycle<br/>keep versions 7–14d,<br/>delete/move ≥30–90d"]
  end

  %% --- BigQuery raw + тех.лог ---
  subgraph BQ_Raw["BigQuery: raw & ingestion log"]
    BQ_RAW_PAY["raw_lifefile.payments_raw"]
    BQ_RAW_PROV["raw_lifefile.providers_raw"]
    BQ_ING_LOG["raw_technical.ingestion_log"]
  end

  %% --- Справочник программ/циклов ---
  subgraph BQ_Ref["BigQuery: reference"]
    BQ_PROGRAMS["ref.program_types<br/>(order_type → 3/6/9 months)"]
  end

  %% --- Трансформации через dbt ---
  subgraph BQ_Transform["BigQuery: dbt layers"]
    DBT_STG["Staging<br/>(stg_lifefile__*, stg_ops__*)"]
    DBT_DV["Data Vault<br/>(hubs / links / sats)"]
    DBT_MARTS["Marts<br/>(fact_payment_events,<br/>fact_provider_cycles)"]
    AI_CHURN["ai_churn.provider_cycle_training"]
  end

  %% --- BI / отчёты ---
  subgraph BI["Analytics & BI"]
    LOOKER["Looker dashboards<br/>(provider segments,<br/>revenue, churn risk)"]
  end

  %% --- Потоки данных ---

  %% 1) LifeFile выгружает файлы на SFTP
  LF_SFTP -- "daily export 23:30" --> CF_SFTP

  %% 2) CF #1: SFTP → GCS incoming + переименование
  CF_SFTP -- "download via Paramiko<br/>rename with timestamp+batch" --> GCS_IN

  %% 3) CF #1 пишет ingestion_log (UPLOADED)
  CF_SFTP -- "insert/UPDATE status='UPLOADED'" --> BQ_ING_LOG

  %% policies на GCS
  GCS_IN --- GCS_POLICIES
  GCS_PROC --- GCS_POLICIES

  %% 4) CF #2: GCS incoming → BigQuery raw
  GCS_IN -- "new *.csv.gz" --> CF_LOAD
  CF_LOAD -- "load payments" --> BQ_RAW_PAY
  CF_LOAD -- "load providers" --> BQ_RAW_PROV

  %% 5) CF #2: move to processed + обновить ingestion_log (SUCCESS/FAILED)
  CF_LOAD -- "move file to processed/" --> GCS_PROC
  CF_LOAD -- "update ingestion_log<br/>status='SUCCESS'/'FAILED'" --> BQ_ING_LOG

  %% 6) Справочник программ
  BQ_PROGRAMS --> DBT_STG

  %% 7) dbt: stg → DV → marts
  BQ_RAW_PAY --> DBT_STG
  BQ_RAW_PROV --> DBT_STG

  DBT_STG --> DBT_DV
  DBT_DV --> DBT_MARTS

  %% 8) BI из дата-мартов
  DBT_MARTS --> LOOKER

  %% 9) AI-датасет из дата-мартов
  DBT_MARTS --> AI_CHURN
