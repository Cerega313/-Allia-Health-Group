# main_gcs_to_bq_raw.py
"""
Cloud Function: GCS landing → BigQuery raw (LifeFile).

High-level flow:
- Trigger: GCS "finalize" event for objects in the LifeFile landing bucket.
- For each new object (e.g. gs://lifefile/raw/incoming/payments/...):
    * determine file_type from the GCS path (payments/providers/etc),
    * check ingestion_log to see if this GCS object was already successfully loaded,
    * if already loaded → skip,
    * if new:
        - run a BigQuery LOAD job from GCS into the appropriate raw table,
        - on SUCCESS:
            * update ingestion_log (status='SUCCESS', target_dataset/table, load_job_id),
            * move object from raw/incoming/... to raw/processed/...
        - on FAILURE:
            * update ingestion_log (status='FAILED', error_message, load_job_id if any),
            * keep file in raw/incoming/... for investigation / re-run.
"""

import datetime as dt
from typing import Optional

from google.cloud import bigquery
from google.cloud import storage

from config import CONFIG  # shared config for SFTP/GCS/BQ


# ==========================
#   Configuration bindings
# ==========================

GCS_BUCKET_NAME = CONFIG.gcs_bucket_name
GCS_INCOMING_PREFIX = CONFIG.gcs_incoming_prefix          # e.g. "raw/incoming"
GCS_PROCESSED_PREFIX = CONFIG.gcs_processed_prefix        # e.g. "raw/processed"

# Normalize prefixes (no trailing slash)
INCOMING_PREFIX = GCS_INCOMING_PREFIX.rstrip("/")
PROCESSED_PREFIX = GCS_PROCESSED_PREFIX.rstrip("/")

BQ_PROJECT = CONFIG.bq_project
BQ_DATASET_ING = CONFIG.bq_dataset_ing    # e.g. "raw_technical"
BQ_TABLE_ING = CONFIG.bq_table_ing        # e.g. "ingestion_log"

SOURCE_SYSTEM = CONFIG.source_system      # "lifefile"

# Dataset for raw LifeFile tables (you can move this to CONFIG as well if you want)
LIFEFILE_RAW_DATASET = "raw_lifefile"

# file_type → raw table name mapping
FILETYPE_TO_RAW_TABLE = {
    "payments": "payments_raw",
    "providers": "providers_raw",
    # extend for other LifeFile feeds if needed
}

# Global clients
bq_client = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()
storage_client = storage.Client()


# =====================================
#   Helper functions
# =====================================

def _parse_file_type_from_object_name(object_name: str) -> Optional[str]:
    """
    Extract file_type from the GCS object name.

    Expected path structure:
      <INCOMING_PREFIX>/<file_type>/YYYY/MM/DD/<filename>

    Example:
      raw/incoming/payments/2025/11/15/payments_2025-11-15T23-45-03Z_batch01.csv.gz
        → file_type = "payments"

    Returns:
        file_type if it can be inferred and is known in FILETYPE_TO_RAW_TABLE,
        otherwise None.
    """
    parts = object_name.split("/")
    prefix_parts = INCOMING_PREFIX.split("/")

    # Need at least: prefix + file_type + yyyy + mm + dd + filename
    if len(parts) < len(prefix_parts) + 2:
        return None

    # Ensure object is under the configured incoming prefix
    if parts[:len(prefix_parts)] != prefix_parts:
        return None

    file_type = parts[len(prefix_parts)]
    if file_type in FILETYPE_TO_RAW_TABLE:
        return file_type

    return None


def _is_gcs_object_already_loaded(gcs_uri: str) -> bool:
    """
    Check if this GCS object has already been successfully loaded into raw tables.

    We use ingestion_log and consider the object "already loaded" if:
      - there exists a row with this gcs_uri AND
      - source_system = 'lifefile' AND
      - target_dataset and target_table are NOT NULL AND
      - status = 'SUCCESS'
    """
    table_full_name = f"{bq_client.project}.{BQ_DATASET_ING}.{BQ_TABLE_ING}"

    query = f"""
      SELECT 1
      FROM `{table_full_name}`
      WHERE gcs_uri = @gcs_uri
        AND source_system = @source_system
        AND target_dataset IS NOT NULL
        AND target_table IS NOT NULL
        AND status = 'SUCCESS'
      LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
            bigquery.ScalarQueryParameter("source_system", "STRING", SOURCE_SYSTEM),
        ]
    )

    result = bq_client.query(query, job_config=job_config).result()

    for _ in result:
        return True

    return False


def _update_ingestion_log_after_load(
    *,
    gcs_uri: str,
    file_type: Optional[str],
    target_dataset: Optional[str],
    target_table: Optional[str],
    load_job_id: Optional[str],
    finished_at: dt.datetime,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Update the corresponding ingestion_log row for this GCS object.

    Used for both SUCCESS and FAILED load attempts.

    On SUCCESS:
      - status         = 'SUCCESS'
      - target_dataset = 'raw_lifefile'
      - target_table   = 'payments_raw' / 'providers_raw'
      - load_job_id    = BigQuery job id
      - error_message  = NULL

    On FAILED:
      - status         = 'FAILED'
      - target_dataset = attempted dataset (optional, usually same as SUCCESS)
      - target_table   = attempted table
      - load_job_id    = job id (if job was created) or NULL
      - error_message  = error details
    """
    table_full_name = f"{bq_client.project}.{BQ_DATASET_ING}.{BQ_TABLE_ING}"

    query = f"""
      UPDATE `{table_full_name}`
      SET
        target_dataset = @target_dataset,
        target_table   = @target_table,
        load_job_id    = @load_job_id,
        status         = @status,
        finished_at    = @finished_at,
        error_message  = @error_message
      WHERE gcs_uri = @gcs_uri
        AND source_system = @source_system
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_dataset", "STRING", target_dataset),
            bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
            bigquery.ScalarQueryParameter("load_job_id", "STRING", load_job_id),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("finished_at", "TIMESTAMP", finished_at),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
            bigquery.ScalarQueryParameter("source_system", "STRING", SOURCE_SYSTEM),
        ]
    )

    job = bq_client.query(query, job_config=job_config)
    result = job.result()

    if getattr(result, "num_dml_affected_rows", None) == 0:
        print(
            f"[WARN] No ingestion_log row updated for gcs_uri={gcs_uri}, "
            f"file_type={file_type}, target={target_dataset}.{target_table}, status={status}"
        )


def _move_object_to_processed(bucket_name: str, object_name: str) -> str:
    """
    Move object from incoming prefix to processed prefix in the same bucket.

    Example:
      raw/incoming/payments/2025/11/15/file.csv.gz
      → raw/processed/payments/2025/11/15/file.csv.gz

    Returns:
        New GCS URI of the processed object.
    """
    if not object_name.startswith(INCOMING_PREFIX + "/"):
        raise ValueError(
            f"Object {object_name} is not under incoming prefix {INCOMING_PREFIX}/"
        )

    # suffix includes everything after INCOMING_PREFIX, e.g. "/payments/2025/11/15/file..."
    suffix = object_name[len(INCOMING_PREFIX):]
    new_object_name = PROCESSED_PREFIX + suffix  # keep same substructure under processed/

    bucket = storage_client.bucket(bucket_name)

    src_blob = bucket.blob(object_name)
    dst_blob = bucket.copy_blob(src_blob, bucket, new_object_name)

    # After copy succeeds, delete the original in incoming/
    src_blob.delete()

    new_gcs_uri = f"gs://{bucket_name}/{new_object_name}"
    print(f"[INFO] Moved object {object_name} → {new_object_name}")
    return new_gcs_uri


# ============================================
#   Cloud Function entry point (GCS trigger)
# ============================================

def cf_gcs_to_bq_raw(event, context):
    """
    GCS event-driven Cloud Function.

    Trigger:
      - Event: google.storage.object.finalize
      - Bucket: same as GCS_BUCKET_NAME for LifeFile landing.

    Steps:
      1) Filter out objects from other buckets or outside INCOMING_PREFIX.
      2) Infer file_type (payments/providers/...) from object path.
      3) Check ingestion_log if this gcs_uri has already been loaded (SUCCESS).
      4) If already loaded → skip.
      5) Otherwise:
           - run BQ LOAD job from GCS to raw_lifefile.<table>,
           - on SUCCESS:
                * update ingestion_log (status='SUCCESS', target_*, job id),
                * move object from raw/incoming/... to raw/processed/...,
           - on FAILURE:
                * update ingestion_log (status='FAILED', error_message, job id if any),
                * keep file in incoming for investigation / re-run.
    """
    bucket_name = event.get("bucket")
    object_name = event.get("name")

    print(f"[INFO] cf_gcs_to_bq_raw triggered for gs://{bucket_name}/{object_name}")

    # 1) Ensure this event comes from the expected bucket
    if bucket_name != GCS_BUCKET_NAME:
        print(f"[INFO] Event bucket {bucket_name} does not match configured {GCS_BUCKET_NAME} — skipping")
        return

    # 2) Ensure this object is under the incoming prefix
    if not object_name.startswith(INCOMING_PREFIX + "/"):
        print(f"[INFO] Object {object_name} is outside prefix {INCOMING_PREFIX}/ — skipping")
        return

    gcs_uri = f"gs://{bucket_name}/{object_name}"

    # 3) Infer file_type based on path
    file_type = _parse_file_type_from_object_name(object_name)
    if file_type is None:
        print(f"[WARN] Cannot infer file_type from object name {object_name} — skipping load")
        return

    raw_table_name = FILETYPE_TO_RAW_TABLE[file_type]
    raw_table_id = f"{bq_client.project}.{LIFEFILE_RAW_DATASET}.{raw_table_name}"

    # 4) Idempotency on GCS object level
    if _is_gcs_object_already_loaded(gcs_uri):
        print(f"[INFO] GCS object {gcs_uri} is already loaded into raw — skipping")
        return

    print(f"[INFO] Loading {gcs_uri} into {raw_table_id} for file_type={file_type}")

    started_at = dt.datetime.utcnow()
    load_job = None  # will be set if load_table_from_uri succeeds in creating a job

    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # assuming header row
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,      # for the exercise; in prod you'd define schema explicitly
            field_delimiter=",",
            quote_character='"',
            allow_quoted_newlines=True,
            compression=bigquery.Compression.GZIP,  # files are .csv.gz
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            raw_table_id,
            job_config=job_config,
        )

        print(f"[INFO] BigQuery load job started: job_id={load_job.job_id}")
        load_job.result()  # wait for completion

        finished_at = dt.datetime.utcnow()

        # 5a) On SUCCESS: update ingestion_log to SUCCESS
        _update_ingestion_log_after_load(
            gcs_uri=gcs_uri,
            file_type=file_type,
            target_dataset=LIFEFILE_RAW_DATASET,
            target_table=raw_table_name,
            load_job_id=load_job.job_id,
            finished_at=finished_at,
            status="SUCCESS",
            error_message=None,
        )

        # 5b) Move file from incoming/ to processed/ after successful load
        try:
            _move_object_to_processed(bucket_name=bucket_name, object_name=object_name)
        except Exception as move_err:
            # Loading to BQ was successful, but move to processed failed.
            # We log a warning but do NOT change SUCCESS status.
            print(f"[WARN] Failed to move object to processed for {gcs_uri}: {move_err!r}")

        print(f"[INFO] Successfully loaded {gcs_uri} into {raw_table_id}")

    except Exception as e:
        finished_at = dt.datetime.utcnow()
        load_job_id = load_job.job_id if load_job else None
        err_msg = f"BigQuery load failed for {gcs_uri}: {e!r}"
        print(f"[ERROR] {err_msg}")

        # On FAILURE: mark status='FAILED' in ingestion_log, keep file in incoming.
        _update_ingestion_log_after_load(
            gcs_uri=gcs_uri,
            file_type=file_type,
            target_dataset=LIFEFILE_RAW_DATASET,
            target_table=raw_table_name,
            load_job_id=load_job_id,
            finished_at=finished_at,
            status="FAILED",
            error_message=err_msg,
        )
