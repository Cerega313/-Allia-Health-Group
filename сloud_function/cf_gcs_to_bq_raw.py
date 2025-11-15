# main_gcs_to_bq_raw.py
"""
Cloud Function: GCS landing → BigQuery raw (LifeFile).

High-level flow:
- Trigger: GCS "finalize" event for objects in the LifeFile landing bucket.
- For each new object (e.g. gs://lifefile-raw/raw/incoming/payments/...):
    * determine file_type from the GCS path (payments/providers/etc),
    * check ingestion_log to see if this GCS object was already loaded to raw,
    * if already loaded → skip,
    * if new:
        - run a BigQuery LOAD job from GCS into the appropriate raw table,
        - update ingestion_log with target_dataset/target_table/load_job_id/status.
"""

import datetime as dt
from typing import Optional

from google.cloud import bigquery
from google.cloud import storage

from config import CONFIG  # same config module as for cf_lifefile_sftp_to_gcs


# ==========================
#   Configuration bindings
# ==========================

GCS_BUCKET_NAME = CONFIG.gcs_bucket_name
GCS_INCOMING_PREFIX = CONFIG.gcs_incoming_prefix  # e.g. "raw/incoming"

BQ_PROJECT = CONFIG.bq_project
BQ_DATASET_ING = CONFIG.bq_dataset_ing   # e.g. "raw_technical"
BQ_TABLE_ING = CONFIG.bq_table_ing       # e.g. "ingestion_log"

SOURCE_SYSTEM = CONFIG.source_system     # "lifefile"

# In a real project you’d also move these to CONFIG,
# but for clarity we keep them here.
LIFEFILE_RAW_DATASET = "raw_lifefile"    # dataset where lifefile raw tables live

FILETYPE_TO_RAW_TABLE = {
    # file_type → raw table name
    "payments": "payments_raw",
    "providers": "providers_raw",
    # you can extend this mapping as needed
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
      <GCS_INCOMING_PREFIX>/<file_type>/YYYY/MM/DD/<filename>

    Example:
      raw/incoming/payments/2025/11/15/payments_2025-11-15T23-45-03Z_batch01.csv.gz
        → file_type = "payments"

    Returns:
        file_type if it can be inferred and is known in FILETYPE_TO_RAW_TABLE,
        otherwise None.
    """
    # Split path into segments, e.g. ["raw", "incoming", "payments", "2025", "11", "15", "file.csv.gz"]
    parts = object_name.split("/")

    # Split incoming prefix as array too, e.g. "raw/incoming" → ["raw", "incoming"]
    prefix_parts = GCS_INCOMING_PREFIX.split("/")

    # We expect at least: prefix + file_type + yyyy + mm + dd + filename
    if len(parts) < len(prefix_parts) + 2:
        return None

    # Ensure the object really starts with our incoming prefix
    if parts[:len(prefix_parts)] != prefix_parts:
        return None

    # file_type is the segment right after the prefix
    file_type = parts[len(prefix_parts)]

    # Only accept file types we know how to route
    if file_type in FILETYPE_TO_RAW_TABLE:
        return file_type

    return None


def _is_gcs_object_already_loaded(gcs_uri: str) -> bool:
    """
    Check if this GCS object has already been successfully loaded into raw tables.

    We use ingestion_log and consider the object "already loaded" if:
      - there exists a row with this gcs_uri AND
      - target_dataset and target_table are NOT NULL
      - and status = 'SUCCESS' (optional, but more explicit).

    This protects us from:
      - duplicate GCS "finalize" events,
      - Cloud Function retries,
      - manual re-triggers, etc.
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

    Typical patterns:
      - on success:
          status = 'SUCCESS',
          target_dataset = 'raw_lifefile',
          target_table = 'payments_raw' (etc),
          load_job_id = <job_id>,
          error_message = NULL

      - on failure:
          status remains 'UPLOADED' from the SFTP→GCS step (do NOT overwrite),
          but we can still update error_message and finished_at if needed.

    For simplicity here:
      - on success: set status='SUCCESS' + target_* + job id.
      - on failure: status is not changed by this helper; we only update error_message
                   via a separate query (so we don't break SFTP idempotency).
    """
    table_full_name = f"{bq_client.project}.{BQ_DATASET_ING}.{BQ_TABLE_ING}"

    # We want to update exactly those rows that match this gcs_uri + source_system
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
    result = job.result()  # wait for completion

    # For DML queries, num_dml_affected_rows tells us how many rows were updated
    if getattr(result, "num_dml_affected_rows", None) == 0:
        # No matching ingestion_log row found — log a warning.
        print(
            f"[WARN] No ingestion_log row found for gcs_uri={gcs_uri}, "
            f"file_type={file_type}, target={target_dataset}.{target_table}"
        )


def _update_ingestion_log_error_only(
    *,
    gcs_uri: str,
    finished_at: dt.datetime,
    error_message: str,
) -> None:
    """
    On load failure, we want to capture error_message and finished_at
    without changing 'status' (so it stays 'UPLOADED' from the landing step).

    This preserves SFTP→GCS semantics and allows us to re-run the loader
    if needed; idempotency for GCS-level is managed separately.
    """
    table_full_name = f"{bq_client.project}.{BQ_DATASET_ING}.{BQ_TABLE_ING}"

    query = f"""
      UPDATE `{table_full_name}`
      SET
        finished_at   = @finished_at,
        error_message = @error_message
      WHERE gcs_uri = @gcs_uri
        AND source_system = @source_system
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("finished_at", "TIMESTAMP", finished_at),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
            bigquery.ScalarQueryParameter("source_system", "STRING", SOURCE_SYSTEM),
        ]
    )

    job = bq_client.query(query, job_config=job_config)
    job.result()


# ============================================
#   Cloud Function entry point (GCS trigger)
# ============================================

def cf_gcs_to_bq_raw(event, context):
    """
    GCS event-driven Cloud Function.

    Trigger:
      - Event: google.storage.object.finalize
      - Bucket: same as GCS_BUCKET_NAME for LifeFile landing.

    Event payload (simplified):
      event = {
        "bucket": "<bucket-name>",
        "name": "<object name>",
        "timeCreated": "...",
        ...
      }

    Steps:
      1) Filter out objects from other buckets or outside GCS_INCOMING_PREFIX.
      2) Derive file_type (payments/providers/...) from object path.
      3) Check ingestion_log if this gcs_uri has already been loaded.
      4) If already loaded → skip.
      5) Otherwise:
         - run BQ LOAD job from GCS to raw_lifefile.<table>,
         - on success: update ingestion_log with status='SUCCESS', target_dataset/table, job id,
         - on failure: update ingestion_log error_message, leave status as is.
    """
    bucket_name = event.get("bucket")
    object_name = event.get("name")

    print(f"[INFO] cf_gcs_to_bq_raw triggered for gs://{bucket_name}/{object_name}")

    # 1) Ensure this event is for the expected bucket
    if bucket_name != GCS_BUCKET_NAME:
        print(f"[INFO] Event bucket {bucket_name} does not match configured {GCS_BUCKET_NAME} — skipping")
        return

    # 2) Ensure this object is under our incoming prefix (e.g. raw/incoming/)
    if not object_name.startswith(f"{GCS_INCOMING_PREFIX}/"):
        print(f"[INFO] Object {object_name} is outside prefix {GCS_INCOMING_PREFIX} — skipping")
        return

    gcs_uri = f"gs://{bucket_name}/{object_name}"

    # 3) Determine file_type from object path
    file_type = _parse_file_type_from_object_name(object_name)
    if file_type is None:
        # We don't know how to route this file — mark as skipped or just log.
        print(f"[WARN] Cannot infer file_type from object name {object_name} — skipping load")
        # Optionally: update ingestion_log with status='SKIPPED' here if you want.
        return

    raw_table_name = FILETYPE_TO_RAW_TABLE[file_type]
    raw_table_id = f"{bq_client.project}.{LIFEFILE_RAW_DATASET}.{raw_table_name}"

    # 4) Idempotency check on the GCS object itself
    if _is_gcs_object_already_loaded(gcs_uri):
        print(f"[INFO] GCS object {gcs_uri} is already loaded into raw — skipping")
        return

    print(f"[INFO] Loading {gcs_uri} into {raw_table_id} for file_type={file_type}")

    started_at = dt.datetime.utcnow()

    try:
        # Configure the BigQuery load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # assuming header row
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,      # for the exercise; in prod you likely define schema explicitly
            field_delimiter=",",
            quote_character='"',
            allow_quoted_newlines=True,
            compression=bigquery.Compression.GZIP,  # our files are .csv.gz
        )

        # Kick off the load job
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            raw_table_id,
            job_config=job_config,
        )

        print(f"[INFO] BigQuery load job started: job_id={load_job.job_id}")
        load_job.result()  # Wait for the job to complete

        finished_at = dt.datetime.utcnow()

        # Update ingestion_log to reflect successful raw load
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

        print(f"[INFO] Successfully loaded {gcs_uri} into {raw_table_id}")

    except Exception as e:
        finished_at = dt.datetime.utcnow()
        err_msg = f"BigQuery load failed for {gcs_uri}: {e!r}"
        print(f"[ERROR] {err_msg}")

        # On failure we do NOT override status='UPLOADED' from the SFTP→GCS step.
        # We only attach error_message and finished_at, so that:
        #   - we retain the fact that landing succeeded,
        #   - we can still re-run the loader if needed.
        _update_ingestion_log_error_only(
            gcs_uri=gcs_uri,
            finished_at=finished_at,
            error_message=err_msg,
        )
