# main.py
"""
Cloud Function: LifeFile SFTP → GCS landing.

High-level flow:
- Triggered by Cloud Scheduler via HTTP.
- Connects to LifeFile SFTP.
- Finds matching files (payments, providers, etc.).
- Checks ingestion_log to see if a given SFTP file was already ingested.
- If not:
    * downloads the file from SFTP,
    * gzips contents,
    * uploads to GCS landing (raw/incoming/...),
    * writes a row to raw_technical.ingestion_log.
- If yes:
    * skips the file (idempotent behavior).
"""

import gzip
import fnmatch
import datetime as dt
from typing import List, Tuple, Optional

import paramiko
from google.cloud import storage
from google.cloud import bigquery

from config import CONFIG  # central configuration (env-based) for SFTP/GCS/BQ


# ==========================
#   Configuration bindings
# ==========================

# Pull config values from CONFIG (instead of reading os.environ directly)
SFTP_HOST = CONFIG.sftp_host
SFTP_PORT = CONFIG.sftp_port
SFTP_USERNAME = CONFIG.sftp_username
SFTP_PASSWORD = CONFIG.sftp_password
SFTP_BASE_DIR = CONFIG.sftp_base_dir
SFTP_FILE_PATTERNS = CONFIG.sftp_file_patterns

GCS_BUCKET_NAME = CONFIG.gcs_bucket_name
GCS_INCOMING_PREFIX = CONFIG.gcs_incoming_prefix

BQ_PROJECT = CONFIG.bq_project
BQ_DATASET_ING = CONFIG.bq_dataset_ing
BQ_TABLE_ING = CONFIG.bq_table_ing

SOURCE_SYSTEM = CONFIG.source_system

# Global clients (Cloud Functions will reuse these across invocations)
storage_client = storage.Client()
bq_client = bigquery.Client(project=BQ_PROJECT) if BQ_PROJECT else bigquery.Client()


# =====================================
#   Helper functions
# =====================================

def _connect_sftp() -> paramiko.SFTPClient:
    """
    Open an SFTP connection to LifeFile using Paramiko and return an SFTPClient.
    """
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


def _list_matching_files(sftp: paramiko.SFTPClient) -> List[Tuple[str, str]]:
    """
    List files in SFTP_BASE_DIR that match any pattern in SFTP_FILE_PATTERNS.

    Returns:
        List of (file_type, remote_path), where:
          - file_type is derived from the filename prefix (e.g. 'payments'),
          - remote_path is the full SFTP path (e.g. '/outgoing/payments_2025-11-15.csv').
    """
    files = sftp.listdir(SFTP_BASE_DIR)
    result: List[Tuple[str, str]] = []

    for name in files:
        full_path = f"{SFTP_BASE_DIR}/{name}"

        for pattern in SFTP_FILE_PATTERNS:
            pattern = pattern.strip()
            if not pattern:
                continue

            # fnmatch supports glob-like patterns (e.g. 'payments*.csv')
            if fnmatch.fnmatch(name, pattern):
                # File type: part before the first "_" and lowercased
                base = name.split(".")[0]
                file_type = base.split("_")[0].lower()
                result.append((file_type, full_path))
                break

    return result


def _generate_new_filename(file_type: str, batch_idx: int) -> Tuple[str, dt.date]:
    """
    Generate a new GCS object name and a logical file date.

    Name format:
        <file_type>_YYYY-MM-DDTHH-MM-SSZ_batchNN.csv.gz

    Args:
        file_type:  'payments', 'providers', etc.
        batch_idx:  1-based batch index within a single function execution.

    Returns:
        (filename, file_date)
    """
    now_utc = dt.datetime.utcnow().replace(microsecond=0)
    ts_str = now_utc.isoformat().replace(":", "-") + "Z"
    batch_str = f"batch{batch_idx:02d}"
    new_name = f"{file_type}_{ts_str}_{batch_str}.csv.gz"
    file_date = now_utc.date()
    return new_name, file_date


def _upload_to_gcs(
    data: bytes,
    file_type: str,
    new_filename: str,
    file_date: dt.date,
) -> storage.Blob:
    """
    Upload gzipped bytes to GCS under:
        gs://<bucket>/<GCS_INCOMING_PREFIX>/<file_type>/YYYY/MM/DD/<new_filename>

    Returns:
        storage.Blob for the created object.
    """
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    object_path = f"{GCS_INCOMING_PREFIX}/{file_type}/{file_date:%Y/%m/%d}/{new_filename}"
    blob = bucket.blob(object_path)
    blob.upload_from_string(data, content_type="application/gzip")
    return blob


def _is_source_file_already_uploaded(source_path: str) -> bool:
    """
    Check if a given SFTP file (source_path) has already been ingested.

    We look into raw_technical.ingestion_log and consider a file "already ingested" if:
      - source_system = 'lifefile'
      - source_path   = given SFTP path (e.g. '/outgoing/payments_2025-11-15.csv')
      - status IN ('UPLOADED', 'SUCCESS')

    Returns:
        True  if at least one matching row exists;
        False otherwise.
    """
    table_full_name = f"{bq_client.project}.{BQ_DATASET_ING}.{BQ_TABLE_ING}"

    query = f"""
      SELECT 1
      FROM `{table_full_name}`
      WHERE source_system = @source_system
        AND source_path   = @source_path
        AND status IN ('UPLOADED', 'SUCCESS')
      LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_system", "STRING", SOURCE_SYSTEM),
            bigquery.ScalarQueryParameter("source_path", "STRING", source_path),
        ]
    )

    result = bq_client.query(query, job_config=job_config).result()

    for _ in result:
        return True

    return False


def _insert_ingestion_log_row(
    *,
    file_type: str,
    file_date: Optional[dt.date],
    source_path: Optional[str],
    blob: Optional[storage.Blob],
    started_at: dt.datetime,
    finished_at: Optional[dt.datetime],
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Insert a single row into raw_technical.ingestion_log.

    At this stage, we only know about the landing in GCS (or failure).
    The raw load into BigQuery will be logged later by a separate function.
    """
    table_id = f"{bq_client.project}.{BQ_DATASET_ING}.{BQ_TABLE_ING}"

    if blob is not None:
        gcs_uri = f"gs://{blob.bucket.name}/{blob.name}"
        gcs_generation = int(blob.generation) if blob.generation is not None else None
        gcs_md5 = blob.md5_hash
        gcs_size_bytes = blob.size
    else:
        gcs_uri = None
        gcs_generation = None
        gcs_md5 = None
        gcs_size_bytes = None

    row = {
        "file_type": file_type,
        "file_date": file_date.isoformat() if file_date else None,
        "source_system": SOURCE_SYSTEM,
        "source_path": source_path,
        "gcs_uri": gcs_uri,
        "gcs_generation": gcs_generation,
        "gcs_md5": gcs_md5,
        "gcs_size_bytes": gcs_size_bytes,
        "target_dataset": None,
        "target_table": None,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat() if finished_at else None,
        "status": status,           # 'UPLOADED' or 'FAILED' at this stage
        "load_job_id": None,        # filled later by GCS→BQ loader
        "error_message": error_message,
    }

    errors = bq_client.insert_rows_json(table_id, [row])
    if errors:
        # Logging failure to insert into ingestion_log itself; we do not raise.
        print(f"[WARN] Failed to insert ingestion_log row: {errors}")


def _process_single_file(
    sftp: paramiko.SFTPClient,
    file_type: str,
    remote_path: str,
    batch_idx: int,
) -> None:
    """
    Process a single SFTP file:
      - download from SFTP,
      - gzip contents,
      - upload to GCS,
      - log result to ingestion_log with status 'UPLOADED' or 'FAILED'.
    """
    print(f"[INFO] Start processing remote file {remote_path} as type={file_type}, batch={batch_idx}")
    started_at = dt.datetime.utcnow()

    blob: Optional[storage.Blob] = None
    file_date: Optional[dt.date] = None

    try:
        # Download file from SFTP as bytes
        with sftp.open(remote_path, "rb") as f:
            raw_data = f.read()

        # Gzip the content
        gz_data = gzip.compress(raw_data)

        # Generate new GCS filename and logical file date
        new_name, file_date = _generate_new_filename(file_type, batch_idx=batch_idx)

        # Upload to GCS landing
        blob = _upload_to_gcs(
            data=gz_data,
            file_type=file_type,
            new_filename=new_name,
            file_date=file_date,
        )

        finished_at = dt.datetime.utcnow()

        # Log successful landing
        _insert_ingestion_log_row(
            file_type=file_type,
            file_date=file_date,
            source_path=remote_path,
            blob=blob,
            started_at=started_at,
            finished_at=finished_at,
            status="UPLOADED",
            error_message=None,
        )

        print(f"[INFO] Successfully uploaded {remote_path} → gs://{blob.bucket.name}/{blob.name}")

    except Exception as e:
        finished_at = dt.datetime.utcnow()
        print(f"[ERROR] Failed to process {remote_path}: {e!r}")

        # Log failure (blob/file_date may be None depending on where the error happened)
        _insert_ingestion_log_row(
            file_type=file_type,
            file_date=file_date,
            source_path=remote_path,
            blob=blob,
            started_at=started_at,
            finished_at=finished_at,
            status="FAILED",
            error_message=str(e),
        )


# ============================================
#   Cloud Function entry point (HTTP trigger)
# ============================================

def cf_lifefile_sftp_to_gcs(request):
    """
    HTTP entry point.

    Expected trigger:
      - Cloud Scheduler → HTTP → this function (e.g. every day at 23:45 UTC).

    Steps:
      1) Validate required configuration (basic sanity check).
      2) Connect to LifeFile SFTP.
      3) List files matching SFTP_FILE_PATTERNS in SFTP_BASE_DIR.
      4) For each matching file:
           - check ingestion_log to see if the SFTP path is already ingested;
           - if yes → skip (idempotent behavior);
           - if no  → process (download → gzip → GCS → ingestion_log).
      5) Return HTTP 200 or 500 depending on connectivity/config errors.
    """
    print("[INFO] cf_lifefile_sftp_to_gcs started")

    # Minimal runtime sanity check (CONFIG already enforces required envs, but this is extra safety).
    required_env = {
        "SFTP_HOST": SFTP_HOST,
        "SFTP_USERNAME": SFTP_USERNAME,
        "SFTP_PASSWORD": SFTP_PASSWORD,
        "GCS_BUCKET_NAME": GCS_BUCKET_NAME,
    }
    missing = [name for name, value in required_env.items() if not value]
    if missing:
        msg = f"Missing required configuration values: {', '.join(missing)}"
        print("[ERROR]", msg)
        return (msg, 500)

    # Connect to SFTP
    try:
        sftp = _connect_sftp()
        print(f"[INFO] Connected to SFTP {SFTP_HOST}:{SFTP_PORT} as {SFTP_USERNAME}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to SFTP: {e!r}")
        return ("Failed to connect to SFTP", 500)

    try:
        # Discover candidate files
        files = _list_matching_files(sftp)
        print(f"[INFO] Found {len(files)} matching files on SFTP under {SFTP_BASE_DIR}")

        if not files:
            return ("No matching files found", 200)

        batch_idx = 0

        for file_type, remote_path in files:
            # Idempotency check based on SFTP source_path
            if _is_source_file_already_uploaded(remote_path):
                print(f"[INFO] Skip already uploaded file: {remote_path}")
                continue

            batch_idx += 1
            _process_single_file(
                sftp=sftp,
                file_type=file_type,
                remote_path=remote_path,
                batch_idx=batch_idx,
            )

        if batch_idx == 0:
            print("[INFO] All matching files already ingested previously")
            return ("No new files to ingest", 200)

    finally:
        # Always try to close SFTP connection
        try:
            sftp.close()
        except Exception:
            pass

    print("[INFO] cf_lifefile_sftp_to_gcs finished")
    return ("OK", 200)
