#
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

"""Cloud Function to poll GCS Dead-Letter Queue (DLQ) directories and publish counts to Cloud Monitoring.

This function is triggered periodically (e.g., every 1 minute via Cloud
Scheduler) during a live migration or reverse replication. It performs the
following steps:
  1. Reads and validates the target GCP project, migration ID, and one or more
     GCS DLQ base directories (e.g., "gs://my-bucket/dlq") from environment
     variables or the incoming HTTP JSON payload.
  2. Lists objects in the "severe/" (permanent errors) and "retry/" (transient
     errors) subdirectories of each configured DLQ path using bounded pagination
     and filtering out Apache Beam temporary staging files.
  3. Publishes the per-directory "severe" and "retry" file counts as a custom
     GAUGE metric (`custom.googleapis.com/migration/gcs_dlq_file_count`) to
     Cloud Monitoring so they can be evaluated by the Cutover/Cutback Readiness
     Scorecard and plotted per directory on the Monitoring Dashboard.
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import functions_framework
from google.cloud import monitoring_v3
from google.cloud import storage

# Custom Cloud Monitoring metric descriptor type used by the Cutover/Cutback
# Monitoring Dashboard.
METRIC_TYPE = "custom.googleapis.com/migration/gcs_dlq_file_count"

# The two subdirectories created by the Dataflow template under the configured
# deadLetterQueueDirectory:
# - "severe": permanent errors that require manual intervention and block cutover.
# - "retry": transient errors that are automatically re-ingested by Dataflow.
#   Note: When PubSubNotifiedDlqIO (dlqGcsPubSubSubscription) is enabled,
#   finalized files in "retry/" are reconsumed and deleted within seconds of
#   creation (after 1-minute windowing in "tmp_retry/"). Thus, the GCS "retry"
#   file count reflects any unconsumed retry backlog in GCS, while active retry
#   churn is tracked by the Dataflow "retryable_errors" counter on the dashboard.
DLQ_CATEGORIES = ("severe", "retry")

# Number of object names fetched per GCS list_blobs HTTP page.
GCS_LIST_PAGE_SIZE = 1000

# Default upper bound on the number of DLQ files counted per directory/category
# before short-circuiting pagination. Because Dataflow's DLQWriteTransform
# writes up to 20 shards per 1-minute window, a prolonged outage can generate
# tens of thousands of files. Capping the count prevents runaway GCS Class A
# list operations and Cloud Function timeouts while still signaling an active
# DLQ backlog on the dashboard.
DEFAULT_MAX_DLQ_COUNT_LIMIT = 10000

# Substrings that identify in-flight Apache Beam / Dataflow temporary files
# (matching FileBasedDeadLetterQueueReconsumer.java and PubSubNotifiedDlqIO.java)
# so transient staging objects are not counted as finalized DLQ files.
TEMP_FILE_MARKERS = (
    ".temp",
    "/tmp/",
    "/tmp_retry/",
    "/tmp_severe/",
    "/tmp_skip/",
)

# Cloud Monitoring API allows at most 200 time series per create_time_series call.
MAX_TIME_SERIES_PER_BATCH = 200

# Per-RPC timeout (in seconds) for GCS list_blobs and Cloud Monitoring
# create_time_series calls so a slow call cannot exceed the 60-second Cloud
# Scheduler cycle and cause overlapping invocations.
RPC_TIMEOUT_SECONDS = 30

# Global client instances cached across warm Cloud Function invocations to avoid
# re-initializing gRPC/HTTP channels on every 1-minute poll.
_storage_client = None
_metric_client = None


def _get_storage_client() -> storage.Client:
  """Returns a cached Cloud Storage client instance."""
  global _storage_client
  if _storage_client is None:
    _storage_client = storage.Client()
  return _storage_client


def _get_metric_client() -> monitoring_v3.MetricServiceClient:
  """Returns a cached Cloud Monitoring MetricServiceClient instance."""
  global _metric_client
  if _metric_client is None:
    _metric_client = monitoring_v3.MetricServiceClient()
  return _metric_client


def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
  """Parses and validates a GCS URI (e.g., 'gs://my-bucket/dlq/') into (bucket_name, prefix).

  Args:
    uri: The GCS URI string to parse. Must start with 'gs://' and contain a
      non-empty bucket name.

  Returns:
    A tuple of (bucket_name, normalized_prefix) with leading/trailing slashes
    stripped from the prefix.

  Raises:
    ValueError: If the URI does not start with 'gs://' or has an empty bucket
      name.
  """
  cleaned = uri.strip() if isinstance(uri, str) else ""
  if not cleaned.startswith("gs://"):
    raise ValueError(
        f"Invalid GCS URI '{uri}': must start with 'gs://' (e.g.,"
        " 'gs://my-bucket/dlq')."
    )
  path_without_scheme = cleaned[len("gs://") :]
  parts = path_without_scheme.split("/", 1)
  bucket_name = parts[0].strip()
  if not bucket_name:
    raise ValueError(
        f"Invalid GCS URI '{uri}': bucket name cannot be empty."
    )
  # Strip leading and trailing slashes so subfolder paths can be joined cleanly.
  prefix = parts[1].strip("/") if len(parts) > 1 else ""
  return bucket_name, prefix


def _is_valid_dlq_blob(
    blob_name: str,
    prefix: str = "",
    blob_size: Optional[int] = None,
) -> bool:
  """Returns True if the GCS object represents a non-empty finalized DLQ file.

  Excludes 0-byte objects, directory placeholder objects (ending in '/'), and
  Apache Beam temporary staging files (e.g., '.temp-beam-...', '/tmp/.temp',
  '/tmp_retry/'). Temporary file markers are checked against the path relative
  to `prefix` so that user-configured DLQ base paths containing '/tmp/' (such
  as Dataflow's default `<tempLocation>/dlq/`) do not cause valid DLQ files to
  be ignored.

  Args:
    blob_name: The full GCS object key name.
    prefix: The category prefix being listed (e.g., 'tmp/dlq/severe/').
    blob_size: Optional size of the GCS object in bytes.
  """
  if not blob_name or blob_name.endswith("/"):
    return False
  if blob_size is not None and blob_size <= 0:
    return False
  relative_name = (
      blob_name[len(prefix) :]
      if prefix and blob_name.startswith(prefix)
      else blob_name
  )
  if not relative_name or relative_name.endswith("/"):
    return False
  normalized_relative = f"/{relative_name.lstrip('/')}"
  for marker in TEMP_FILE_MARKERS:
    if marker in normalized_relative:
      return False
  return True


def _count_blobs(
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str,
    max_count_limit: int = DEFAULT_MAX_DLQ_COUNT_LIMIT,
) -> int:
  """Counts finalized DLQ objects under gs://<bucket_name>/<prefix> up to max_count_limit.

  Args:
    storage_client: Initialized google.cloud.storage.Client.
    bucket_name: Name of the GCS bucket to query.
    prefix: Object key prefix (e.g., 'dlq/severe/' or 'dlq/retry/').
    max_count_limit: Maximum number of DLQ files to count before stopping
      pagination. Must be a positive integer.

  Returns:
    Total number of finalized DLQ files under the prefix, capped at
    `max_count_limit`.
  """
  count = 0
  # Request only object names, sizes, and pagination tokens with an explicit
  # page_size of 1000 to minimize response payload size and HTTP round-trips.
  blobs = storage_client.list_blobs(
      bucket_name,
      prefix=prefix,
      page_size=GCS_LIST_PAGE_SIZE,
      fields="items(name,size),nextPageToken",
      timeout=RPC_TIMEOUT_SECONDS,
  )
  for blob in blobs:
    if _is_valid_dlq_blob(
        blob.name,
        prefix=prefix,
        blob_size=getattr(blob, "size", None),
    ):
      count += 1
      # Short-circuit pagination once the cap is reached so the Cloud Function
      # cannot time out or incur excessive GCS Class A list costs during a
      # large DLQ backlog.
      if count >= max_count_limit:
        return max_count_limit
  return count


def _parse_dlq_directories(raw_dirs) -> List[str]:
  """Normalizes DLQ directories from a comma-separated string or JSON list.

  Supports both single-pipeline setups (a single URI) and sharded migrations
  where multiple DLQ directories may be passed as a comma-separated string or
  JSON array.

  Args:
    raw_dirs: A comma-separated string or list of GCS URIs.

  Returns:
    A list of non-empty, whitespace-trimmed GCS URI strings.
  """
  if isinstance(raw_dirs, list):
    return [str(d).strip() for d in raw_dirs if str(d).strip()]
  if isinstance(raw_dirs, str):
    return [d.strip() for d in raw_dirs.split(",") if d.strip()]
  return []


def _parse_max_count_limit(raw_limit) -> int:
  """Parses and validates the maximum DLQ file count limit.

  Args:
    raw_limit: Raw limit value from JSON request or environment variable, or
      None/empty string to use `DEFAULT_MAX_DLQ_COUNT_LIMIT`.

  Returns:
    Validated positive integer limit.

  Raises:
    ValueError: If `raw_limit` is not a positive integer.
  """
  if raw_limit is None or str(raw_limit).strip() == "":
    return DEFAULT_MAX_DLQ_COUNT_LIMIT
  if isinstance(raw_limit, bool):
    raise ValueError(
        f"Invalid max_dlq_count_limit '{raw_limit}': must be a positive integer."
    )
  try:
    limit = int(str(raw_limit).strip())
  except ValueError as exc:
    raise ValueError(
        f"Invalid max_dlq_count_limit '{raw_limit}': must be a positive integer."
    ) from exc
  if limit <= 0:
    raise ValueError(
        f"Invalid max_dlq_count_limit '{raw_limit}': must be greater than 0."
    )
  return limit


def _publish_dlq_metrics(
    metric_client: monitoring_v3.MetricServiceClient,
    project_id: str,
    migration_id: str,
    directory_counts: Dict[str, Dict[str, int]],
) -> None:
  """Writes per-directory severe and retry DLQ file counts to Cloud Monitoring.

  Each `(dlq_directory, dlq_category)` pair is written as a distinct GAUGE time
  series under `custom.googleapis.com/migration/gcs_dlq_file_count` using the
  `global` monitored resource type, tagged with `migration_id`, `dlq_category`,
  and `dlq_directory`. Tagging each series with `dlq_directory` provides
  per-shard visibility on the dashboard's XY chart and prevents time-series
  write collisions across shards.

  Args:
    metric_client: Initialized Cloud Monitoring MetricServiceClient.
    project_id: Target GCP project ID where metrics are published.
    migration_id: Unique migration identifier used to filter dashboard queries.
    directory_counts: Nested dictionary mapping each normalized `dlq_directory`
      URI to a dictionary of `{dlq_category: file_count}`.
  """
  now = time.time()
  seconds = int(now)
  nanos = int((now - seconds) * 10**9)
  interval = monitoring_v3.TimeInterval(
      {"end_time": {"seconds": seconds, "nanos": nanos}}
  )

  time_series_list = []
  for dlq_uri, category_map in directory_counts.items():
    for category, count in category_map.items():
      series = monitoring_v3.TimeSeries()
      series.metric.type = METRIC_TYPE
      # Attach metric labels used by the dashboard's PromQL scorecard and XY chart.
      series.metric.labels["migration_id"] = migration_id
      series.metric.labels["dlq_category"] = category
      series.metric.labels["dlq_directory"] = dlq_uri

      # Custom metrics in Cloud Monitoring use the "global" monitored resource.
      series.resource.type = "global"
      series.resource.labels["project_id"] = project_id

      series.metric_kind = monitoring_v3.MetricDescriptor.MetricKind.GAUGE
      series.value_type = monitoring_v3.MetricDescriptor.ValueType.INT64

      point = monitoring_v3.Point(
          {"interval": interval, "value": {"int64_value": count}}
      )
      series.points.append(point)
      time_series_list.append(series)

  # Batch time series writes (up to 200 series per API request).
  for i in range(0, len(time_series_list), MAX_TIME_SERIES_PER_BATCH):
    batch = time_series_list[i : i + MAX_TIME_SERIES_PER_BATCH]
    metric_client.create_time_series(
        name=f"projects/{project_id}",
        time_series=batch,
        timeout=RPC_TIMEOUT_SECONDS,
    )


@functions_framework.http
def poll_gcs_dlq(request):
  """HTTP Cloud Function entrypoint triggered by Cloud Scheduler.

  Configuration can be supplied via environment variables (set at deployment
  time) or overridden per request via a JSON body:
    - PROJECT_ID / project_id: GCP project ID.
    - MIGRATION_ID / migration_id: Unique migration identifier.
    - DLQ_DIRECTORIES / dlq_directories: Comma-separated string (or JSON list)
      of GCS DLQ root URIs (e.g., "gs://my-bucket/dlq").
    - MAX_DLQ_COUNT_LIMIT / max_dlq_count_limit: Optional positive integer cap
      on the number of files counted per directory/category (default: 10000).

  Note: A single poller instance (or non-overlapping `dlq_directories` per
  poller) should be used for a given `(project_id, migration_id, dlq_directory)`
  tuple so that GAUGE points are not written more than once per minute to the
  same time series.

  Args:
    request: Flask Request object provided by Functions Framework.

  Returns:
    A tuple of (json_response_body, http_status_code, headers).
  """
  # Allow optional per-request JSON overrides in addition to environment vars.
  try:
    req_json = request.get_json(silent=True)
  except Exception:
    req_json = {}
  if not isinstance(req_json, dict):
    req_json = {}

  # Resolve project ID, migration ID, and DLQ directory list.
  raw_project_id = (
      req_json.get("project_id")
      or os.environ.get("PROJECT_ID")
      or os.environ.get("GCP_PROJECT")
      or os.environ.get("GOOGLE_CLOUD_PROJECT")
      or ""
  )
  project_id = str(raw_project_id).strip()
  raw_migration_id = (
      req_json.get("migration_id") or os.environ.get("MIGRATION_ID") or ""
  )
  migration_id = str(raw_migration_id).strip()
  dlq_directories = _parse_dlq_directories(
      req_json.get("dlq_directories") or os.environ.get("DLQ_DIRECTORIES", "")
  )

  # Validate that all required inputs are present before querying GCS.
  if not project_id or not migration_id or not dlq_directories:
    error_body = {
        "status": "error",
        "message": (
            "Missing required configuration. Provide PROJECT_ID, MIGRATION_ID,"
            " and DLQ_DIRECTORIES via environment variables or JSON body."
        ),
    }
    return (
        json.dumps(error_body),
        400,
        {"Content-Type": "application/json"},
    )

  # Fail-closed validation: validate max_dlq_count_limit and all GCS URIs
  # upfront before querying GCS or publishing any metrics. If any URI is
  # malformed (e.g., "gs://"), return HTTP 400 rather than silently skipping it
  # and publishing a false 0 count. Deduplicate normalized URIs so equivalent
  # entries (e.g., "gs://b/dlq" and "gs://b/dlq/") are not double-counted.
  try:
    raw_limit = (
        req_json.get("max_dlq_count_limit")
        if "max_dlq_count_limit" in req_json
        else os.environ.get("MAX_DLQ_COUNT_LIMIT")
    )
    max_count_limit = _parse_max_count_limit(raw_limit)
    parsed_directories: List[Tuple[str, str, str]] = []
    seen_uris = set()
    for dlq_uri in dlq_directories:
      bucket_name, prefix = _parse_gcs_uri(dlq_uri)
      normalized_uri = (
          f"gs://{bucket_name}/{prefix}" if prefix else f"gs://{bucket_name}"
      )
      if normalized_uri not in seen_uris:
        seen_uris.add(normalized_uri)
        parsed_directories.append((normalized_uri, bucket_name, prefix))
  except ValueError as exc:
    error_body = {
        "status": "error",
        "message": str(exc),
    }
    return (
        json.dumps(error_body),
        400,
        {"Content-Type": "application/json"},
    )

  try:
    storage_client = _get_storage_client()
    metric_client = _get_metric_client()

    # Track both aggregate counts across all directories and per-directory
    # breakdowns. Single-threaded execution is used to keep resource footprint
    # minimal inside the Cloud Function.
    totals: Dict[str, int] = {category: 0 for category in DLQ_CATEGORIES}
    directory_counts: Dict[str, Dict[str, int]] = {}

    for normalized_uri, bucket_name, prefix in parsed_directories:
      per_dir: Dict[str, int] = {}
      for category in DLQ_CATEGORIES:
        # Construct the category prefix (e.g., 'dlq/severe/' or 'dlq/retry/').
        category_prefix = f"{prefix}/{category}/" if prefix else f"{category}/"
        cnt = _count_blobs(
            storage_client,
            bucket_name,
            category_prefix,
            max_count_limit=max_count_limit,
        )
        per_dir[category] = cnt
        totals[category] += cnt

      directory_counts[normalized_uri] = per_dir

    # Publish the per-directory severe and retry counts to Cloud Monitoring.
    _publish_dlq_metrics(
        metric_client, project_id, migration_id, directory_counts
    )

    response_body = {
        "status": "ok",
        "project_id": project_id,
        "migration_id": migration_id,
        "max_dlq_count_limit": max_count_limit,
        "totals": totals,
        "directories": directory_counts,
    }
    return (
        json.dumps(response_body),
        200,
        {"Content-Type": "application/json"},
    )
  except Exception as exc:
    logging.exception("Failed to poll GCS DLQ or publish metrics")
    error_body = {
        "status": "error",
        "message": str(exc),
    }
    return (
        json.dumps(error_body),
        500,
        {"Content-Type": "application/json"},
    )


# Alias `main` to `poll_gcs_dlq` so the function works whether the Cloud
# Function entrypoint is configured as `poll_gcs_dlq` or `main`.
main = poll_gcs_dlq
