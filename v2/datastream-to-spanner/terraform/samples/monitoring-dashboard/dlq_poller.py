import http.server
import json
import os
import time
import urllib.parse
import urllib.request

PROJECT_ID = os.environ.get("PROJECT_ID", "")
REGION = os.environ.get("REGION", "")
MIGRATION_ID = os.environ.get("MIGRATION_ID", "")
DLQ_DIRECTORIES = [
    d.strip() for d in os.environ.get("DLQ_DIRECTORIES", "").split(",") if d.strip()
]


def get_access_token():
  req = urllib.request.Request(
      "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
      headers={"Metadata-Flavor": "Google"},
  )
  with urllib.request.urlopen(req, timeout=10) as resp:
    return json.loads(resp.read().decode("utf-8"))["access_token"]


def list_gcs_objects(token, bucket, prefix):
  count = 0
  page_token = None
  while True:
    params = {"prefix": prefix, "fields": "items(name),nextPageToken"}
    if page_token:
      params["pageToken"] = page_token
    url = f"https://storage.googleapis.com/storage/v1/b/{urllib.parse.quote(bucket, safe='')}/o?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
      data = json.loads(resp.read().decode("utf-8"))
    for item in data.get("items", []):
      name = item.get("name", "")
      if name and not name.endswith("/"):
        count += 1
    page_token = data.get("nextPageToken")
    if not page_token:
      break
  return count


def list_cdc_dlq_objects(token, bucket):
  severe_count = 0
  retry_count = 0
  page_token = None
  while True:
    params = {"prefix": "cdc/", "fields": "items(name),nextPageToken"}
    if page_token:
      params["pageToken"] = page_token
    url = f"https://storage.googleapis.com/storage/v1/b/{urllib.parse.quote(bucket, safe='')}/o?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
      data = json.loads(resp.read().decode("utf-8"))
    for item in data.get("items", []):
      name = item.get("name", "")
      if not name or name.endswith("/"):
        continue
      if "/severe/" in name:
        severe_count += 1
      elif "/retry/" in name:
        retry_count += 1
    page_token = data.get("nextPageToken")
    if not page_token:
      break
  return severe_count, retry_count


def parse_gcs_uri(uri):
  if uri.startswith("gs://"):
    uri = uri[len("gs://") :]
  parts = uri.split("/", 1)
  bucket = parts[0]
  prefix = parts[1].strip("/") if len(parts) > 1 else ""
  return bucket, prefix


def poll_and_publish():
  token = get_access_token()
  bucket_counts = {}
  for dlq_uri in DLQ_DIRECTORIES:
    bucket, prefix = parse_gcs_uri(dlq_uri)
    if not bucket:
      continue
    severe_prefix = f"{prefix}/severe/" if prefix else "severe/"
    retry_prefix = f"{prefix}/retry/" if prefix else "retry/"
    severe_cnt = list_gcs_objects(token, bucket, severe_prefix)
    retry_cnt = list_gcs_objects(token, bucket, retry_prefix)
    if prefix != "cdc" and not prefix.startswith("cdc/"):
      cdc_severe, cdc_retry = list_cdc_dlq_objects(token, bucket)
      severe_cnt += cdc_severe
      retry_cnt += cdc_retry
    if bucket not in bucket_counts:
      bucket_counts[bucket] = {"severe": 0, "retry": 0}
    bucket_counts[bucket]["severe"] += severe_cnt
    bucket_counts[bucket]["retry"] += retry_cnt

  now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
  time_series = []
  for bucket, counts in bucket_counts.items():
    for category, count in counts.items():
      time_series.append({
          "metric": {
              "type": "custom.googleapis.com/migration/gcs_dlq_file_count",
              "labels": {
                  "migration_id": MIGRATION_ID,
                  "dlq_category": category,
              },
          },
          "resource": {
              "type": "gcs_bucket",
              "labels": {
                  "project_id": PROJECT_ID,
                  "bucket_name": bucket,
                  "location": REGION,
              },
          },
          "metricKind": "GAUGE",
          "valueType": "INT64",
          "points": [{
              "interval": {"endTime": now},
              "value": {"int64Value": str(count)},
          }],
      })

  if time_series:
    url = f"https://monitoring.googleapis.com/v3/projects/{PROJECT_ID}/timeSeries"
    req = urllib.request.Request(
        url,
        data=json.dumps({"timeSeries": time_series}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
      resp.read()

  return {"status": "ok", "migration_id": MIGRATION_ID, "buckets": bucket_counts}


class Handler(http.server.BaseHTTPRequestHandler):

  def do_GET(self):
    self._handle()

  def do_POST(self):
    self._handle()

  def _handle(self):
    try:
      result = poll_and_publish()
      body = json.dumps(result).encode("utf-8")
      self.send_response(200)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(body)))
      self.end_headers()
      self.wfile.write(body)
    except Exception as e:
      err = json.dumps({"status": "error", "message": str(e)}).encode("utf-8")
      self.send_response(500)
      self.send_header("Content-Type", "application/json")
      self.send_header("Content-Length", str(len(err)))
      self.end_headers()
      self.wfile.write(err)


if __name__ == "__main__":
  port = int(os.environ.get("PORT", "8080"))
  server = http.server.HTTPServer(("0.0.0.0", port), Handler)
  server.serve_forever()
