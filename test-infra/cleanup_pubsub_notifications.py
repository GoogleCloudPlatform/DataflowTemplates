import datetime
from google.cloud import storage

# Delete notifications older than or equal to these manty days
delete_threshold_in_days = 2


def delete_old_pubsub_notifications():
  storage_client = storage.Client()
  buckets = [f"cloud-teleport-spanner-it-{i}" for i in range(10)]
  buckets += ["cloud-teleport-testing-it-gitactions", "cloud-teleport-testing-it", "cloud-teleport-testing"]
  today = datetime.datetime.now(datetime.UTC).date()  # Get today's date in UTC

  for bucket_name in buckets:
    print("Cleaning up notifications for bucket", bucket_name)
    bucket = storage_client.bucket(bucket_name)
    notifications = list(bucket.list_notifications())

    if not notifications:
      print(f"Bucket '{bucket_name}' has no notifications.")
      continue
    for notification in notifications:
      # Extract object_name_prefix
      object_name_prefix = notification._properties.get("object_name_prefix", "")

      # Try to extract the date from the prefix (format: YYYYMMDD)
      date_str = extract_date_from_prefix(object_name_prefix)

      if date_str:
        notification_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()

        if today - notification_date >= datetime.timedelta(days=delete_threshold_in_days):
          print(f"Deleting old notification from {bucket_name}, created on {notification_date}")
          notification.delete()
        else:
          print(f"Skipping notification created today ({notification_date})")
      else:
        print(f"Skipping notification - No valid date found in prefix")


  return "Old Pub/Sub notifications deleted", 200


def extract_date_from_prefix(prefix):
  """Extracts YYYYMMDD date from object_name_prefix, if available."""
  import re
  match = re.search(r'(\d{8})', prefix)  # Look for a YYYYMMDD pattern
  return match.group(1) if match else None

if __name__ == "__main__":
    delete_old_pubsub_notifications()