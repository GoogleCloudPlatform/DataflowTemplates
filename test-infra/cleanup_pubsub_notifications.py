import datetime
from google.cloud import storage
import argparse

# GCS buckets in which notifications will be deleted
# Structure - {projectId: {"buckets": list(<buckets>), "delete_threshold_in_days": <delete_threshold_in_days>}}
resources = {
  "cloud-teleport-testing": {
    "buckets": [
        "cloud-teleport-spanner-it-0",
        "cloud-teleport-spanner-it-1",
        "cloud-teleport-spanner-it-2",
        "cloud-teleport-spanner-it-3",
        "cloud-teleport-spanner-it-4",
        "cloud-teleport-spanner-it-5",
        "cloud-teleport-spanner-it-6",
        "cloud-teleport-spanner-it-7",
        "cloud-teleport-spanner-it-8",
        "cloud-teleport-spanner-it-9",
        "cloud-teleport-testing-it-gitactions"],
    "delete_threshold_in_days": 2
  },
  "span-cloud-teleport-testing": {
    "buckets": [
        "span-cloud-teleport-testing-it-0",
        "span-cloud-teleport-testing-it-1",
        "span-cloud-teleport-testing-it-2",
        "span-cloud-teleport-testing-it-3",
        "span-cloud-teleport-testing-it-4",
        "span-cloud-teleport-testing-it-5",
        "span-cloud-teleport-testing-it-6",
        "span-cloud-teleport-testing-it-7",
        "span-cloud-teleport-testing-it-8",
        "span-cloud-teleport-testing-it-9"],
    "delete_threshold_in_days": 2
  }
}

def delete_old_pubsub_notifications(project_id=None):
  """
  Iterates through projects and buckets, deleting Pub/Sub notifications
  older than a specified duration based on a date in the object prefix.
  """

  projects_to_process = dict()
  if project_id:
    if project_id in resources:
      projects_to_process = {project_id: resources[project_id]}
    else:
      print(f"Project {project_id} not found in resources configuration. Skipping.")
      return

  # Iterate through each project defined in the resources dictionary
  for project, config in projects_to_process.items():
    print(f"\n--- Checking project: {project} ---")

    # Create a new client specifically for the current project
    try:
      storage_client = storage.Client(project=project)
    except Exception as e:
      print(f"Error creating Storage client for project {project}: {e}")
      continue # Skip to the next project if client creation fails

    buckets = config.get("buckets", [])
    delete_threshold_in_days = config.get("delete_threshold_in_days")

    if not buckets or delete_threshold_in_days is None:
      print(f"Skipping project {project} due to missing 'buckets' or 'delete_threshold_in_days' configuration.")
      continue

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
    parser = argparse.ArgumentParser(description='Cleanup old Pub/Sub notifications.')
    parser.add_argument('--project', type=str, help='Project ID to cleanup')
    args = parser.parse_args()

    delete_old_pubsub_notifications(args.project)
