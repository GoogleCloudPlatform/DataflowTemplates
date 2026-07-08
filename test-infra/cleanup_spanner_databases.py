import datetime
from google.cloud import spanner
import argparse

# Spanner instances in which databases will be deleted
# Structure - {projectId: list(instanceIds)}
resources = {
    "cloud-teleport-testing": ["teleport", "teleport1", "teleport2", "teleport3", "teleport4"],
    "span-cloud-teleport-testing": ["teleport", "teleport1", "teleport2", "teleport3", "teleport4"]
}

def delete_old_spanner_databases(project_id=None):
  projects_to_process = dict()
  if project_id:
    if project_id in resources:
      projects_to_process = {project_id: resources[project_id]}
    else:
      print(f"Project {project_id} not found in resources configuration. Skipping.")
      return

  for project in projects_to_process:
    print(f"\n--- Checking project: {project} ---")

    try:
      spanner_client = spanner.Client(project=project)
    except Exception as e:
      print(f"Error creating Spanner client for project {project}: {e}")
      continue # Skip to the next project if client creation fails

    instance_ids = projects_to_process[project]

    for instance_id in instance_ids:
      instance = spanner_client.instance(instance_id)
      for database in instance.list_databases():
        create_time = database.create_time
        if create_time:
          time_difference = datetime.datetime.now(datetime.UTC) - create_time
          if time_difference > datetime.timedelta(hours=20):
            print(f"Deleting database {database.name} in instance {instance_id}")
            try:
              database_id = database.name.split('/')[-1]
              instance.database(database_id).drop()
              print(f"Successfully deleted {database.name}")
            except Exception as e:
              print(f"Error deleting database {database.name}: {e}")
          else:
            print(f"Skipping database {database.name} in instance {instance_id} as it is not older than 5 hours")
        else:
          print(f"Could not determine create time for database {database.name}, skipping.")
  return "Completed database cleanup", 200


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Cleanup old Spanner databases.')
  parser.add_argument('--project', type=str, help='Project ID to cleanup')
  args = parser.parse_args()
  
  delete_old_spanner_databases(args.project)