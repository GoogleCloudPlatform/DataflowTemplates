import datetime
from google.cloud import datastream_v1
from google.api_core import exceptions
from google.api_core import retry
import argparse

# Datastream locations to clean up
# Structure - {projectId: [locations]}
resources = {
    "cloud-teleport-testing": ["us-central1"],
    "span-cloud-teleport-testing": ["us-central1"]
}

# Retry policy configuration
def is_retryable(exception):
    return isinstance(exception, (
        exceptions.ServiceUnavailable,
        exceptions.DeadlineExceeded,
        exceptions.ResourceExhausted,
        exceptions.Aborted,
        exceptions.InternalServerError,
        exceptions.TooManyRequests
    ))

RETRY_POLICY = retry.Retry(predicate=is_retryable, initial=1.0, maximum=60.0, multiplier=1.3, deadline=900.0)
LRO_TIMEOUT = 1800 # 30 minutes

def delete_streams(client, parent, look_back_period=7):
    """Deletes Datastream streams older than look_back_period days and returns their connection profiles."""
    profiles_to_delete = set()
    try:
        streams = client.list_streams(parent=parent)
        for stream in streams:
            create_time = stream.create_time
            if create_time:
                age = datetime.datetime.now(datetime.timezone.utc) - create_time
                if age > datetime.timedelta(days=look_back_period):
                    print(f"Deleting stream {stream.name} (age: {age})")
                    
                    # Collect connection profiles before deleting the stream
                    if stream.source_config.source_connection_profile:
                        profiles_to_delete.add(stream.source_config.source_connection_profile)
                    if stream.destination_config.destination_connection_profile:
                        profiles_to_delete.add(stream.destination_config.destination_connection_profile)
                        
                    try:
                        operation = client.delete_stream(name=stream.name, retry=RETRY_POLICY)
                        operation.result(timeout=LRO_TIMEOUT) # Wait for completion
                        print(f"Successfully deleted stream {stream.name}")
                    except exceptions.NotFound:
                        print(f"Stream {stream.name} already deleted.")
                    except Exception as e:
                        print(f"Error deleting stream {stream.name}: {e}")
                else:
                    print(f"Skipping stream {stream.name} (age: {age})")
    except Exception as e:
        print(f"Error listing streams in {parent}: {e}")
    
    return profiles_to_delete

def find_old_connection_profiles(client, parent, look_back_period=7):
    """Finds Datastream connection profiles older than look_back_period days."""
    old_profiles = set()
    try:
        profiles = client.list_connection_profiles(parent=parent)
        for profile in profiles:
            create_time = profile.create_time
            if create_time:
                age = datetime.datetime.now(datetime.timezone.utc) - create_time
                if age > datetime.timedelta(days=look_back_period):
                    old_profiles.add(profile.name)
    except Exception as e:
        print(f"Error listing connection profiles in {parent}: {e}")
    return old_profiles

def delete_connection_profiles(client, profiles):
    """Deletes specified Datastream connection profiles."""
    for profile_name in profiles:
        print(f"Attempting to delete connection profile {profile_name}")
        try:
            operation = client.delete_connection_profile(name=profile_name, retry=RETRY_POLICY)
            operation.result(timeout=LRO_TIMEOUT)
            print(f"Successfully deleted connection profile {profile_name}")
        except exceptions.NotFound:
            print(f"Connection profile {profile_name} already deleted.")
        except exceptions.FailedPrecondition as e:
            print(f"Skipping connection profile {profile_name} as it is in use: {e}")
        except Exception as e:
            # Check for "in use" message if exception type isn't exactly FailedPrecondition or if wrapped
            if "is being used" in str(e):
                print(f"Skipping connection profile {profile_name} as it is in use.")
            else:
                print(f"Error deleting connection profile {profile_name}: {e}")

def delete_old_datastream_resources(project_id=None, look_back_period=7):
    projects_to_process = resources
    if project_id:
        if project_id in resources:
            projects_to_process = {project_id: resources[project_id]}
        else:
            print(f"Project {project_id} not found in resources configuration. Skipping.")
            return

    for project, locations in projects_to_process.items():
        print(f"\n--- Checking project: {project} ---")
        
        try:
            client = datastream_v1.DatastreamClient()
        except Exception as e:
            print(f"Error creating Datastream client: {e}")
            continue

        for location in locations:
            parent = f"projects/{project}/locations/{location}"
            print(f"Checking location: {location}")

            profiles_from_streams = delete_streams(client, parent, look_back_period)
            old_profiles = find_old_connection_profiles(client, parent, look_back_period)
            
            # Combine sets
            all_profiles_to_delete = profiles_from_streams.union(old_profiles)
            
            if all_profiles_to_delete:
                delete_connection_profiles(client, all_profiles_to_delete)
            else:
                print("No connection profiles to delete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cleanup old Datastream resources.')
    parser.add_argument('--project', type=str, help='Project ID to cleanup')
    parser.add_argument('--look_back_period', type=int, default=7, help='Look back period in days')
    args = parser.parse_args()
    
    delete_old_datastream_resources(args.project, args.look_back_period)
