# Common Parameters
common_params = {
  project = "<YOUR_PROJECT_ID>"
  # Replace with your GCP project ID
  host_project = "<YOUR_HOST_PROJECT_ID>"
  # If you are using a shared VPC
  region = "<YOUR_REGION>"
  # Replace with your desired GCP region
}

# Datastream Parameters
datastream_params = {
  source_connection_profile_id = "<YOUR_SOURCE_CONNECTION_PROFILE_ID>"
  # ID of the MySQL source connection profile
  target_connection_profile_id = "<YOUR_TARGET_CONNECTION_PROFILE_ID>"
  # ID of the GCS target connection profile
  target_gcs_bucket_name = "<YOUR_TARGET_GCS_BUCKET_NAME>"
  # Name of the target GCS bucket used in the target connection profile above.
  mysql_databases = [
    {
      database = "<YOUR_DATABASE_NAME>"
      tables   = []
      # Optionally list specific tables, or remove "tables" all together for all tables
    }
  ]
}

# Dataflow Parameters
dataflow_params = {
  template_params = {
    spanner_database_id = "<YOUR_SPANNER_DATABASE_ID>"
    # ID of the target Cloud Spanner database
    spanner_instance_id = "<YOUR_SPANNER_INSTANCE_ID>"
    # ID of the target Cloud Spanner instance
  }
  runner_params = {
    max_workers = 10       # Adjust based on your requirements
    num_workers = 4        # Adjust based on your requirements
    on_delete   = "cancel" # Or "drain"
    network     = "<YOUR_VPC_NETWORK>"
    subnetwork  = "<YOUR_SUBNETWORK_NAME>"
    # subnetwork is passed "as-is". This is intentionally kept like so to
    # allow for shared VPC configurations. Learn more about subnetwork
    # configuration at: https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter
    ip_configuration = "WORKER_IP_PRIVATE"
    # Keep this WORKER_IP_PRIVATE to disable public IPs for Dataflow workers.
    # This will require enabling private google access for the subnetwork being
    # used. Otherwise remove this configuration to enable public IPs.
  }
}
