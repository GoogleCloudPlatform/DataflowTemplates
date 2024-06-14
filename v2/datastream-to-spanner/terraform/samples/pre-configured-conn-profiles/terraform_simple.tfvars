# Common Parameters
common_params = {
  project               = "<YOUR_PROJECT_ID>"
  # Replace with your GCP project ID
  region                = "<YOUR_REGION>"
  # Replace with your desired GCP region
  service_account_email = "<YOUR_SERVICE_ACCOUNT_EMAIL>"
  # Replace with your service account email, this is what Dataflow will run as.
}

# Datastream Parameters
datastream_params = {
  source_connection_profile_id = "<YOUR_SOURCE_CONNECTION_PROFILE_ID>"
  # ID of the MySQL source connection profile
  target_connection_profile_id = "<YOUR_TARGET_CONNECTION_PROFILE_ID>"
  # ID of the GCS target connection profile
  target_gcs_bucket_name       = "<YOUR_TARGET_GCS_BUCKET_NAME>"
  # Name of the target GCS bucket used in the target connection profile above.
  mysql_databases              = [
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
    max_workers = 10 # Adjust based on your requirements
    num_workers = 4  # Adjust based on your requirements
    on_delete   = "cancel" # Or "drain"
  }
}
