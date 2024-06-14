# Common Parameters
common_params = {
  project = "<YOUR_PROJECT_ID>" # Replace with your GCP project ID
  region  = "<YOUR_GCP_REGION>" # Replace with your desired GCP region
}

# Datastream Parameters
datastream_params = {
  source_connection_profile_id  = "source-mysql"   # Or provide a custom ID
  target_connection_profile_id  = "target-gcs"     # Or provide a custom ID
  target_gcs_bucket_name        = "live-migration" # Or provide a custom bucket name
  pubsub_topic_name             = "live-migration" # Or provide a custom topic name
  stream_id                     = "mysql-stream"   # Or provide a custom stream ID
  max_concurrent_cdc_tasks      = 50               # Adjust as needed
  max_concurrent_backfill_tasks = 50               # Adjust as needed
  mysql_databases = [
    {
      database = "<YOUR_DATABASE_NAME>"
      tables   = [] # List specific tables to replicate (optional)
    }
    # Add more database objects if needed
  ]
}

# Dataflow Parameters
dataflow_params = {
  template_params = {
    shadow_table_prefix                 = "<YOUR_SHADOW_TABLE_PREFIX>" # e.g., "shadow_" (optional)
    create_shadow_tables                = true                         # true or false
    rfc_start_date_time                 = "<YYYY-MM-DDTHH:MM:SSZ>"     # e.g., "2023-12-31T12:00:00Z" (optional)
    file_read_concurrency               = 10                           # Adjust as needed
    session_file_path                   = "<YOUR_SESSION_FILE_PATH>"   # Path to your session file (optional)
    spanner_instance_id                 = "<YOUR_SPANNER_INSTANCE_ID>"
    spanner_database_id                 = "<YOUR_SPANNER_DATABASE_ID>"
    spanner_host                        = "https://<YOUR_REGION>-spanner.googleapis.com" # Replace <YOUR_REGION>
    dead_letter_queue_directory         = "<YOUR_DLQ_DIRECTORY>"                         # e.g., "gs://<YOUR_BUCKET>/dlq" (optional)
    dlq_retry_minutes                   = 10                                             # Adjust as needed
    dlq_max_retry_count                 = 3                                              # Adjust as needed
    datastream_root_url                 = "<YOUR_DATASTREAM_ROOT_URL>"                   # Base URL of your Datastream API (optional)
    datastream_source_type              = "MYSQL"
    round_json_decimals                 = false
    run_mode                            = "STREAMING"
    transformation_context_file_path    = "<YOUR_TRANSFORMATION_FILE_PATH>" # Path to your transformation file (optional)
    directory_watch_duration_in_minutes = "5"                               # Adjust as needed
    spanner_priority                    = "high"
    dlq_gcs_pub_sub_subscription        = "<YOUR_DLQ_PUBSUB_SUBSCRIPTION>" # Optional
  }

  runner_params = {
    additional_experiments       = []                           # Add any additional experiments or leave empty
    autoscaling_algorithm        = "THROUGHPUT_BASED"           # Or NONE
    enable_streaming_engine      = true                         # true or false
    kms_key_name                 = "<YOUR_KMS_KEY_NAME>"        # If you're using customer-managed encryption key
    labels                       = {}                           # Add any labels you want
    launcher_machine_type        = "n1-standard-1"              # Adjust as needed
    machine_type                 = "n2-standard-2"              # Adjust as needed
    max_workers                  = 10                           # Adjust based on your requirements
    job_name                     = "live-migration-job"         # Or your custom job name
    network                      = "<YOUR_NETWORK>"             # Network for your Dataflow job
    num_workers                  = 4                            # Adjust based on your requirements
    sdk_container_image          = "<YOUR_SDK_CONTAINER_IMAGE>" # If you have a custom image
    service_account_email        = "<YOUR_SERVICE_ACCOUNT_EMAIL>"
    skip_wait_on_job_termination = false
    staging_location             = "gs://<YOUR_BUCKET>/staging"
    subnetwork                   = "<YOUR_SUBNETWORK>"
    temp_location                = "gs://<YOUR_BUCKET>/tmp"
    on_delete                    = "drain" # Or "cancel"
  }
}
