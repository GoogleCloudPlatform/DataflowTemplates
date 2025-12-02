common_params = {
  project                         = "span-cloud-ck-testing-external"      # Replace with your GCP project ID
  region                          = "us-central1"      # Replace with your desired GCP region (e.g., "us-central1")
  migration_id                    = "aastha-live-timestamp-long"    # Will be used as a prefix for all resources, auto-generated if not specified

  datastream_params = {
    stream_prefix_path            = "data"                # Prefix for Datastream stream IDs (e.g., "data")
    gcs_bucket_name               = "bucket"              # GCS bucket name for storing change data
    pubsub_topic_name             = "pubsub"            # Pub/Sub topic for change data notifications

    # private_connectivity = {
    #   private_connectivity_id = "priv-conn-default"
    #   vpc_name                = "default"
    #   range                   = "10.1.1.0/24"
    # }

    mysql_databases = [
      {
        database = "dev_s5_fos_jasf_profiles"
      }
    ]
  }

  dataflow_params = {
    skip_dataflow = false
    template_params = {
      shadow_table_prefix                 = "shadow_live"            # Prefix for shadow tables (e.g., "shadow_")
      create_shadow_tables                = true                          # Whether to create shadow tables in Spanner
      spanner_project_id                  = "span-cloud-ck-testing-external"                     # GCP project ID for Spanner
      spanner_instance_id                 = "ea-functional-tests"            # Spanner instance ID
      spanner_database_id                 = "jasf_profiles"            # Spanner database ID
      datastream_source_type              = "mysql"         # Datastream source type (e.g., "mysql")
      local_session_file_path             = "/Users/aasthabharill/Downloads/ea-functional-tests/jasf_profiles.session.json"              # Path to local session file (optional)
      local_sharding_context_path         = "shardingContext.json"                   # Path to local sharding context (optional)
      # filtered_events_directory           = "<YOUR_GCS_PATH_FOR_FILTERED_EVENTS>"   # GCS path to store the filtered events(Optional)
      dead_letter_queue_directory         = "gs://ea-functional-tests/ea-functional-tests/dev_s5_fos_jasf_profiles/live-migration/bug-fix/dlq"
      dlq_max_retry_count                 = 5
      # run_mode                            = "retryDLQ"                       # Dataflow run mode ("regular" or "retryDLQ")
      # transformation_jar_path             = "gs://ea-functional-tests/ea-functional-tests/dev_s5_fos_jasf_profiles/spanner-custom-shard-1.0-SNAPSHOT.jar" # GCS path to the custom transformation JAR(Optional)
      # transformation_class_name           = "com.custom.CustomTransformationFetcher"      # Fully Classified Class Name(Optional)
    }

    runner_params = {
      # additional_experiments = ["enable_google_cloud_profiler", "enable_stackdriver_agent_metrics",
      # "disable_runner_v2", "enable_google_cloud_heap_sampling"]
      # autoscaling_algorithm        = "<YOUR_AUTOSCALING_ALGORITHM>"   # e.g., "BASIC", "NONE"
      # enable_streaming_engine      = "<TRUE/FALSE>"                   # Whether to use Dataflow Streaming Engine
      # kms_key_name                 = "<YOUR_KMS_KEY_NAME>"            # KMS key name for encryption (optional)
      # labels                       = { env = "<YOUR_ENVIRONMENT>" }   # Labels for the Dataflow job
      # launcher_machine_type        = "<YOUR_LAUNCHER_MACHINE_TYPE>"   # Machine type for the launcher VM (e.g., "n1-standard-1")
      # machine_type                 = "<YOUR_MACHINE_TYPE>"            # Machine type for worker VMs (e.g., "n2-standard-2")
      max_workers                  = "15"             # Maximum number of worker VMs
      job_name                     = "migration"                # Name of the Dataflow job
      # network                      = "default"             # VPC network for the Dataflow job
      num_workers                  = "1"             # Initial number of worker VMs
      # sdk_container_image          = "<YOUR_SDK_CONTAINER_IMAGE>"     # Dataflow SDK container image
      # service_account_email        = "<YOUR_SERVICE_ACCOUNT_EMAIL>"   # Service account email for Dataflow
      # skip_wait_on_job_termination = "<TRUE/FALSE>"                   # Whether to skip waiting for job termination on deletion
      staging_location             = "gs://ea-functional-tests/dev_s5_fos_jasf_profiles/live-migration/bug-fix/staging" # GCS staging location for Dataflow
      # subnetwork                   = "default"    # Give the full path to the subnetwork
      temp_location                = "gs://ea-functional-tests/dev_s5_fos_jasf_profiles/live-migration/bug-fix/temp"    # GCS temp location for Dataflow
      # on_delete                    = "<YOUR_ON_DELETE_ACTION>"        # Action on Dataflow job deletion ("cancel" or "drain")
      # ip_configuration             = "WORKER_IP_PRIVATE"        # IP configuration for Dataflow workers ("WORKER_IP_PRIVATE" or "WORKER_IP_PUBLIC")
    }
  }
}
# Shards
shard_list = [
  {
    shard_id = "shard1"
    datastream_params = {
      mysql_host     = "34.59.240.202"     # MySQL host address
      mysql_username = "aasthabharill" # MySQL username
      mysql_password = "Welcome@1" # MySQL password
      mysql_port     = "3306"     # MySQL port (typically 3306)
    }
  },
  {
    shard_id = "shard2"
    datastream_params = {
      mysql_host     = "34.56.235.146"     # MySQL host address
      mysql_username = "aasthabharill" # MySQL username
      mysql_password = "Welcome@1" # MySQL password
      mysql_port     = "3306"     # MySQL port (typically 3306)
    }
  },
]