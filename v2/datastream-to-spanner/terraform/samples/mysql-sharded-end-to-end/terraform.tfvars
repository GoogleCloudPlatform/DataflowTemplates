# terraform.tfvars

common_params = {
  project                         = "<YOUR_PROJECT_ID>"      # Replace with your GCP project ID
  host_project                    = "<YOUR_HOST_PROJECT_ID>" # If you are using a shared VPC
  region                          = "<YOUR_GCP_REGION>"      # Replace with your desired GCP region (e.g., "us-central1")
  migration_id                    = "<YOUR_MIGRATION_ID>"    # Will be used as a prefix for all resources, auto-generated if not specified
  add_policies_to_service_account = "<TRUE/FALSE>"           # This will decide if roles will be attached to service accounts or not.

  datastream_params = {
    stream_prefix_path            = "<YOUR_STREAM_PREFIX>"  # Prefix for Datastream stream IDs (e.g., "data")
    enable_backfill               = true                    # This should always be enabled unless using sourcedb-to-spanner template for bulk migrations.
    max_concurrent_cdc_tasks      = "<YOUR_CDC_TASKS>"      # Maximum concurrent CDC tasks (e.g., 5)
    max_concurrent_backfill_tasks = "<YOUR_BACKFILL_TASKS>" # Maximum concurrent backfill tasks (e.g., 15)

    create_firewall_rule      = "<TRUE/FALSE>"               # This will decide if a firewall rule allowing datastream IP ranges will be created or not.
    firewall_rule_target_tags = "<YOUR_TARGET_NETWORK_TAGS>" # Target network tags on which the firewall rule will be applied.
    # Specify either firewall_rule_target_tags or firewall_rule_target_ranges to allow targets in the firewall.
    firewall_rule_target_ranges = "<YOUR_TARGET_IP_RANGES>" # Target IP ranges on which the rule firewall will be applied.

    private_connectivity_id = "<YOUR_PRIVATE_CONNECTIVITY_ID>" # If using Private Service Connect

    private_connectivity = {
      private_connectivity_id = "<YOUR_PRIVATE_CONNECTIVITY_ID>" # If using Private Service Connect
      vpc_name                = "<YOUR_VPC_NAME>"                # The VPC network name to attach Private Service Connect to
      range                   = "<YOUR_IP_RANGE>"                # The IP address range for Private Service Connect (e.g., "10.1.0.0/16")
    }

    mysql_databases = [
      {
        database = "<YOUR_DATABASE_NAME>"
        tables   = [] # List specific tables to replicate (optional)
      }
      # Add more database objects if needed
    ]
  }

  dataflow_params = {
    skip_dataflow = false
    template_params = {
      shadow_table_prefix                 = "<YOUR_SHADOW_TABLE_PREFIX>"            # Prefix for shadow tables (e.g., "shadow_")
      create_shadow_tables                = "<TRUE/FALSE>"                          # Whether to create shadow tables in Spanner
      rfc_start_date_time                 = "<YOUR_RFC_START_DATETIME>"             # RFC 3339 timestamp for the start of replication (optional)
      file_read_concurrency               = "<YOUR_CONCURRENCY>"                    # File read concurrency for Dataflow
      spanner_project_id                  = "<YOUR_PROJECT_ID>"                     # GCP project ID for Spanner
      spanner_instance_id                 = "<YOUR_SPANNER_INSTANCE_ID>"            # Spanner instance ID
      spanner_database_id                 = "<YOUR_SPANNER_DATABASE_ID>"            # Spanner database ID
      spanner_host                        = "<YOUR_SPANNER_HOST>"                   # Spanner host (typically "spanner.googleapis.com")
      dlq_retry_minutes                   = "<YOUR_DLQ_RETRY_MINUTES>"              # Retry interval for dead-letter queue messages (in minutes)
      dlq_max_retry_count                 = "<YOUR_DLQ_MAX_RETRIES>"                # Maximum retry count for dead-letter queue messages
      datastream_root_url                 = "<YOUR_DATASTREAM_ROOT_URL>"            # Datastream API root URL (typically "https://datastream.googleapis.com/v1")
      datastream_source_type              = "<YOUR_DATASTREAM_SOURCE_TYPE>"         # Datastream source type (e.g., "mysql")
      round_json_decimals                 = "<TRUE/FALSE>"                          # Whether to round JSON decimal values in Dataflow
      directory_watch_duration_in_minutes = "<YOUR_WATCH_DURATION>"                 # Directory watch duration (in minutes) for Dataflow
      spanner_priority                    = "<YOUR_SPANNER_PRIORITY>"               # Spanner priority ("HIGH", "MEDIUM", or "LOW")
      local_session_file_path             = "<YOUR_SESSION_FILE_PATH>"              # Path to local session file (optional)
      transformation_jar_path             = "<YOUR_CUSTOM_TRANSFORMATION_JAR_PATH>" # GCS path to the custom transformation JAR(Optional)
      transformation_custom_parameters    = "<YOUR_CUSTOM_PARAMETERS_FOR_JAR>"      # Custom parameters used by the transformation JAR(Optional)
      transformation_class_name           = "<YOUR_TRANSFORMATION_CLASS_NAME>"      # Fully Classified Class Name(Optional)
      filtered_events_directory           = "<YOUR_GCS_PATH_FOR_FILTERED_EVENTS>"   # GCS path to store the filtered events(Optional)
      table_overrides                     = "<YOUR_TABLE_NAME_OVERRIDES"
      column_overrides                    = "<YOUR_COLUMN_NAME_OVERRIDES"
      local_schema_overrides_file_path    = "<YOUR_LOCAL_SCHEMA_OVERRIDES_FILE_PATH>" #One of string based overrides should be used or the file based overrides; not both.
    }

    runner_params = {
      additional_experiments = ["enable_google_cloud_profiler", "enable_stackdriver_agent_metrics",
      "disable_runner_v2", "enable_google_cloud_heap_sampling"]
      autoscaling_algorithm        = "<YOUR_AUTOSCALING_ALGORITHM>"   # e.g., "BASIC", "NONE"
      enable_streaming_engine      = "<TRUE/FALSE>"                   # Whether to use Dataflow Streaming Engine
      kms_key_name                 = "<YOUR_KMS_KEY_NAME>"            # KMS key name for encryption (optional)
      labels                       = { env = "<YOUR_ENVIRONMENT>" }   # Labels for the Dataflow job
      launcher_machine_type        = "<YOUR_LAUNCHER_MACHINE_TYPE>"   # Machine type for the launcher VM (e.g., "n1-standard-1")
      machine_type                 = "<YOUR_MACHINE_TYPE>"            # Machine type for worker VMs (e.g., "n2-standard-2")
      max_workers                  = "<YOUR_MAX_WORKERS>"             # Maximum number of worker VMs
      job_name                     = "<YOUR_JOB_NAME>"                # Name of the Dataflow job
      network                      = "<YOUR_VPC_NETWORK>"             # VPC network for the Dataflow job
      num_workers                  = "<YOUR_NUM_WORKERS>"             # Initial number of worker VMs
      sdk_container_image          = "<YOUR_SDK_CONTAINER_IMAGE>"     # Dataflow SDK container image
      service_account_email        = "<YOUR_SERVICE_ACCOUNT_EMAIL>"   # Service account email for Dataflow
      skip_wait_on_job_termination = "<TRUE/FALSE>"                   # Whether to skip waiting for job termination on deletion
      staging_location             = "gs://<YOUR_GCS_BUCKET>/staging" # GCS staging location for Dataflow
      subnetwork                   = "<YOUR-FULL-PATH-SUBNETWORK>"    # Give the full path to the subnetwork
      temp_location                = "gs://<YOUR_GCS_BUCKET>/temp"    # GCS temp location for Dataflow
      on_delete                    = "<YOUR_ON_DELETE_ACTION>"        # Action on Dataflow job deletion ("cancel" or "drain")
      ip_configuration             = "<YOUR_IP_CONFIGURATION>"        # IP configuration for Dataflow workers ("WORKER_IP_PRIVATE" or "WORKER_IP_PUBLIC")
    }
  }
}

shard_list = [
  {
    shard_id = "<YOUR_SHARD_ID>" # A unique identifier for the shard (e.g., "shard-01")

    datastream_params = {
      source_connection_profile_id = "<YOUR_SOURCE_CONNECTION_PROFILE_ID>" # Datastream source connection profile ID
      mysql_host                   = "<YOUR_MYSQL_HOST>"                   # MySQL host address
      mysql_username               = "<YOUR_MYSQL_USERNAME>"               # MySQL username
      mysql_password               = "<YOUR_MYSQL_PASSWORD>"               # MySQL password
      mysql_port                   = "<YOUR_MYSQL_PORT>"                   # MySQL port (typically 3306)

      target_connection_profile_id = "<YOUR_TARGET_CONNECTION_PROFILE_ID>" # Datastream target connection profile ID (GCS)
      gcs_bucket_name              = "<YOUR_GCS_BUCKET_NAME>"              # GCS bucket name for storing change data
      gcs_root_path                = "<YOUR_GCS_ROOT_PATH>"                # Root path within the GCS bucket (e.g., "/")

      pubsub_topic_name = "<YOUR_PUBSUB_TOPIC_NAME>" # Pub/Sub topic for change data notifications
      stream_id         = "<YOUR_STREAM_ID>"         # Datastream stream ID (will be prefixed with common_params.datastream_params.stream_prefix_path)
    }

    dataflow_params = {
      template_params = {
        run_mode                          = "<YOUR_RUN_MODE>"         # Dataflow run mode ("regular" or "retryDLQ")
        local_transformation_context_path = "<YOUR_CONTEXT_PATH>"     # Path to local transformation context (optional)
        dlq_gcs_pub_sub_subscription      = "<YOUR_DLQ_SUBSCRIPTION>" # Pub/Sub subscription for the dead-letter queue (optional)
      }

      runner_params = {
        max_workers  = "<YOUR_MAX_WORKERS>"  # Maximum number of worker VMs for this shard
        num_workers  = "<YOUR_NUM_WORKERS>"  # Initial number of worker VMs for this shard
        machine_type = "<YOUR_MACHINE_TYPE>" # Machine type for worker VMs in this shard (e.g., "n2-standard-2")
      }
    }
  }
  # Add more shard definitions as needed
]