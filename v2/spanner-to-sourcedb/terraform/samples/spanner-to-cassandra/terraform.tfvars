common_params = {
  # The project where the resources will be deployed
  project = "<YOUR_PROJECT_ID>" # Replace with your GCP project ID
  # The host project in case of Shared VPC setup
  host_project = "<YOUR_HOST_PROJECT_ID>" # If you are using a shared VPC
  # The region where the resources will be deployed
  region = "us-central1"
  # Optional ID for the migration process
  migration_id = "migration-123"
  # Optional name for the replication bucket (defaults to "rr-bucket")
  replication_bucket = "my-replication-bucket"
  # Optional flag to control adding policies to the service account (defaults to true)
  add_policies_to_service_account = false
}

dataflow_params = {
  template_params = {
    # Optional name for the change stream
    change_stream_name = "my-change-stream"
    # The ID of the Spanner instance
    instance_id = "<YOUR_SPANNER_INSTANCE_ID>" # Replace with your Spanner instance ID
    # The ID of the Spanner database
    database_id = "<YOUR_SPANNER_DATABASE_ID>" # Replace with your Spanner database ID
    # Optional ID of the Spanner project
    spanner_project_id = "<YOUR_SPANNER_PROJECT_ID>" # Replace with your Spanner project ID (if different from the main project)
    # Optional ID of the metadata instance
    metadata_instance_id = "<YOUR_METADATA_INSTANCE_ID>" # Replace with your metadata instance ID (if applicable)
    # Optional ID of the metadata database
    metadata_database_id = "<YOUR_METADATA_DATABASE_ID>" # Replace with your metadata database ID (if applicable)
    # Optional start timestamp for replication
    start_timestamp = "2024-10-01T00:00:00Z"
    # Optional end timestamp for replication
    end_timestamp = "2024-10-31T23:59:59Z"
    # Optional prefix for shadow tables
    shadow_table_prefix = "shadow_"
    # Optional path to a local session file
    local_session_file_path = "/path/to/session/file"
    # Optional filtration mode
    filtration_mode = "column-list"
    # Optional path to a custom sharding JAR file
    sharding_custom_jar_path = "/path/to/sharding/jar"
    # Optional name of the custom sharding class
    sharding_custom_class_name = "com.example.ShardingClass"
    # Optional parameters for custom sharding
    sharding_custom_parameters = "param1=value1,param2=value2"
    # Optional timezone offset for the source database
    source_db_timezone_offset = "+08:00"
    # Optional DLQ GCS Pub/Sub subscription
    dlq_gcs_pub_sub_subscription = "projects/<YOUR_PROJECT_ID>/subscriptions/my-subscription" # Replace with your project ID and subscription name
    # Optional name of the directory to skip
    skip_directory_name = "skip-directory"
    # Optional maximum number of shard connections
    max_shard_connections = "10"
    # Optional dead letter queue directory
    dead_letter_queue_directory = "gs://my-bucket/dlq"
    # Optional maximum retry count for DLQ
    dlq_max_retry_count = "5"
    # Optional run mode
    run_mode = "regular"
    # Optional retry minutes for DLQ
    dlq_retry_minutes = "10"
  }
  runner_params = {
    # Optional additional experiments for the Dataflow runner
    additional_experiments = ["enable_google_cloud_profiler", "use_runner_v2"]
    # Optional autoscaling algorithm for the Dataflow runner
    autoscaling_algorithm = "THROUGHPUT_BASED"
    # Optional flag to enable Streaming Engine (defaults to true)
    enable_streaming_engine = false
    # Optional KMS key name for encryption
    kms_key_name = "projects/<YOUR_PROJECT_ID>/locations/<YOUR_LOCATION>/keyRings/<YOUR_KEYRING>/cryptoKeys/<YOUR_KEY>" # Replace with your project ID, location, keyring and key
    # Optional labels for the Dataflow job
    labels = { env = "dev", team = "data-eng" }
    # Optional machine type for the launcher VM
    launcher_machine_type = "n2-standard-4"
    # Optional machine type for worker VMs (defaults to "n2-standard-2")
    machine_type = "n1-standard-1"
    # Maximum number of workers for the Dataflow job
    max_workers = 100
    # Optional name for the Dataflow job (defaults to "reverse-replication-job")
    job_name = "my-replication-job"
    # Optional network for the Dataflow job
    network = "default"
    # Number of workers for the Dataflow job
    num_workers = 10
    # Optional SDK container image for the Dataflow job
    sdk_container_image = "gcr.io/dataflow-templates/spanner-to-sourcedb:latest"
    # Optional service account email for the Dataflow job
    service_account_email = "dataflow-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" # Replace with your project ID
    # Optional flag to skip waiting on job termination (defaults to false)
    skip_wait_on_job_termination = true
    # Optional staging location for the Dataflow job
    staging_location = "gs://<YOUR_BUCKET_NAME>/staging" # Replace with your bucket name
    # Optional subnetwork for the Dataflow job
    subnetwork = "regions/us-central1/subnetworks/<YOUR_SUBNETWORK>" # Replace with your subnetwork
    # Optional temporary location for the Dataflow job
    temp_location = "gs://<YOUR_BUCKET_NAME>/temp" # Replace with your bucket name
    # Optional action on delete (defaults to "drain")
    on_delete = "cancel"
    # Optional IP configuration for the Dataflow job
    ip_configuration = "WORKER_IP_PRIVATE"
  }
}

shard_list = [
  {
    # Logical ID of the shard
    logicalShardId = "shard1"
    # Hostname or IP address of the shard
    host = "<YOUR_SHARD_HOST_OR_IP>" # Replace with the shard's hostname or IP address
    # Username for connecting to the shard
    user = "root"
    # URI of the Secret Manager secret containing the password (optional)
    secretManagerUri = "projects/<YOUR_PROJECT_ID>/secrets/shard1-password/versions/latest" # Replace with your project ID and secret name
    # Password for connecting to the shard (optional, use either this or secretManagerUri)
    password = null
    # Port number for connecting to the shard
    port = "3306"
    # Name of the database on the shard
    dbName = "db1"
  },
  {
    # Logical ID of the shard
    logicalShardId = "shard2"
    # Hostname or IP address of the shard
    host = "<YOUR_SHARD_HOST_OR_IP>" # Replace with the shard's hostname or IP address
    # Username for connecting to the shard
    user = "root"
    # URI of the Secret Manager secret containing the password (optional)
    secretManagerUri = "projects/<YOUR_PROJECT_ID>/secrets/shard2-password/versions/latest" # Replace with your project ID and secret name
    # Password for connecting to the shard (optional, use either this or secretManagerUri)
    password = null
    # Port number for connecting to the shard
    port = "3306"
    # Name of the database on the shard
    dbName = "db2"
  }
]