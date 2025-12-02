common_params = {
  # The project where the resources will be deployed
  project = "span-cloud-ck-testing-external" # Replace with your GCP project ID
  # The region where the resources will be deployed
  region = "us-central1"
  # Optional ID for the migration process
  migration_id = "aastha-reverse-bytes-test1"
  # Optional name for the replication bucket (defaults to "rr-bucket")
  # replication_bucket = "rr-watermark"
}

dataflow_params = {
  template_params = {
    # The ID of the Spanner instance
    instance_id = "ea-functional-tests" # Replace with your Spanner instance ID
    # The ID of the Spanner database
    database_id = "custom_transform_test" # Replace with your Spanner database ID
    # Optional ID of the Spanner project
    spanner_project_id = "span-cloud-ck-testing-external" # Replace with your Spanner project ID (if different from the main project)
    # Optional ID of the metadata instance
    # metadata_instance_id = "aastha-metadata-instance" # Replace with your metadata instance ID (if applicable)
    # Optional ID of the metadata database
    # metadata_database_id = "custom_transform_test" # Replace with your metadata database ID (if applicable)
    # Optional prefix for shadow tables

    shadow_table_prefix = "rev_shadow_"
    # Optional path to a local session file
    local_session_file_path = "/Users/aasthabharill/Downloads/ea-functional-tests/custom_transform_test.session.json"
    # # # Optional DLQ GCS Pub/Sub subscription
    # dlq_gcs_pub_sub_subscription = "projects/span-cloud-ck-testing-external/subscriptions/ea-itemhouse-dlq-topic-sub" # Replace with your project ID and subscription name
    # Optional maximum number of shard connections
    # max_shard_connections = "600"
    # # Optional dead letter queue directory
    dead_letter_queue_directory = "gs://ea-functional-tests/custom_transform_test/reverse-migration/dlq"
    # Optional maximum retry count for DLQ
    dlq_max_retry_count = "5"
    # # # Optional run mode
    # run_mode = "retryDLQ"
    # # Optional retry minutes for DLQ
    # dlq_retry_minutes = "10"
    # Optional path to a custom sharding JAR file
    sharding_custom_jar_path = "gs://ea-functional-tests/custom_transform_test/spanner-custom-shard-1.0-SNAPSHOT.jar"
    sharding_custom_class_name = "com.custom.CustomShardIdFetcher"
    transformation_jar_path             = "gs://ea-functional-tests/custom_transform_test/spanner-custom-shard-1.0-SNAPSHOT.jar" # GCS path to the custom transformation JAR(Optional)
    transformation_class_name           = "com.custom.CustomTransformationFetcher"
  }
  runner_params = {
    # Optional machine type for the launcher VM
    launcher_machine_type = "n2-standard-4"
    # Optional machine type for worker VMs (defaults to "n2-standard-2")
    machine_type = "n2-standard-8"
    # Maximum number of workers for the Dataflow job
    max_workers = 600
    # Optional name for the Dataflow job (defaults to "reverse-replication-job")
    job_name = "reverse"
    # Number of workers for the Dataflow job
    num_workers = 1
    # # Optional temporary location for the Dataflow job
    temp_location = "gs://ea-functional-tests/custom_transform_test/reverse-migration/temp" # Replace with your bucket name
    # Optional IP configuration for the Dataflow job
    ip_configuration = "WORKER_IP_PRIVATE"
  }
}

shard_list = [
  {
    "logicalShardId": "shard1_00",
    # "host": "34.59.240.202",
    "host": "10.37.232.133",
    "user": "root",
    "password": "Welcome@1",
    "port": "3306",
    "dbName": "custom_transform_test00"
  },
  {
    "logicalShardId": "shard1_01",
    # "host": "34.59.240.202",
    "host": "10.37.232.133",
    "user": "root",
    "password": "Welcome@1",
    "port": "3306",
    "dbName": "custom_transform_test01"
  },
  {
    "logicalShardId": "shard2_00",
    # "host": "34.56.235.146",
    "host": "10.37.232.135",
    "user": "root",
    "password": "Welcome@1",
    "port": "3306",
    "dbName": "custom_transform_test00"
  },
  {
    "logicalShardId": "shard2_01",
    # "host": "34.56.235.146",
    "host": "10.37.232.135",
    "user": "root",
    "password": "Welcome@1",
    "port": "3306",
    "dbName": "custom_transform_test01"
  }
]