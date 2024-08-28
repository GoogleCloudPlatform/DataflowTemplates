common_params = {
  project                  = "project-name"
  region                   = "us-central1" # Or your desired region
  working_directory_bucket = "bucket-name" # example "test-bucket"
  working_directory_prefix = "path/to/working/directory" # should not start or end with a '/'
  jdbcDriverJars           = "gs://path/to/driver/jars"
  jdbcDriverClassName      = "com.mysql.jdbc.driver"
  num_partitions           = 4000
  max_connections          = 320
  instanceId               = "my-spanner-instance"
  databaseId               = "my-spanner-database"
  projectId                = "my-spanner-project"
  spannerHost              = "https://batch-spanner.googleapis.com"
  local_session_file_path  = "/local/path/to/smt/session/file"
  local_sharding_config    = "/Users/deepchowdhury/Downloads/128shardconfig.json"

  additional_experiments = ["disable_runner_v2"] # This option is required for bulk jobs. Do not remove.
  network                = "network-name"
  subnetwork             = "regions/<region>/subnetworks/<subnetwork-name>"
  service_account_email  = "your-service-account-email@your-project-id.iam.gserviceaccount.com"
  launcher_machine_type  = "n1-highmem-32" # Recommend using larger launcher VMs
  machine_type           = "n1-highmem-4"
  max_workers            = 50
  ip_configuration       = "WORKER_IP_PRIVATE"
  num_workers            = 1
  defaultLogLevel        = "INFO"

  # This parameters decides the number of physical shards to migrate using a single dataflow job.
  # Set this in a way that restricts the total number of tables to 150 within a single job.
  # Ex: if each physical shard has 2 logical shards, and each logical shard has 15 tables,
  # the batch size should not exceed 5.
  batch_size = 10
}