common_params = {
  run_id                   = "sharded-run"
  project                  = "project-name"
  host_project             = "host-project"
  region                   = "us-central1"               # Or your desired region
  working_directory_bucket = "bucket-name"               # example "test-bucket"
  working_directory_prefix = "path/to/working/directory" # should not start or end with a '/'
  jdbc_driver_jars         = "gs://path/to/driver/jars"
  jdbc_driver_class_name   = "com.mysql.jdbc.driver"
  num_partitions           = 4000
  max_connections          = 320
  instance_id              = "my-spanner-instance"
  database_id              = "my-spanner-database"
  spanner_project_id       = "my-spanner-project"
  spanner_host             = "https://batch-spanner.googleapis.com"
  local_session_file_path  = "/local/path/to/smt/session/file"

  network               = "network-name"
  subnetwork            = "subnetwork-name"
  service_account_email = "your-service-account-email@your-project-id.iam.gserviceaccount.com"
  launcher_machine_type = "n1-highmem-32" # Recommend using larger launcher VMs
  machine_type          = "n1-highmem-4"
  max_workers           = 50
  ip_configuration      = "WORKER_IP_PRIVATE"
  num_workers           = 1
  default_log_level     = "INFO"

  # This parameters decides the number of physical shards to migrate using a single dataflow job.
  # Set this in a way that restricts the total number of tables to 150 within a single job.
  # Ex: if each physical shard has 2 logical shards, and each logical shard has 15 tables,
  # the batch size should not exceed 5.
  batch_size = 10
}

data_shards = [
  {
    dataShardId = "data-shard1"
    host        = "10.128.0.2"
    user        = "user"
    password    = "password"
    port        = "3306"
    databases = [
      {
        dbName         = "tftest"
        databaseId     = "logicaldb1"
        refDataShardId = "data-shard1"
      }
    ]
  },
  {
    dataShardId = "data-shard2"
    host        = "10.128.0.3"
    user        = "user"
    password    = "password"
    port        = "3306"
    databases = [
      {
        dbName         = "tftest"
        databaseId     = "logicaldb2"
        refDataShardId = "data-shard2"
      }
    ]
  }
]