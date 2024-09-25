# Below is a simplified version of terraform.tfvars which only configures the
# most commonly set properties.

common_params = {
  run_id                   = "simple-run"
  project                  = "project-name"
  region                   = "us-central1"               # Or your desired region
  working_directory_bucket = "bucket-name"               # example "test-bucket"
  working_directory_prefix = "path/to/working/directory" # should not start or end with a '/'
  instance_id              = "my-spanner-instance"
  database_id              = "my-spanner-database"
  spanner_project_id       = "my-spanner-project"
  local_session_file_path  = "/local/path/to/smt/session/file"

  batch_size = 1
}

data_shards = [
  {
    dataShardId = "data-shard1"
    host        = "10.1.1.1"
    user        = "username"
    password    = "password"
    port        = "3306"
    databases = [
      {
        dbName         = "db1"
        databaseId     = "logicaldb1"
        refDataShardId = "data-shard1"
      }
    ]
  }
]