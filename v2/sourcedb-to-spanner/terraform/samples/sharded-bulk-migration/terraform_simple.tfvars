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
  project_id               = "my-spanner-project"
  local_session_file_path  = "/local/path/to/smt/session/file"

  additional_experiments = ["disable_runner_v2"] # This option is required for bulk jobs. Do not remove.

  batch_size = 1
}

data_shards = [
  {
    data_shard_id = "data-shard1"
    host          = "10.1.1.1"
    user          = "username"
    password      = "password"
    port          = 3306
    databases = [
      {
        db_name           = "db1"
        database_id       = "logicaldb1"
        ref_data_shard_id = "data-shard1"
      }
    ]
  }
]