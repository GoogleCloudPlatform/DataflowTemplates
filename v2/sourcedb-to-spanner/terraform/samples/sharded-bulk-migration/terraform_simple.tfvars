# Below is a simplified version of terraform.tfvars which only configures the
# most commonly set properties.

common_params = {
  project                  = "project-name"
  region                   = "us-central1"               # Or your desired region
  working_directory_bucket = "bucket-name"               # example "test-bucket"
  working_directory_prefix = "path/to/working/directory" # should not start or end with a '/'
  instanceId               = "my-spanner-instance"
  databaseId               = "my-spanner-database"
  projectId                = "my-spanner-project"
  local_session_file_path  = "/local/path/to/smt/session/file"
  local_sharding_config    = "/local/path/to/shardconfig.json"

  additional_experiments = ["disable_runner_v2"] # This option is required for bulk jobs. Do not remove.

  batch_size = 1
}