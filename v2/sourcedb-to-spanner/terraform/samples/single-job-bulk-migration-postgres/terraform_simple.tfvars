# Below is a simplified version of terraform.tfvars which only configures the
# most commonly set properties.
job_name                 = "test-job"
project                  = "project-name"
region                   = "us-central1"               # Or your desired region
working_directory_bucket = "bucket-name"               # example "test-bucket"
working_directory_prefix = "path/to/working/directory" # should not start or end with a '/'
source_config_url        = "jdbc:mysql://127.4.5.30:3306/my-db"
username                 = "root"
password                 = "abc"
instance_id              = "my-spanner-instance"
database_id              = "my-spanner-database"
spanner_project_id       = "my-spanner-project"