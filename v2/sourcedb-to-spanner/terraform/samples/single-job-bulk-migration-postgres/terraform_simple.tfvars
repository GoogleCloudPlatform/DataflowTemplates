# Below is a simplified version of terraform.tfvars which only configures the
# most commonly set properties.
job_name                 = "test-job"
project                  = "project-name"
region                   = "us-central1"               # Or your desired region
working_directory_bucket = "bucket-name"               # example "test-bucket"
working_directory_prefix = "path/to/working/directory" # should not start or end with a '/'
source_config_url        = "gs://your-bucket/jdbc-shard-config.json"
source_db_dialect        = "POSTGRESQL"
jdbc_driver_jars         = "gs://path/to/driver/jars/postgresql-42.7.3.jar"
jdbc_driver_class_name   = "org.postgresql.Driver"
instance_id              = "my-spanner-instance"
database_id              = "my-spanner-database"
spanner_project_id       = "my-spanner-project"