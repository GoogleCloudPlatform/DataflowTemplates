job_name            = "validation-job"
project             = "my-project-id"
region              = "us-central1"
gcs_input_directory = "gs://my-bucket/records-path"
instance_id         = "my-spanner-instance"
database_id         = "my-spanner-database"
spanner_project_id  = "my-spanner-project-id"
bigquery_dataset    = "my_validation_dataset"

# Optional parameters
# spanner_host               = "https://batch-spanner.googleapis.com"
# spanner_priority           = "HIGH"
# session_file_path          = "gs://my-bucket/smt-session.json"
# schema_overrides_file_path = "gs://my-bucket/overrides.json"
# table_overrides            = "[{SourceTableName1, SpannerTableName1}]"
# column_overrides           = "[{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}]"
# run_id                     = "validation-run-001"

# Network settings
# network                  = "default"
# subnetwork               = "regions/us-central1/subnetworks/default"
# service_account_email    = "my-service-account@my-project-id.iam.gserviceaccount.com"
# machine_type             = "n1-standard-4"
# max_workers              = 10
# ip_configuration         = "WORKER_IP_PRIVATE"
