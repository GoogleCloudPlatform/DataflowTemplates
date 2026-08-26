# Below is a simplified version of the configuration for GCS to Spanner Data Validation
# Provide the Google Cloud project ID
project             = "my-gcp-project"

# The region to deploy the Dataflow job
region              = "us-central1"

# The name of the Dataflow job
job_name            = "gcs-spanner-dv-sample"

# The Cloud Storage directory containing validation data (should end with a '/')
gcs_input_directory = "gs://my-bucket/validation-data/"

# The Spanner instance ID to validate against
instance_id         = "my-spanner-instance"

# The Spanner database ID to validate against
database_id         = "my-spanner-database"

# The BigQuery dataset to store validation reports
bigquery_dataset    = "validation_report_dataset"
