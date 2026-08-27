# ==========================================
# Global Project & Provider Configuration
# ==========================================
# Provide the Google Cloud project ID
project             = "<YOUR_PROJECT_ID>"

# The region to deploy the Dataflow job
region              = "<YOUR_REGION>"

# ==========================================
# Dataflow Job Configuration
# ==========================================
# The name of the Dataflow job
job_name            = "<YOUR_JOB_NAME>"

# ==========================================
# Source (GCS) Configuration
# ==========================================
# The Cloud Storage directory containing validation data
gcs_input_directory = "gs://<YOUR_BUCKET_NAME>/<YOUR_DIRECTORY>/"

# ==========================================
# Destination (Spanner) Configuration
# ==========================================
# The Spanner instance ID to validate against
instance_id         = "<YOUR_SPANNER_INSTANCE_ID>"

# The Spanner database ID to validate against
database_id         = "<YOUR_SPANNER_DATABASE_ID>"

# The Google Cloud project ID where the Spanner instance is located
spanner_project_id  = "<YOUR_SPANNER_PROJECT_ID>"

# ==========================================
# Reporting (BigQuery) Configuration
# ==========================================
# The BigQuery dataset to store validation reports
bigquery_dataset    = "<YOUR_BIGQUERY_DATASET>"
