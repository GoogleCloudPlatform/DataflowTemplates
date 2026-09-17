common_params = {
  project = "<YOUR_PROJECT_ID>"               # Replace with your GCP project ID
  region  = "<YOUR_GCP_REGION>"               # Replace with your desired GCP region
}

dataflow_params = {
  template_params = {
    instance_id                = "<YOUR_SPANNER_INSTANCE_ID>"      # The Spanner instance ID to validate against
    database_id                = "<YOUR_SPANNER_DATABASE_ID>"      # The Spanner database ID to validate against
    spanner_project_id         = "<YOUR_SPANNER_PROJECT_ID>"       # Optional: Project ID where the Spanner instance is located
    gcs_input_directory        = "gs://<YOUR_BUCKET>/source-avro"  # The Cloud Storage directory containing validation data
    bigquery_dataset           = "<YOUR_BIGQUERY_DATASET>"         # The BigQuery dataset to store validation reports
    schema_overrides_file_path = "<YOUR_SCHEMA_OVERRIDES_FILE_PATH>" # Optional: GCS path to your overrides file
    run_id                     = "<YOUR_RUN_ID>"                   # Optional: Custom run identifier
  }

  runner_params = {
    job_name = "<YOUR_JOB_NAME>"               # Custom job name for the Dataflow pipeline
  }
}
