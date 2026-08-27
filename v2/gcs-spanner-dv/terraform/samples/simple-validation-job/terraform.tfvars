common_params = {
  project                         = "<YOUR_PROJECT_ID>"               # Replace with your GCP project ID
  host_project                    = "<YOUR_HOST_PROJECT_ID>"          # Optional: Project ID hosting the network if using a shared VPC
  region                          = "<YOUR_GCP_REGION>"               # Replace with your desired GCP region
  add_policies_to_service_account = true                              # Optional: Whether Terraform should add required permissions to the service account
}

dataflow_params = {
  template_params = {
    gcs_input_directory              = "gs://<YOUR_BUCKET>/source-avro"  # The Cloud Storage directory containing validation data
    instance_id                      = "<YOUR_SPANNER_INSTANCE_ID>"      # The Spanner instance ID to validate against
    database_id                      = "<YOUR_SPANNER_DATABASE_ID>"      # The Spanner database ID to validate against
    spanner_project_id               = "<YOUR_SPANNER_PROJECT_ID>"       # Optional: Project ID where the Spanner instance is located (defaults to Dataflow project)
    bigquery_dataset                 = "<YOUR_BIGQUERY_DATASET>"         # The BigQuery dataset to store validation reports (e.g., validation_report_dataset)
    spanner_host                     = "<YOUR_SPANNER_HOST>"             # Optional: Custom Spanner host endpoint
    spanner_priority                 = "HIGH"                            # Optional: Priority for Spanner RPC invocations (HIGH, MEDIUM, LOW)
    local_session_file_path          = "<YOUR_LOCAL_SESSION_FILE_PATH>"  # Optional: Local path to the session file (will be uploaded to working directory)
    session_file_path                = "<YOUR_SESSION_FILE_PATH>"        # Optional: Existing GCS path to the session file
    schema_overrides_file_path       = "<YOUR_SCHEMA_OVERRIDES_FILE_PATH>" # Optional: GCS path to your overrides file
    table_overrides                  = "<YOUR_TABLE_OVERRIDES>"          # Optional: Table name overrides (e.g., "[{OldTableName,NewTableName}]")
    column_overrides                 = "<YOUR_COLUMN_OVERRIDES>"         # Optional: Column name overrides (e.g., "[{TableName.OldColumnName,TableName.NewColumnName}]")
    transformation_jar_path          = "<YOUR_TRANSFORMATION_JAR_PATH>"  # Optional: GCS path to the transformation JAR file
    transformation_class_name        = "<YOUR_TRANSFORMATION_CLASS_NAME>" # Optional: Fully qualified transformation class name
    transformation_custom_parameters = "<YOUR_TRANSFORMATION_CUSTOM_PARAMS>" # Optional: Custom parameters for the transformation
    working_directory_bucket         = "<YOUR_WORKING_DIRECTORY_BUCKET>" # Optional: Bucket for uploading session file and creating output directory
    working_directory_prefix         = "<YOUR_WORKING_DIRECTORY_PREFIX>" # Optional: Prefix within the GCS bucket for working directory
    run_id                           = "<YOUR_RUN_ID>"                   # Optional: Custom run identifier
  }

  runner_params = {
    job_name                    = "data-validation-job"             # Or your custom job name
    service_account_email       = "<YOUR_SERVICE_ACCOUNT_EMAIL>"    # Optional: Service account email for Dataflow workers
    network                     = "<YOUR_NETWORK>"                  # Optional: Network for Dataflow workers
    subnetwork                  = "<YOUR_SUBNETWORK>"               # Optional: Subnetwork for Dataflow workers
    machine_type                = "n2-standard-4"                   # Optional: Machine type for Dataflow worker VMs
    max_workers                 = 10                                # Optional: Maximum number of Dataflow worker VMs
    num_workers                 = 4                                 # Optional: Initial number of Dataflow worker VMs
    additional_experiments      = ["<EXPERIMENT_1>"]                # Optional: Additional Dataflow experiments (list of strings)
    ip_configuration            = "WORKER_IP_PRIVATE"               # Optional: IP configuration for Dataflow workers
    launcher_machine_type       = "n1-standard-1"                   # Optional: Machine type for the Dataflow launcher VM
    additional_pipeline_options = { "<KEY>" = "<VALUE>" }           # Optional: Additional Dataflow pipeline options (map of strings)
    labels                      = { "env" = "test" }                # Optional: Labels to apply to the Dataflow job (map of strings)
    kms_key_name                = "<YOUR_KMS_KEY_NAME>"             # Optional: Cloud KMS key name for data encryption
  }
}
