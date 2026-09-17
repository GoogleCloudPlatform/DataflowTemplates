common_params = {
  project                         = "<YOUR_PROJECT_ID>"
  region                          = "<YOUR_GCP_REGION>"
  migration_id                    = "<YOUR_MIGRATION_ID>"
  add_policies_to_service_account = false
}

dataflow_params = {
  template_params = {
    batch_size                    = 100
    insert_qps                    = 1000
    update_qps                    = 0
    delete_qps                    = 0
    update_interval               = 5
    delete_interval               = 5
    local_schema_config_file_path = null                      # "/path/to/schema-config.json"
    local_sink_options_file_path  = "mysql-sink-options.json" # Or your custom sink options file name
    sink_options_gcs_path         = null                      # "gs://my-bucket/my-options.json"
    dlq_directory                 = null                      # "gs://my-bucket/dlq"
    max_parallelism               = null                      # optional parameter
  }
  runner_params = {
    container_spec_gcs_path      = "gs://dataflow-templates-<YOUR_GCP_REGION>/latest/flex/Cdc_Data_Generator"
    max_workers                  = 10
    num_workers                  = 2
    job_name                     = "data-generator-mysql-job"
    network                      = "<YOUR_VPC_NETWORK>"
    subnetwork                   = "<YOUR_SUBNETWORK>"
    enable_streaming_engine      = true
    additional_experiments       = ["enable_google_cloud_profiler", "enable_stackdriver_agent_metrics", "enable_google_cloud_heap_sampling", "use_runner_v2"]
    autoscaling_algorithm        = null
    kms_key_name                 = null
    labels                       = {}
    launcher_machine_type        = "n1-standard-1"
    machine_type                 = "n2-standard-8"
    sdk_container_image          = null
    service_account_email        = null
    skip_wait_on_job_termination = false
    staging_location             = null
    temp_location                = null
    on_delete                    = "cancel"
    ip_configuration             = null
  }
}
