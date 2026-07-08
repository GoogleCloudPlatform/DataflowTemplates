variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    project                         = string
    region                          = string
    migration_id                    = optional(string)
    add_policies_to_service_account = optional(bool, true)
  })
}

variable "dataflow_params" {
  description = "Parameters for the Dataflow job."
  type = object({
    template_params = object({
      batch_size                    = optional(number, 100)
      insert_qps                    = optional(number, 1000)
      update_qps                    = optional(number, 0)
      delete_qps                    = optional(number, 0)
      update_interval               = optional(number, 5)
      delete_interval               = optional(number, 5)
      local_schema_config_file_path = optional(string)
      local_sink_options_file_path  = optional(string, "spanner-sink-options.json")
      sink_options_gcs_path         = optional(string)
      dlq_directory                 = optional(string)
      max_parallelism               = optional(number)
    })
    runner_params = object({
      container_spec_gcs_path = optional(string)
      additional_experiments = optional(set(string), [
        "enable_google_cloud_profiler", "enable_stackdriver_agent_metrics",
        "enable_google_cloud_heap_sampling", "use_runner_v2"
      ])
      autoscaling_algorithm        = optional(string)
      enable_streaming_engine      = optional(bool, true)
      kms_key_name                 = optional(string)
      labels                       = optional(map(string))
      launcher_machine_type        = optional(string)
      machine_type                 = optional(string, "n2-standard-2")
      max_workers                  = optional(number)
      job_name                     = optional(string, "cdc-data-generator-job")
      network                      = optional(string)
      num_workers                  = optional(number)
      sdk_container_image          = optional(string)
      service_account_email        = optional(string)
      skip_wait_on_job_termination = optional(bool, false)
      staging_location             = optional(string)
      subnetwork                   = optional(string)
      temp_location                = optional(string)
      on_delete                    = optional(string, "cancel")
      ip_configuration             = optional(string)
    })
  })
}