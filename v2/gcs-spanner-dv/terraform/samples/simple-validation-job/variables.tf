variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    project                         = string
    host_project                    = optional(string, "")
    region                          = string
    add_policies_to_service_account = optional(bool, true)
  })
}

variable "dataflow_params" {
  description = "Parameters for the Dataflow job."
  type = object({
    template_params = object({
      gcs_input_directory              = string
      instance_id                      = string
      database_id                      = string
      spanner_project_id               = optional(string, "")
      bigquery_dataset                 = string
      spanner_host                     = optional(string, "")
      spanner_priority                 = optional(string, null)
      session_file_path                = optional(string, "")
      local_session_file_path          = optional(string, null)
      schema_overrides_file_path       = optional(string, "")
      table_overrides                  = optional(string, "")
      column_overrides                 = optional(string, "")
      run_id                           = optional(string, "")
      transformation_jar_path          = optional(string, "")
      transformation_class_name        = optional(string, "")
      transformation_custom_parameters = optional(string, "")
      working_directory_bucket         = optional(string, null)
      working_directory_prefix         = optional(string, null)
    })
    runner_params = object({
      job_name                    = string
      service_account_email       = optional(string, "")
      network                     = optional(string, "")
      subnetwork                  = optional(string, "")
      machine_type                = optional(string, "n1-standard-4")
      max_workers                 = optional(number, null)
      num_workers                 = optional(number, null)
      additional_experiments      = optional(list(string), [])
      ip_configuration            = optional(string, "")
      launcher_machine_type       = optional(string, "")
      additional_pipeline_options = optional(map(string), {})
      labels                      = optional(map(string), {})
      kms_key_name                = optional(string, "")
    })
  })
}
