variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    project                         = string
    host_project                    = optional(string, null)
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
      spanner_project_id               = optional(string, null)
      bigquery_dataset                 = string
      spanner_host                     = optional(string, null)
      spanner_priority                 = optional(string, null)
      session_file_path                = optional(string, null)
      schema_overrides_file_path       = optional(string, null)
      table_overrides                  = optional(string, null)
      column_overrides                 = optional(string, null)
      tables                           = optional(string, null)
      table_configuration_file_path             = optional(string, null)
      run_id                           = optional(string, null)
      transformation_jar_path          = optional(string, null)
      transformation_class_name        = optional(string, null)
      transformation_custom_parameters = optional(string, null)
    })
    runner_params = object({
      job_name                    = string
      service_account_email       = optional(string, null)
      network                     = optional(string, null)
      subnetwork                  = optional(string, null)
      machine_type                = optional(string, "n1-standard-4")
      max_workers                 = optional(number, null)
      num_workers                 = optional(number, null)
      additional_experiments      = optional(list(string), [])
      ip_configuration            = optional(string, null)
      launcher_machine_type       = optional(string, null)
      labels                      = optional(map(string), {})
      kms_key_name                = optional(string, null)
    })
  })
}
