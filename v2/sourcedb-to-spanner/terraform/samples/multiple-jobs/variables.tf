variable "common_params" {
  description = "Parameters which are common across jobs."
  type        = object({
    on_delete                    = optional(string, "drain")
    project                      = string
    region                       = string
    jdbcDriverJars               = optional(string)
    jdbcDriverClassName          = optional(string)
    instanceId                   = string
    databaseId                   = string
    projectId                    = string
    spannerHost                  = optional(string, "https://batch-spanner.googleapis.com")
    sessionFilePath              = optional(string)
    disabledAlgorithms           = optional(string)
    extraFilesToStage            = optional(string)
    additional_experiments       = optional(set(string))
    autoscaling_algorithm        = optional(string)
    enable_streaming_engine      = optional(bool)
    network                      = optional(string)
    subnetwork                   = optional(string)
    sdk_container_image          = optional(string)
    service_account_email        = optional(string)
    skip_wait_on_job_termination = optional(bool, true)
    staging_location             = optional(string)
    temp_location                = optional(string)
  })
}

variable "jobs" {
  description = "List of job configurations."
  type        = list(object({
    sourceDbURL           = string
    username              = string
    password              = string
    tables                = optional(string)
    numPartitions         = optional(string)
    maxConnections        = optional(number, 0)
    DLQDirectory          = string
    defaultLogLevel       = optional(string, "INFO")
    ip_configuration      = optional(string)
    kms_key_name          = optional(string)
    labels                = optional(map(string))
    launcher_machine_type = optional(string)
    machine_type          = optional(string)
    max_workers           = optional(number)
    name                  = optional(string, "bulk-migration-job")
    num_workers           = optional(number)
  }))
}
