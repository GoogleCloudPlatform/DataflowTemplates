variable "common_params" {
  type = object({
    project                  = string
    region                   = string
    working_directory_prefix = string
    working_directory_bucket = string
    jdbcDriverJars           = string
    jdbcDriverClassName      = string
    num_partitions           = number
    max_connections          = number
    instanceId               = string
    databaseId               = string
    projectId                = string
    spannerHost              = string
    local_session_file_path  = string
    additional_experiments   = list(string)
    network                  = string
    subnetwork               = string
    service_account_email    = string
    launcher_machine_type    = string
    machine_type             = string
    max_workers              = number
    ip_configuration         = string
    num_workers              = number
    defaultLogLevel          = string
    local_sharding_config    = string
    batch_size               = number
  })
  description = "Common parameters shared across multiple Dataflow jobs."
}