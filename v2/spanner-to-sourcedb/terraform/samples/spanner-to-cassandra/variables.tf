variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    project                         = string
    host_project                    = optional(string)
    region                          = string
    migration_id                    = optional(string)
    replication_bucket              = optional(string, "rr-bucket")
    add_policies_to_service_account = optional(bool, true)
    target_tags                     = optional(list(string))
  })
}

variable "dataflow_params" {
  description = "Parameters for the Dataflow job. Please refer to https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-to-sourcedb/README.md for the description of the parameters below."
  type = object({
    template_params = object({
      change_stream_name           = optional(string)
      instance_id                  = string
      database_id                  = string
      spanner_project_id           = optional(string)
      metadata_instance_id         = optional(string)
      metadata_database_id         = optional(string)
      start_timestamp              = optional(string)
      end_timestamp                = optional(string)
      shadow_table_prefix          = optional(string)
      local_session_file_path      = string
      filtration_mode              = optional(string)
      sharding_custom_jar_path     = optional(string)
      sharding_custom_class_name   = optional(string)
      sharding_custom_parameters   = optional(string)
      source_db_timezone_offset    = optional(string)
      dlq_gcs_pub_sub_subscription = optional(string)
      skip_directory_name          = optional(string)
      max_shard_connections        = optional(string)
      dead_letter_queue_directory  = optional(string)
      dlq_max_retry_count          = optional(string)
      run_mode                     = optional(string)
      dlq_retry_minutes            = optional(string)
    })
    runner_params = object({
      additional_experiments = optional(set(string), [
        "enable_google_cloud_profiler", "enable_stackdriver_agent_metrics",
        "use_runner_v2", "enable_google_cloud_heap_sampling"
      ])
      autoscaling_algorithm        = optional(string)
      enable_streaming_engine      = optional(bool, true)
      kms_key_name                 = optional(string)
      labels                       = optional(map(string))
      launcher_machine_type        = optional(string)
      machine_type                 = optional(string, "n2-standard-2")
      max_workers                  = number
      job_name                     = optional(string, "reverse-replication-job")
      network                      = optional(string)
      num_workers                  = number
      sdk_container_image          = optional(string)
      service_account_email        = optional(string)
      skip_wait_on_job_termination = optional(bool, false)
      staging_location             = optional(string)
      subnetwork                   = optional(string)
      temp_location                = optional(string)
      on_delete                    = optional(string, "drain")
      ip_configuration             = optional(string)
    })
  })
}

variable "shard_list" {
  description = "Details of the source shards to do the reverse replication to"
  type = list(object({
    logicalShardId   = string
    host             = string
    user             = string
    secretManagerUri = optional(string)
    password         = optional(string)
    port             = string
    dbName           = string
  }))
}

variable "shard_config" {
  type = map(any)
  default = {
    host             = "host_name"
    port             = "port_name"
    user             = "user_name"
    password         = "password"
    keyspace         = "your_keyspace_name"
    consistencyLevel = "consistency_level"
    sslOptions       = false
    protocolVersion  = "v5"
    dataCenter       = "datacenter_name"
    localPoolSize    = "local_pool_size"
    remotePoolSize   = "remote_pool_size"
  }
}

# Path to the existing shard_config.conf file
variable "cassandra_template_config_file" {
  default = ""
}