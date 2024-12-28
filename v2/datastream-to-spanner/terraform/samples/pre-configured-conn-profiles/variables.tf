variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    project      = string
    host_project = optional(string)
    region       = string
    migration_id = optional(string)
    # Will be auto-generated if not specified
    add_policies_to_service_account = optional(bool, true)
  })
}

variable "datastream_params" {
  description = "Parameters to setup Datastream"
  type = object({
    source_connection_profile_id = string
    target_connection_profile_id = string
    # Datastream does not expose a data source to read connection profile information
    # so the bucket has to be explicitly added. This will be simplified in the
    # future to only require the target_connection_profile_id.
    # Specify the bucket name without gs:// prefix.
    target_gcs_bucket_name        = string
    pubsub_topic_name             = optional(string, "live-migration")
    stream_id                     = optional(string, "mysql-stream")
    stream_prefix_path            = optional(string, "data")
    max_concurrent_cdc_tasks      = optional(number, 5)
    max_concurrent_backfill_tasks = optional(number, 20)
    mysql_database = object({
      database = string
      tables   = optional(list(string))
    })
  })
}

variable "dataflow_params" {
  description = "Parameters for the Dataflow job. Please refer to https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md for the description of the parameters below."
  type = object({
    template_params = object({
      shadow_table_prefix                 = optional(string)
      create_shadow_tables                = optional(bool)
      rfc_start_date_time                 = optional(string)
      file_read_concurrency               = optional(number)
      local_session_file_path             = optional(string)
      spanner_project_id                  = optional(string)
      spanner_instance_id                 = string
      spanner_database_id                 = string
      spanner_host                        = optional(string)
      dead_letter_queue_directory         = optional(string)
      dlq_retry_minutes                   = optional(number)
      dlq_max_retry_count                 = optional(number)
      datastream_root_url                 = optional(string)
      datastream_source_type              = optional(string)
      round_json_decimals                 = optional(bool)
      run_mode                            = optional(string)
      directory_watch_duration_in_minutes = optional(string)
      spanner_priority                    = optional(string)
      dlq_gcs_pub_sub_subscription        = optional(string)
      transformation_jar_path             = optional(string)
      transformation_custom_parameters    = optional(string)
      transformation_class_name           = optional(string)
      filtered_events_directory           = optional(string)
      table_overrides                     = optional(string)
      column_overrides                    = optional(string)
      local_schema_overrides_file_path    = optional(string)
    })
    runner_params = object({
      additional_experiments = optional(set(string), [
        "enable_google_cloud_profiler", "enable_stackdriver_agent_metrics",
        "disable_runner_v2", "enable_google_cloud_heap_sampling"
      ])
      autoscaling_algorithm        = optional(string)
      enable_streaming_engine      = optional(bool, true)
      kms_key_name                 = optional(string)
      labels                       = optional(map(string))
      launcher_machine_type        = optional(string)
      machine_type                 = optional(string, "n2-standard-2")
      max_workers                  = number
      job_name                     = optional(string, "live-migration-job")
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