variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    project      = string
    host_project = optional(string)
    region       = string
    migration_id = optional(string)
    # Will be auto-generated if not specified
    add_policies_to_service_account = optional(bool, true)
    datastream_params = object({
      gcs_bucket_name               = optional(string, "live-migration")
      pubsub_topic_name             = optional(string, "live-migration")
      stream_prefix_path            = optional(string, "data")
      target_connection_profile_id  = optional(string, "target-gcs")
      gcs_root_path                 = optional(string, "/")
      source_type                   = optional(string, "mysql")
      enable_backfill               = optional(bool, true)
      max_concurrent_cdc_tasks      = optional(number, 5)
      max_concurrent_backfill_tasks = optional(number, 20)
      private_connectivity_id       = optional(string)
      private_connectivity = optional(object({
        private_connectivity_id = optional(string, "priv-conn")
        vpc_name                = string
        range                   = string
      }))
      mysql_databases = list(object({
        database = string
        tables   = optional(list(string))
      }))
    })
    dataflow_params = object({
      skip_dataflow = optional(bool, false)
      template_params = object({
        shadow_table_prefix                 = optional(string)
        create_shadow_tables                = optional(bool)
        rfc_start_date_time                 = optional(string)
        file_read_concurrency               = optional(number)
        spanner_project_id                  = optional(string)
        spanner_instance_id                 = string
        spanner_database_id                 = string
        spanner_host                        = optional(string)
        dlq_retry_minutes                   = optional(number)
        dlq_max_retry_count                 = optional(number)
        datastream_root_url                 = optional(string)
        datastream_source_type              = optional(string)
        round_json_decimals                 = optional(bool)
        directory_watch_duration_in_minutes = optional(string)
        spanner_priority                    = optional(string)
        local_session_file_path             = optional(string)
        transformation_jar_path             = optional(string)
        transformation_custom_parameters    = optional(string)
        transformation_class_name           = optional(string)
        filtered_events_directory           = optional(string)
        run_mode                            = optional(string)
        local_sharding_context_path         = optional(string)
        dlq_gcs_pub_sub_subscription        = optional(string)
      })
      runner_params = object({
        additional_experiments = optional(set(string), [
          "enable_google_cloud_profiler", "enable_stackdriver_agent_metrics",
          "disable_runner_v2", "enable_google_cloud_heap_sampling", "enable_streaming_engine_resource_based_billing"
        ])
        autoscaling_algorithm        = optional(string)
        enable_streaming_engine      = optional(bool, true)
        kms_key_name                 = optional(string)
        labels                       = optional(map(string))
        launcher_machine_type        = optional(string)
        machine_type                 = optional(string, "n1-standard-4")
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
  })
}

variable "shard_list" {
  description = "Parameters for the Dataflow job. Please refer to https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/README_Cloud_Datastream_to_Spanner.md for the description of the parameters below."
  type = list(object({
    shard_id = optional(string)
    datastream_params = object({
      source_connection_profile_id = optional(string, "source-mysql")
      mysql_host                   = string
      mysql_username               = string
      mysql_password               = string
      mysql_port                   = number
      stream_id                    = optional(string, "mysql-stream")
    })
  }))
}