variable "common_params" {
  type = object({
    add_policies_to_service_account = optional(bool, true)
    # Template parameters
    run_id                           = string
    project                          = string
    host_project                     = optional(string)
    region                           = string
    working_directory_bucket         = string # example "test-bucket"
    working_directory_prefix         = string # should not start or end with a '/'
    jdbc_driver_jars                 = optional(string)
    jdbc_driver_class_name           = optional(string)
    num_partitions                   = optional(number)
    max_connections                  = optional(number)
    instance_id                      = string
    database_id                      = string
    spanner_project_id               = string
    spanner_host                     = optional(string)
    local_session_file_path          = string
    transformation_jar_path          = optional(string)
    transformation_custom_parameters = optional(string)
    transformation_class_name        = optional(string)
    fetch_size                       = optional(number)
    gcs_output_directory             = optional(string)

    # Dataflow runtime parameters
    additional_experiments = optional(list(string), [
      "disable_runner_v2", "use_network_tags=allow-dataflow", "use_network_tags_for_flex_templates=allow-dataflow"
    ])
    additional_pipeline_options = optional(list(string))
    network                     = optional(string)
    subnetwork                  = optional(string)
    service_account_email       = optional(string)
    # Recommend using larger launcher VMs. Machine with >= 16 vCPUs should be safe.
    launcher_machine_type = optional(string, "n1-highmem-32")
    machine_type          = optional(string, "n1-highmem-4")
    max_workers           = optional(number)
    ip_configuration      = optional(string)
    num_workers           = optional(number)
    default_log_level     = optional(string)

    # This parameters decides the number of physical shards to migrate using a single dataflow job.
    # Set this in a way that restricts the total number of tables to 150 within a single job.
    # Ex: if each physical shard has 2 logical shards, and each logical shard has 15 tables,
    # the batch size should not exceed 5.
    batch_size = optional(number, 1)
  })
  description = "Parameters which are common across jobs. Please refer to https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md for the description of the parameters below."
}

variable "data_shards" {
  type = list(object({
    dataShardId = string
    host        = string
    user        = string
    password    = string
    port        = string
    databases = list(object({
      dbName         = string
      databaseId     = string
      refDataShardId = string
    }))
  }))
}
