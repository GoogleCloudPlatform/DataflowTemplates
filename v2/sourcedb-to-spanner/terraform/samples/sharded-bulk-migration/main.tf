locals {
  sharding_config = jsondecode(file(var.common_params.local_sharding_config))
  dataShards      = local.sharding_config.shardConfigurationBulk.dataShards
  # Generate individual source configs for each group of data shards based on batch size.
  source_configs  = [
    for batch_start in range(0, length(local.dataShards), var.common_params.batch_size) : {
      configType : "dataflow",
      shardConfigurationBulk : {
        schemaSource : local.sharding_config.shardConfigurationBulk.schemaSource,
        dataShards : slice(local.dataShards, batch_start, min(batch_start + var.common_params.batch_size, length(local.dataShards)))
      }
    }
  ]

  working_directory_gcs = "gs://${var.common_params.working_directory_bucket}/${var.common_params.working_directory_prefix}"
}

resource "random_pet" "job_names" {
  count = length(local.source_configs)
}

resource "google_storage_bucket_object" "source_config_upload" {
  count      = length(local.source_configs)
  name       = "${var.common_params.working_directory_prefix}/${random_pet.job_names[count.index].id}/sourceConfig.json"
  content    = jsonencode(local.source_configs[count.index])
  bucket     = var.common_params.working_directory_bucket
  depends_on = [google_project_service.enabled_apis]
}

# upload local session file to the created GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  depends_on   = [google_project_service.enabled_apis]
  name         = "${var.common_params.working_directory_prefix}/session.json"
  source       = var.common_params.local_session_file_path
  content_type = "application/json"
  bucket       = var.common_params.working_directory_bucket
}

resource "google_dataflow_flex_template_job" "generated" {
  count      = length(local.source_configs)
  depends_on = [
    google_project_service.enabled_apis, google_storage_bucket_object.source_config_upload,
    google_storage_bucket_object.session_file_object
  ]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Sourcedb_to_Spanner_Flex"

  parameters = {
    # Uncomment these optional parameters to use custom options.
    jdbcDriverJars                 = var.common_params.jdbcDriverJars
    jdbcDriverClassName            = var.common_params.jdbcDriverClassName
    maxConnections                 = tostring(var.common_params.max_connections)
    sourceConfigURL                = "${local.working_directory_gcs}/${random_pet.job_names[count.index].id}/sourceConfig.json"
    numPartitions                  = tostring(var.common_params.num_partitions)
    instanceId                     = var.common_params.instanceId
    databaseId                     = var.common_params.databaseId
    projectId                      = var.common_params.projectId
    spannerHost                    = var.common_params.spannerHost
    sessionFilePath                = "${local.working_directory_gcs}/session.json"
    outputDirectory                = "${local.working_directory_gcs}/${random_pet.job_names[count.index].id}/output/"
    transformationJarPath          = var.common_params.transformation_jar_path
    transformationClassName        = var.common_params.transformation_class_name
    transformationCustomParameters = var.common_params.transformation_custom_parameters
    defaultSdkHarnessLogLevel      = var.common_params.defaultLogLevel
  }

  service_account_email  = var.common_params.service_account_email
  additional_experiments = var.common_params.additional_experiments
  launcher_machine_type  = var.common_params.launcher_machine_type
  machine_type           = var.common_params.machine_type
  max_workers            = var.common_params.max_workers
  name                   = "${random_pet.job_names[count.index].id}"
  ip_configuration       = var.common_params.ip_configuration
  network                = var.common_params.network
  subnetwork             = var.common_params.subnetwork
  num_workers            = var.common_params.num_workers
  project                = var.common_params.project
  region                 = var.common_params.region
}