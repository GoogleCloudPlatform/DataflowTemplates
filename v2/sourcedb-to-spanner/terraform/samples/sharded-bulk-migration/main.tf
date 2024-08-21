locals {
  sharding_config = jsondecode(file(var.common_params.local_sharding_config))
  dataShards      = local.sharding_config.shardConfigurationBulk.dataShards
  # Generate individual source configs for each data shard
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

resource "google_storage_bucket_object" "source_config_upload" {
  for_each   = {for idx, config in local.source_configs : idx => config}
  name       = "${var.common_params.working_directory_prefix}/${local.dataShards[each.key].dataShardId}/sourceConfig.json"
  content    = jsonencode(each.value)
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

resource "random_pet" "job_name_suffix" {
  count = length(local.dataShards)
}

resource "google_dataflow_flex_template_job" "generated" {
  count      = length(local.dataShards)
  depends_on = [
    google_project_service.enabled_apis, google_storage_bucket_object.source_config_upload,
    google_storage_bucket_object.session_file_object
  ]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Sourcedb_to_Spanner_Flex"

  parameters = {
    #jdbcDriverJars            = var.common_params.jdbcDriverJars
    #jdbcDriverClassName       = var.common_params.jdbcDriverClassName
    sourceConfigURL           = "${local.working_directory_gcs}/${local.dataShards[count.index].dataShardId}/sourceConfig.json"
    numPartitions             = tostring(var.common_params.num_partitions)
    instanceId                = var.common_params.instanceId
    databaseId                = var.common_params.databaseId
    projectId                 = var.common_params.projectId
    spannerHost               = var.common_params.spannerHost
    #maxConnections            = tostring(var.common_params.max_connections)
    sessionFilePath           = "${local.working_directory_gcs}/session.json"
    outputDirectory           = "${local.working_directory_gcs}/${local.dataShards[count.index].dataShardId}/output/"
    defaultSdkHarnessLogLevel = var.common_params.defaultLogLevel
  }

  additional_experiments = var.common_params.additional_experiments
  ip_configuration       = var.common_params.ip_configuration
  launcher_machine_type  = var.common_params.launcher_machine_type
  machine_type           = var.common_params.machine_type
  max_workers            = var.common_params.max_workers
  name                   = "${random_pet.job_name_suffix[count.index].id}-${local.dataShards[count.index].dataShardId}"
  network                = var.common_params.network
  subnetwork             = var.common_params.subnetwork
  num_workers            = var.common_params.num_workers
  project                = var.common_params.project
  region                 = var.common_params.region
}