locals {
  # Generate individual source configs for each group of data shards based on batch size.
  source_configs = [
    for batch_start in range(0, length(var.data_shards), var.common_params.batch_size) : {
      configType : "dataflow",
      shardConfigurationBulk : {
        dataShards : slice(var.data_shards, batch_start, min(batch_start + var.common_params.batch_size, length(var.data_shards)))
      }
    }
  ]

  working_directory_gcs = "gs://${var.common_params.working_directory_bucket}/${var.common_params.working_directory_prefix}"
}

resource "random_pet" "job_prefixes" {
  count  = length(local.source_configs)
  prefix = "smt"
}

resource "google_storage_bucket_object" "source_config_upload" {
  count      = length(local.source_configs)
  name       = "${var.common_params.working_directory_prefix}/${random_pet.job_prefixes[count.index].id}/shardConfig.json"
  content    = jsonencode(local.source_configs[count.index])
  bucket     = var.common_params.working_directory_bucket
  depends_on = [google_project_service.enabled_apis]
}

# Setup network firewalls rules to enable Dataflow access to source.
resource "google_compute_firewall" "allow-dataflow-to-source" {
  depends_on  = [google_project_service.enabled_apis]
  project     = var.common_params.host_project != null ? var.common_params.host_project : var.common_params.project
  name        = "allow-dataflow-to-source"
  network     = var.common_params.network != null ? var.common_params.host_project != null ? "projects/${var.common_params.host_project}/global/networks/${var.common_params.network}" : "projects/${var.common_params.project}/global/networks/${var.common_params.network}" : "default"
  description = "Allow traffic from Dataflow to source databases"

  allow {
    protocol = "tcp"
    ports    = ["3306"]
  }
  source_tags = ["dataflow"]
  target_tags = ["databases"]
}

# upload local session file to the working GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  depends_on   = [google_project_service.enabled_apis]
  name         = "${var.common_params.working_directory_prefix}/session.json"
  source       = var.common_params.local_session_file_path
  content_type = "application/json"
  bucket       = var.common_params.working_directory_bucket
}

# Add roles to the service account that will run Dataflow for bulk migration
resource "google_project_iam_member" "live_migration_roles" {
  for_each = var.common_params.add_policies_to_service_account ? toset([
    "roles/viewer",
    "roles/storage.objectAdmin",
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/spanner.databaseAdmin",
    "roles/monitoring.metricWriter",
    "roles/cloudprofiler.agent"
  ]) : toset([])
  project = data.google_project.project.id
  role    = each.key
  member  = var.common_params.service_account_email != null ? "serviceAccount:${var.common_params.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}

resource "google_dataflow_flex_template_job" "generated" {
  count = length(local.source_configs)
  depends_on = [
    google_project_service.enabled_apis, google_storage_bucket_object.source_config_upload,
    google_storage_bucket_object.session_file_object
  ]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Sourcedb_to_Spanner_Flex"

  parameters = {
    jdbcDriverJars                 = var.common_params.jdbc_driver_jars
    jdbcDriverClassName            = var.common_params.jdbc_driver_class_name
    maxConnections                 = tostring(var.common_params.max_connections)
    sourceConfigURL                = "${local.working_directory_gcs}/${random_pet.job_prefixes[count.index].id}/shardConfig.json"
    numPartitions                  = tostring(var.common_params.num_partitions)
    instanceId                     = var.common_params.instance_id
    databaseId                     = var.common_params.database_id
    projectId                      = var.common_params.spanner_project_id
    spannerHost                    = var.common_params.spanner_host
    sessionFilePath                = "${local.working_directory_gcs}/session.json"
    outputDirectory                = "${local.working_directory_gcs}/${random_pet.job_prefixes[count.index].id}/output/"
    transformationJarPath          = var.common_params.transformation_jar_path
    transformationClassName        = var.common_params.transformation_class_name
    transformationCustomParameters = var.common_params.transformation_custom_parameters
    defaultSdkHarnessLogLevel      = var.common_params.default_log_level
    fetchSize                      = var.common_params.fetch_size
    gcsOutputDirectory             = var.common_params.gcs_output_directory
  }

  service_account_email       = var.common_params.service_account_email
  additional_experiments      = var.common_params.additional_experiments
  additional_pipeline_options = var.common_params.additional_pipeline_options
  launcher_machine_type       = var.common_params.launcher_machine_type
  machine_type                = var.common_params.machine_type
  max_workers                 = var.common_params.max_workers
  name                        = "${random_pet.job_prefixes[count.index].id}-${var.common_params.run_id}"
  ip_configuration            = var.common_params.ip_configuration
  network                     = var.common_params.network != null ? var.common_params.host_project != null ? "projects/${var.common_params.host_project}/global/networks/${var.common_params.network}" : "projects/${var.common_params.project}/global/networks/${var.common_params.network}" : null
  subnetwork                  = var.common_params.subnetwork != null ? var.common_params.host_project != null ? "https://www.googleapis.com/compute/v1/projects/${var.common_params.host_project}/regions/${var.common_params.region}/subnetworks/${var.common_params.subnetwork}" : "https://www.googleapis.com/compute/v1/projects/${var.common_params.project}/regions/${var.common_params.region}/subnetworks/${var.common_params.subnetwork}" : null
  num_workers                 = var.common_params.num_workers
  project                     = var.common_params.project
  region                      = var.common_params.region

  labels = {
    "migration_id" = "${random_pet.job_prefixes[count.index].id}-${var.common_params.run_id}"
  }
}