resource "random_pet" "migration_id" {
  prefix = "cdc"
}

locals {
  migration_id = var.common_params.migration_id != null ? var.common_params.migration_id : random_pet.migration_id.id
}

# GCS Bucket for templates and configs
resource "google_storage_bucket" "generator_bucket" {
  depends_on                  = [google_project_service.enabled_apis]
  name                        = "${local.migration_id}-cdc-generator"
  location                    = var.common_params.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels = {
    "migration_id" = local.migration_id
  }
}

# Upload local schema config overrides file if provided
resource "google_storage_bucket_object" "schema_config_file" {
  count        = var.dataflow_params.template_params.local_schema_config_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "schema-config.json"
  source       = var.dataflow_params.template_params.local_schema_config_file_path
  content_type = "application/json"
  bucket       = google_storage_bucket.generator_bucket.id
}

# Upload local sink options file if provided
resource "google_storage_bucket_object" "sink_options_file" {
  count        = var.dataflow_params.template_params.local_sink_options_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "sink-options.json"
  source       = var.dataflow_params.template_params.local_sink_options_file_path
  content_type = "application/json"
  bucket       = google_storage_bucket.generator_bucket.id
}

# Add IAM roles to Dataflow Service Account
resource "google_project_iam_member" "dataflow_roles" {
  for_each = var.common_params.add_policies_to_service_account ? toset([
    "roles/viewer",
    "roles/storage.objectAdmin",
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/spanner.databaseUser",
    "roles/monitoring.metricWriter",
    "roles/cloudprofiler.agent"
  ]) : toset([])
  project = data.google_project.project.id
  role    = each.key
  member  = var.dataflow_params.runner_params.service_account_email != null ? "serviceAccount:${var.dataflow_params.runner_params.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}

# Dataflow Flex Template Job (CDC Data Generator)
resource "google_dataflow_flex_template_job" "generator_job" {
  depends_on = [
    google_project_service.enabled_apis,
    google_project_iam_member.dataflow_roles
  ]
  provider                = google-beta
  container_spec_gcs_path = var.dataflow_params.runner_params.container_spec_gcs_path != null ? var.dataflow_params.runner_params.container_spec_gcs_path : "gs://dataflow-templates-${var.common_params.region}/latest/flex/Cdc_Data_Generator"

  parameters = {
    sinkType       = "SPANNER"
    sinkOptions    = var.dataflow_params.template_params.sink_options_gcs_path != null ? var.dataflow_params.template_params.sink_options_gcs_path : "gs://${google_storage_bucket.generator_bucket.name}/sink-options.json"
    batchSize      = tostring(var.dataflow_params.template_params.batch_size)
    insertQps      = tostring(var.dataflow_params.template_params.insert_qps)
    updateQps      = tostring(var.dataflow_params.template_params.update_qps)
    deleteQps      = tostring(var.dataflow_params.template_params.delete_qps)
    updateInterval = tostring(var.dataflow_params.template_params.update_interval)
    deleteInterval = tostring(var.dataflow_params.template_params.delete_interval)
    schemaConfig   = var.dataflow_params.template_params.local_schema_config_file_path != null ? "gs://${google_storage_bucket.generator_bucket.name}/schema-config.json" : null
    dlqDirectory   = var.dataflow_params.template_params.dlq_directory != null ? var.dataflow_params.template_params.dlq_directory : "gs://${google_storage_bucket.generator_bucket.name}/dlq"
    maxParallelism = var.dataflow_params.template_params.max_parallelism != null ? tostring(var.dataflow_params.template_params.max_parallelism) : null
  }

  additional_experiments       = var.dataflow_params.runner_params.additional_experiments
  autoscaling_algorithm        = var.dataflow_params.runner_params.autoscaling_algorithm
  enable_streaming_engine      = var.dataflow_params.runner_params.enable_streaming_engine
  kms_key_name                 = var.dataflow_params.runner_params.kms_key_name
  launcher_machine_type        = var.dataflow_params.runner_params.launcher_machine_type
  machine_type                 = var.dataflow_params.runner_params.machine_type
  max_workers                  = var.dataflow_params.runner_params.max_workers
  name                         = "${local.migration_id}-${var.dataflow_params.runner_params.job_name}"
  network                      = var.dataflow_params.runner_params.network
  num_workers                  = var.dataflow_params.runner_params.num_workers
  sdk_container_image          = var.dataflow_params.runner_params.sdk_container_image
  service_account_email        = var.dataflow_params.runner_params.service_account_email
  skip_wait_on_job_termination = var.dataflow_params.runner_params.skip_wait_on_job_termination
  staging_location             = var.dataflow_params.runner_params.staging_location
  subnetwork                   = var.dataflow_params.runner_params.subnetwork
  temp_location                = var.dataflow_params.runner_params.temp_location
  on_delete                    = var.dataflow_params.runner_params.on_delete
  region                       = var.common_params.region
  ip_configuration             = var.dataflow_params.runner_params.ip_configuration
  labels = merge({
    "migration_id" = local.migration_id
  }, var.dataflow_params.runner_params.labels != null ? var.dataflow_params.runner_params.labels : {})
}