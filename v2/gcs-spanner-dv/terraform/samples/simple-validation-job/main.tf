locals {
  effective_sa_email = (var.dataflow_params.runner_params.service_account_email != null && var.dataflow_params.runner_params.service_account_email != "") ? var.dataflow_params.runner_params.service_account_email : data.google_compute_default_service_account.gce_account.email

  # Network resolution (handling Shared VPC structures)
  network_project = (var.common_params.host_project != null && var.common_params.host_project != "") ? var.common_params.host_project : var.common_params.project
  network_uri     = (var.dataflow_params.runner_params.network != null && var.dataflow_params.runner_params.network != "") ? "projects/${local.network_project}/global/networks/${var.dataflow_params.runner_params.network}" : null
  subnetwork_uri  = (var.dataflow_params.runner_params.subnetwork != null && var.dataflow_params.runner_params.subnetwork != "") ? "https://www.googleapis.com/compute/v1/projects/${local.network_project}/regions/${var.common_params.region}/subnetworks/${var.dataflow_params.runner_params.subnetwork}" : null

  # Spanner project resolution
  spanner_project_id = (var.dataflow_params.template_params.spanner_project_id != null && var.dataflow_params.template_params.spanner_project_id != "") ? var.dataflow_params.template_params.spanner_project_id : var.common_params.project
}

# upload local session file to the working GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  count        = var.dataflow_params.template_params.local_session_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  
  lifecycle {
    precondition {
      condition     = var.dataflow_params.template_params.working_directory_bucket != null
      error_message = "You must provide a working_directory_bucket in template_params when uploading a local_session_file_path."
    }
  }

  name         = "${var.dataflow_params.template_params.working_directory_prefix}/session.json"
  source       = var.dataflow_params.template_params.local_session_file_path
  content_type = "application/json"
  bucket       = var.dataflow_params.template_params.working_directory_bucket
}

# Add roles to the service account that will run Dataflow for data validation
resource "google_project_iam_member" "dataflow_roles" {
  for_each = var.common_params.add_policies_to_service_account ? toset([
    "roles/dataflow.worker",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/monitoring.metricWriter",
    "roles/cloudprofiler.agent"
  ]) : toset([])

  project = var.common_params.project
  role    = each.key
  member  = "serviceAccount:${local.effective_sa_email}"
}

resource "google_project_iam_member" "spanner_reader_role" {
  count   = var.common_params.add_policies_to_service_account ? 1 : 0
  project = local.spanner_project_id
  role    = "roles/spanner.databaseReader"
  member  = "serviceAccount:${local.effective_sa_email}"
}

# Define the Dataflow Flex Template job for GCS to Spanner Data Validation
resource "google_dataflow_flex_template_job" "gcs_spanner_dv_job" {
  provider                = google-beta
  name                    = var.dataflow_params.runner_params.job_name
  project                 = var.common_params.project
  region                  = var.common_params.region
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/GCS_Spanner_Data_Validator"

  parameters = {
    for k, v in {
      gcsInputDirectory              = var.dataflow_params.template_params.gcs_input_directory
      projectId                      = local.spanner_project_id
      instanceId                     = var.dataflow_params.template_params.instance_id
      databaseId                     = var.dataflow_params.template_params.database_id
      bigQueryDataset                = var.dataflow_params.template_params.bigquery_dataset
      spannerHost                    = var.dataflow_params.template_params.spanner_host
      spannerPriority                = var.dataflow_params.template_params.spanner_priority
      sessionFilePath                = var.dataflow_params.template_params.local_session_file_path != null ? "gs://${var.dataflow_params.template_params.working_directory_bucket}/${var.dataflow_params.template_params.working_directory_prefix}/session.json" : var.dataflow_params.template_params.session_file_path
      schemaOverridesFilePath        = var.dataflow_params.template_params.schema_overrides_file_path
      tableOverrides                 = var.dataflow_params.template_params.table_overrides
      columnOverrides                = var.dataflow_params.template_params.column_overrides
      runId                          = var.dataflow_params.template_params.run_id
      transformationJarPath          = var.dataflow_params.template_params.transformation_jar_path
      transformationClassName        = var.dataflow_params.template_params.transformation_class_name
      transformationCustomParameters = var.dataflow_params.template_params.transformation_custom_parameters
    } : k => v if v != null && v != ""
  }

  service_account_email       = local.effective_sa_email
  network                     = local.network_uri
  subnetwork                  = local.subnetwork_uri
  kms_key_name                = var.dataflow_params.runner_params.kms_key_name != "" ? var.dataflow_params.runner_params.kms_key_name : null
  additional_pipeline_options = var.dataflow_params.runner_params.additional_pipeline_options
  machine_type                = var.dataflow_params.runner_params.machine_type
  max_workers                 = var.dataflow_params.runner_params.max_workers
  additional_experiments      = var.dataflow_params.runner_params.additional_experiments
  launcher_machine_type       = var.dataflow_params.runner_params.launcher_machine_type
  ip_configuration            = var.dataflow_params.runner_params.ip_configuration
  num_workers                 = var.dataflow_params.runner_params.num_workers
  labels                      = merge(var.dataflow_params.runner_params.labels, {
    "migration_id" = var.dataflow_params.runner_params.job_name
  })

  depends_on = [
    google_project_iam_member.dataflow_roles,
    google_project_iam_member.spanner_reader_role,
    google_storage_bucket_object.session_file_object
  ]
}
