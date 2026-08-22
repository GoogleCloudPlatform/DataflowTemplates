# Add roles to the service account that will run Dataflow for validation
resource "google_project_iam_member" "validation_roles" {
  for_each = var.add_policies_to_service_account ? toset([
    "roles/viewer",
    "roles/storage.objectViewer",
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/spanner.databaseReader",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/monitoring.metricWriter",
    "roles/cloudprofiler.agent"
  ]) : toset([])
  project = data.google_project.project.id
  role    = each.key
  member  = var.service_account_email != null ? "serviceAccount:${var.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}

resource "google_dataflow_flex_template_job" "generated" {
  depends_on = [
    google_project_service.enabled_apis
  ]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_Spanner_Data_Validator"

  parameters = {
    gcsInputDirectory       = var.gcs_input_directory
    projectId               = var.spanner_project_id
    spannerHost             = var.spanner_host
    instanceId              = var.instance_id
    databaseId              = var.database_id
    spannerPriority         = var.spanner_priority
    sessionFilePath         = var.session_file_path
    schemaOverridesFilePath = var.schema_overrides_file_path
    tableOverrides          = var.table_overrides
    columnOverrides         = var.column_overrides
    bigQueryDataset         = var.bigquery_dataset
    runId                   = var.run_id
  }

  service_account_email       = var.service_account_email
  additional_experiments      = var.additional_experiments
  additional_pipeline_options = var.additional_pipeline_options
  launcher_machine_type       = var.launcher_machine_type
  machine_type                = var.machine_type
  max_workers                 = var.max_workers
  name                        = var.job_name
  ip_configuration            = var.ip_configuration
  network                     = var.network != null ? var.host_project != null ? "projects/${var.host_project}/global/networks/${var.network}" : "projects/${var.project}/global/networks/${var.network}" : null
  subnetwork                  = var.subnetwork != null ? var.host_project != null ? "https://www.googleapis.com/compute/v1/projects/${var.host_project}/regions/${var.region}/subnetworks/${var.subnetwork}" : "https://www.googleapis.com/compute/v1/projects/${var.project}/regions/${var.region}/subnetworks/${var.subnetwork}" : null
  num_workers                 = var.num_workers
  project                     = var.project
  region                      = var.region
}
