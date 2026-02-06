# Add roles to the service account that will run Dataflow for bulk migration
resource "google_project_iam_member" "data_validation_roles" {
  for_each = var.add_policies_to_service_account ? toset([
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
  member  = var.service_account_email != null ? "serviceAccount:${var.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}

resource "google_dataflow_flex_template_job" "generated" {
  depends_on = [
    google_project_service.enabled_apis
  ]
  provider                = google-beta
  container_spec_gcs_path = "gs://manit-testing-ck/templates/flex/GCS_Spanner_DV"

  parameters = {
    gcsInputDirectory             = var.gcs_input_directory
    instanceId                    = var.instance_id
    databaseId                    = var.database_id
    spannerPriority               = var.spanner_priority
    spannerHost                   = var.spanner_host
    bigQueryDataset               = var.big_query_dataset
  }

  service_account_email       = var.service_account_email
  additional_experiments      = var.additional_experiments
  additional_pipeline_options = var.additional_pipeline_options
  launcher_machine_type       = var.launcher_machine_type
  machine_type                = var.machine_type
  max_workers                 = var.max_workers
  name                        = var.job_name
  num_workers                 = var.num_workers
  project                     = var.project
  region                      = var.region

  labels = {
    "migration_id" = var.job_name
  }
}