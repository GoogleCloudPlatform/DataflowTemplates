data "google_project" "project" {
  project_id = var.project
  depends_on = [google_project_service.enabled_apis]
}

data "google_compute_default_service_account" "gce_account" {
  project    = var.project
  depends_on = [google_project_service.enabled_apis]
}

locals {
  effective_sa_email = (var.service_account_email != null && var.service_account_email != "") ? var.service_account_email : data.google_compute_default_service_account.gce_account.email
}

# Add roles to the service account that will run Dataflow for data validation
resource "google_project_iam_member" "dataflow_roles" {
  for_each = var.add_policies_to_service_account ? toset([
    "roles/dataflow.worker",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/monitoring.metricWriter",
    "roles/cloudprofiler.agent"
  ]) : toset([])

  project = var.project
  role    = each.key
  member  = "serviceAccount:${local.effective_sa_email}"
}

resource "google_project_iam_member" "spanner_reader_role" {
  count   = var.add_policies_to_service_account ? 1 : 0
  project = var.spanner_project_id != null && var.spanner_project_id != "" ? var.spanner_project_id : var.project
  role    = "roles/spanner.databaseReader"
  member  = "serviceAccount:${local.effective_sa_email}"
}

# Define the Dataflow Flex Template job for GCS to Spanner Data Validation
resource "google_dataflow_flex_template_job" "gcs_spanner_dv_job" {
  provider                = google-beta
  name                    = var.job_name
  project                 = var.project
  region                  = var.region
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_Spanner_Data_Validator"

  parameters = {
    for k, v in {
      gcsInputDirectory              = var.gcs_input_directory
      projectId                      = var.spanner_project_id != null && var.spanner_project_id != "" ? var.spanner_project_id : var.project
      instanceId                     = var.instance_id
      databaseId                     = var.database_id
      bigQueryDataset                = var.bigquery_dataset
      spannerHost                    = var.spanner_host
      spannerPriority                = var.spanner_priority
      sessionFilePath                = var.session_file_path
      schemaOverridesFilePath        = var.schema_overrides_file_path
      tableOverrides                 = var.table_overrides
      columnOverrides                = var.column_overrides
      runId                          = var.run_id
      transformationJarPath          = var.transformation_jar_path
      transformationClassName        = var.transformation_class_name
      transformationCustomParameters = var.transformation_custom_parameters
    } : k => v if v != null && v != ""
  }

  service_account_email  = local.effective_sa_email
  network                = (var.network != null && var.network != "") ? (var.host_project != null && var.host_project != "") ? "projects/${var.host_project}/global/networks/${var.network}" : "projects/${var.project}/global/networks/${var.network}" : null
  subnetwork             = (var.subnetwork != null && var.subnetwork != "") ? (var.host_project != null && var.host_project != "") ? "https://www.googleapis.com/compute/v1/projects/${var.host_project}/regions/${var.region}/subnetworks/${var.subnetwork}" : "https://www.googleapis.com/compute/v1/projects/${var.project}/regions/${var.region}/subnetworks/${var.subnetwork}" : null
  machine_type           = var.machine_type
  max_workers            = var.max_workers
  additional_experiments = var.additional_experiments

  depends_on = [
    google_project_iam_member.dataflow_roles,
    google_project_iam_member.spanner_reader_role
  ]
}
