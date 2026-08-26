provider "google" {
  project = var.project
}

provider "google-beta" {
  project = var.project
}

data "google_project" "project" {
  project_id = var.project
}

data "google_compute_default_service_account" "gce_account" {
  project = var.project
}

# Add roles to the service account that will run Dataflow for data validation
resource "google_project_iam_member" "dataflow_roles" {
  for_each = var.add_policies_to_service_account ? toset([
    "roles/dataflow.worker",
    "roles/spanner.databaseReader",
    "roles/storage.objectViewer",
    "roles/bigquery.dataEditor"
  ]) : toset([])
  project = data.google_project.project.id
  role    = each.key
  member  = var.service_account_email != null ? "serviceAccount:${var.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}

# Define the Dataflow Flex Template job for GCS to Spanner Data Validation
resource "google_dataflow_flex_template_job" "gcs_spanner_dv_job" {
  provider                = google-beta
  name                    = var.job_name
  project                 = var.project
  region                  = var.region
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/GCS_Spanner_Data_Validator"
  
  parameters = {
    gcsInputDirectory              = var.gcs_input_directory
    projectId                      = var.spanner_project_id != null ? var.spanner_project_id : var.project
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
  }

  service_account_email  = var.service_account_email
  network                = var.network != null ? "projects/${var.project}/global/networks/${var.network}" : null
  subnetwork             = var.subnetwork != null ? "https://www.googleapis.com/compute/v1/projects/${var.project}/regions/${var.region}/subnetworks/${var.subnetwork}" : null
  machine_type           = var.machine_type
  max_workers            = var.max_workers
  additional_experiments = var.additional_experiments

  depends_on = [google_project_iam_member.dataflow_roles]
}
