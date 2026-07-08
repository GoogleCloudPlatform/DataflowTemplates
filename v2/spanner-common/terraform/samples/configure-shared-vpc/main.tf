# A service project gains access to network resources provided by its
# associated host project.
resource "google_compute_shared_vpc_host_project" "host" {
  project = var.common_params.host_project
}

resource "google_compute_shared_vpc_service_project" "shared_vpc" {
  host_project    = google_compute_shared_vpc_host_project.host.project
  service_project = var.common_params.service_project
}

# Grant service project's Datastream service account permissions to deploy
# a private connectivity link in the host project.
resource "google_project_iam_member" "datastream_service_account_network_admin" {
  depends_on = [google_project_service.enabled_apis_service_project]
  project    = var.common_params.host_project
  role       = "roles/compute.networkAdmin"
  member     = "serviceAccount:service-${data.google_project.service_project.number}@gcp-sa-datastream.iam.gserviceaccount.com"
}

# Grant service account's Dataflow service account permissions to use the
# shared VPC since it has to launch VMs inside it.
resource "google_project_iam_member" "dataflow_service_account_compute_user" {
  depends_on = [google_project_service.enabled_apis_service_project]
  project    = var.common_params.host_project
  role       = "roles/compute.networkUser"
  member     = "serviceAccount:service-${data.google_project.service_project.number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

# Grant the service account running the Dataflow job in the service account
# permissions to perform cross project writes to Spanner. By default, this
# configures the default compute engine service account of the service project
resource "google_project_iam_member" "compute_engine_service_account_spanner_database_admin" {
  depends_on = [google_project_service.enabled_apis_service_project]
  project    = var.common_params.host_project
  role       = "roles/spanner.databaseAdmin"
  member     = var.common_params.service_project_service_account != null ? "serviceAccount:${var.common_params.service_project_service_account}" : "serviceAccount:${data.google_project.service_project.number}-compute@developer.gserviceaccount.com"
}