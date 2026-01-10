# upload local session file to the working GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  count        = var.local_session_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "${var.working_directory_prefix}/session.json"
  source       = var.local_session_file_path
  content_type = "application/json"
  bucket       = var.working_directory_bucket
}

# Setup network firewalls rules to enable Dataflow access to source.
resource "google_compute_firewall" "allow-dataflow-to-source" {
  depends_on  = [google_project_service.enabled_apis]
  project     = var.host_project != null ? var.host_project : var.project
  name        = "allow-dataflow-to-source"
  network     = var.network != null ? var.host_project != null ? "projects/${var.host_project}/global/networks/${var.network}" : "projects/${var.project}/global/networks/${var.network}" : "default"
  description = "Allow traffic from Dataflow to source databases"

  allow {
    protocol = "tcp"
    ports    = ["3306"]
  }
  source_tags = ["dataflow"]
  target_tags = ["databases"]
}

# Add roles to the service account that will run Dataflow for bulk migration
resource "google_project_iam_member" "live_migration_roles" {
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
    google_project_service.enabled_apis,
    google_storage_bucket_object.session_file_object
  ]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Sourcedb_to_Spanner_Flex"

  parameters = {
    jdbcDriverJars                 = var.jdbc_driver_jars
    jdbcDriverClassName            = var.jdbc_driver_class_name
    maxConnections                 = tostring(var.max_connections)
    sourceConfigURL                = var.source_config_url
    username                       = var.username
    password                       = var.password
    numPartitions                  = tostring(var.num_partitions)
    instanceId                     = var.instance_id
    databaseId                     = var.database_id
    projectId                      = var.spanner_project_id
    spannerHost                    = var.spanner_host
    sessionFilePath                = var.local_session_file_path != null ? "gs://${var.working_directory_bucket}/${var.working_directory_prefix}/session.json" : null
    outputDirectory                = "gs://${var.working_directory_bucket}/${var.working_directory_prefix}/output/"
    transformationJarPath          = var.transformation_jar_path
    transformationClassName        = var.transformation_class_name
    transformationCustomParameters = var.transformation_custom_parameters
    defaultSdkHarnessLogLevel      = var.default_log_level
    fetchSize                      = var.fetch_size
    gcsOutputDirectory             = var.gcs_output_directory
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

  labels = {
    "migration_id" = var.job_name
  }
}