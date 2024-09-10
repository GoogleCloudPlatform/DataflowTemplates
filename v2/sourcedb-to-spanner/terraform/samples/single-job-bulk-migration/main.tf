# upload local session file to the working GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  count        = var.local_session_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "${var.working_directory_prefix}/session.json"
  source       = var.local_session_file_path
  content_type = "application/json"
  bucket       = var.working_directory_bucket
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
    projectId                      = var.project_id
    spannerHost                    = var.spanner_host
    sessionFilePath                = var.local_session_file_path != null ? "gs://${var.working_directory_bucket}/${var.working_directory_prefix}/session.json" : null
    outputDirectory                = "gs://${var.working_directory_bucket}/${var.working_directory_prefix}/output/"
    transformationJarPath          = var.transformation_jar_path
    transformationClassName        = var.transformation_class_name
    transformationCustomParameters = var.transformation_custom_parameters
    defaultSdkHarnessLogLevel      = var.default_log_level
  }

  service_account_email  = var.service_account_email
  additional_experiments = var.additional_experiments
  launcher_machine_type  = var.launcher_machine_type
  machine_type           = var.machine_type
  max_workers            = var.max_workers
  name                   = var.job_name
  ip_configuration       = var.ip_configuration
  network                = var.network
  subnetwork             = var.subnetwork
  num_workers            = var.num_workers
  project                = var.project
  region                 = var.region

  labels = {
    "migration_id" = var.job_name
  }
}