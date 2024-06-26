provider "google-beta" {
  project = var.common_params.project
  region  = var.common_params.region
}

resource "google_project_service" "required" {
  service            = "dataflow.googleapis.com"
  project            = var.common_params.project
  disable_on_destroy = false
}

resource "random_pet" "job_name_suffix" {
  count = length(var.jobs)
}

resource "google_dataflow_flex_template_job" "generated" {
  count                   = length(var.jobs)
  depends_on              = [google_project_service.required]
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Sourcedb_to_Spanner_Flex"

  parameters = {
    jdbcDriverJars      = var.common_params.jdbcDriverJars
    jdbcDriverClassName = var.common_params.jdbcDriverClassName
    sourceDbURL         = var.jobs[count.index].sourceDbURL
    username            = var.jobs[count.index].username
    password            = var.jobs[count.index].password
    tables              = var.jobs[count.index].tables
    numPartitions       = tostring(var.jobs[count.index].numPartitions)
    instanceId          = var.jobs[count.index].instanceId
    databaseId          = var.jobs[count.index].databaseId
    projectId           = var.common_params.projectId
    spannerHost         = var.common_params.spannerHost
    maxConnections      = tostring(var.jobs[count.index].maxConnections)
    sessionFilePath     = var.common_params.sessionFilePath
    DLQDirectory        = var.jobs[count.index].DLQDirectory
    disabledAlgorithms  = var.common_params.disabledAlgorithms
    extraFilesToStage   = var.common_params.extraFilesToStage
    defaultLogLevel     = var.jobs[count.index].defaultLogLevel
  }

  additional_experiments       = var.common_params.additional_experiments
  autoscaling_algorithm        = var.common_params.autoscaling_algorithm
  enable_streaming_engine      = var.common_params.enable_streaming_engine
  ip_configuration             = var.jobs[count.index].ip_configuration
  kms_key_name                 = var.jobs[count.index].kms_key_name
  labels                       = var.jobs[count.index].labels
  launcher_machine_type        = var.jobs[count.index].launcher_machine_type
  machine_type                 = var.jobs[count.index].machine_type
  max_workers                  = var.jobs[count.index].max_workers
  name                         = "${var.jobs[count.index].name}-${random_pet.job_name_suffix[count.index].id}"
  network                      = var.common_params.network
  subnetwork                   = var.common_params.subnetwork
  num_workers                  = var.jobs[count.index].num_workers
  on_delete                    = var.common_params.on_delete
  project                      = var.common_params.project
  region                       = var.common_params.region
  sdk_container_image          = var.common_params.sdk_container_image
  service_account_email        = var.common_params.service_account_email
  skip_wait_on_job_termination = var.common_params.skip_wait_on_job_termination
  staging_location             = var.common_params.staging_location
  temp_location                = var.common_params.temp_location
}