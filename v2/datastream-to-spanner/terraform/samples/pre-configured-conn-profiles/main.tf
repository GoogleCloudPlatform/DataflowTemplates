resource "random_pet" "migration_id" {
  prefix = "smt"
}

locals {
  migration_id = var.common_params.migration_id != null ? var.common_params.migration_id : random_pet.migration_id.id
}

# Pub/Sub Topic for Datastream
resource "google_pubsub_topic" "datastream_topic" {
  name    = "${local.migration_id}-${var.datastream_params.pubsub_topic_name}"
  project = var.common_params.project
  labels = {
    "migration_id" = random_pet.migration_id.id
  }
}

# Configure permissions to publish Pub/Sub notifications
resource "google_pubsub_topic_iam_member" "gcs_publisher_role" {
  count = var.common_params.add_policies_to_service_account ? 1 : 0
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_topic.datastream_topic
  ]
  topic  = google_pubsub_topic.datastream_topic.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

# Pub/Sub Notification on GCS Bucket
resource "google_storage_notification" "bucket_notification" {
  depends_on = [
    google_project_service.enabled_apis, google_pubsub_topic.datastream_topic
  ] # Create a bucket notification using the created pubsub topic.
  bucket             = var.datastream_params.target_gcs_bucket_name
  object_name_prefix = var.datastream_params.stream_prefix_path
  payload_format     = "JSON_API_V1"
  topic              = google_pubsub_topic.datastream_topic.id
  event_types        = ["OBJECT_FINALIZE"]
}

# Pub/Sub Subscription for the created notification
resource "google_pubsub_subscription" "datastream_subscription" {
  depends_on = [
    google_project_service.enabled_apis,
    google_storage_notification.bucket_notification
  ] # Create the subscription once the notification is created.
  name  = "${google_pubsub_topic.datastream_topic.name}-sub"
  topic = google_pubsub_topic.datastream_topic.id
  labels = {
    "migration_id" = random_pet.migration_id.id
  }
}

# Datastream Stream (MySQL to GCS)
resource "google_datastream_stream" "mysql_to_gcs" {
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_subscription.datastream_subscription
  ]
  # Create the stream once the source and target profiles are created along with the subscription.
  stream_id     = "${local.migration_id}-${var.datastream_params.stream_id}"
  location      = var.common_params.region
  display_name  = "${local.migration_id}-${var.datastream_params.stream_id}"
  desired_state = "RUNNING"
  backfill_all {
  }

  source_config {
    source_connection_profile = "projects/${var.common_params.project}/locations/${var.common_params.region}/connectionProfiles/${var.datastream_params.source_connection_profile_id}"

    mysql_source_config {
      max_concurrent_cdc_tasks      = var.datastream_params.max_concurrent_cdc_tasks
      max_concurrent_backfill_tasks = var.datastream_params.max_concurrent_backfill_tasks
      include_objects {
        dynamic "mysql_databases" {
          for_each = var.datastream_params.mysql_databases
          content {
            database = mysql_databases.value.database
            dynamic "mysql_tables" {
              for_each = mysql_databases.value.tables != null ? mysql_databases.value.tables : []
              # Handle optional tables
              content {
                table = mysql_tables.value
              }
            }
          }
        }
      }
    }
  }

  destination_config {
    destination_connection_profile = "projects/${var.common_params.project}/locations/${var.common_params.region}/connectionProfiles/${var.datastream_params.target_connection_profile_id}"
    gcs_destination_config {
      path = var.datastream_params.stream_prefix_path
      avro_file_format {
      }
    }
  }
  labels = {
    "migration_id" = random_pet.migration_id.id
  }
}

# Add roles to the service account that will run Dataflow for live migration
resource "google_project_iam_member" "live_migration_roles" {
  for_each = var.common_params.add_policies_to_service_account ? toset([
    "roles/viewer",
    "roles/storage.objectAdmin",
    "roles/datastream.viewer",
    "roles/dataflow.worker",
    "roles/dataflow.admin",
    "roles/pubsub.subscriber",
    "roles/pubsub.viewer",
    "roles/spanner.databaseAdmin",
    "roles/monitoring.metricWriter",
    "roles/cloudprofiler.agent"
  ]) : toset([])

  project = data.google_project.project.id
  role    = each.key
  member  = var.dataflow_params.runner_params.service_account_email != null ? "serviceAccount:${var.dataflow_params.runner_params.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}


# Dataflow Flex Template Job (for CDC to Spanner)
resource "google_dataflow_flex_template_job" "live_migration_job" {
  depends_on = [
    google_project_service.enabled_apis, google_datastream_stream.mysql_to_gcs, google_project_iam_member.live_migration_roles
  ] # Launch the template once the stream is created.
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Cloud_Datastream_to_Spanner"

  # Parameters from Dataflow Template
  parameters = {
    inputFileFormat                 = "avro"
    inputFilePattern                = "gs://replaced-by-pubsub-notification"
    sessionFilePath                 = var.dataflow_params.template_params.session_file_path
    instanceId                      = var.dataflow_params.template_params.spanner_instance_id
    databaseId                      = var.dataflow_params.template_params.spanner_database_id
    projectId                       = var.common_params.project
    spannerHost                     = var.dataflow_params.template_params.spanner_host
    gcsPubSubSubscription           = google_pubsub_subscription.datastream_subscription.id
    streamName                      = google_datastream_stream.mysql_to_gcs.id
    shadowTablePrefix               = var.dataflow_params.template_params.shadow_table_prefix
    shouldCreateShadowTables        = tostring(var.dataflow_params.template_params.create_shadow_tables)
    rfcStartDateTime                = var.dataflow_params.template_params.rfc_start_date_time
    fileReadConcurrency             = tostring(var.dataflow_params.template_params.file_read_concurrency)
    deadLetterQueueDirectory        = "gs://${var.datastream_params.target_gcs_bucket_name}/dlq"
    dlqRetryMinutes                 = tostring(var.dataflow_params.template_params.dlq_retry_minutes)
    dlqMaxRetryCount                = tostring(var.dataflow_params.template_params.dlq_max_retry_count)
    dataStreamRootUrl               = var.dataflow_params.template_params.datastream_root_url
    datastreamSourceType            = var.dataflow_params.template_params.datastream_source_type
    roundJsonDecimals               = tostring(var.dataflow_params.template_params.round_json_decimals)
    runMode                         = var.dataflow_params.template_params.run_mode
    transformationContextFilePath   = var.dataflow_params.template_params.transformation_context_file_path
    directoryWatchDurationInMinutes = tostring(var.dataflow_params.template_params.directory_watch_duration_in_minutes)
    spannerPriority                 = var.dataflow_params.template_params.spanner_priority
    dlqGcsPubSubSubscription        = var.dataflow_params.template_params.dlq_gcs_pub_sub_subscription
  }

  # Additional Job Configurations
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
  labels = {
    "migration_id" = random_pet.migration_id.id
  }
}

