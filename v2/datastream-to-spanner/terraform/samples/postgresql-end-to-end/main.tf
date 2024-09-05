resource "random_pet" "migration_id" {
  prefix = "smt"
}

locals {
  migration_id = var.common_params.migration_id != null ? var.common_params.migration_id : random_pet.migration_id.id
}

# Setup network firewalls for datastream if creating a private connection.
resource "google_compute_firewall" "allow-datastream" {
  depends_on  = [google_project_service.enabled_apis]
  count       = var.datastream_params.create_firewall_rule == true ? 1 : 0
  project     = var.common_params.host_project != null ? var.common_params.host_project : var.common_params.project
  name        = "allow-datastream"
  network     = var.common_params.host_project != null ? "projects/${var.common_params.host_project}/global/networks/${var.datastream_params.private_connectivity.vpc_name}" : "projects/${var.common_params.project}/global/networks/${var.datastream_params.private_connectivity.vpc_name}"
  description = "Allow traffic from private connectivity endpoint of Datastream"

  allow {
    protocol = "tcp"
    ports    = ["3306"]
  }
  source_ranges      = [var.datastream_params.private_connectivity.range]
  target_tags        = var.datastream_params.firewall_rule_target_tags != null ? var.datastream_params.firewall_rule_target_tags : []
  destination_ranges = var.datastream_params.firewall_rule_target_ranges != null ? var.datastream_params.firewall_rule_target_ranges : []

}

# Create a private connectivity configuration if needed.
resource "google_datastream_private_connection" "datastream_private_connection" {
  depends_on            = [google_project_service.enabled_apis]
  count                 = var.datastream_params.private_connectivity != null ? 1 : 0
  display_name          = "${local.migration_id}-${var.datastream_params.private_connectivity.private_connectivity_id}"
  location              = var.common_params.region
  private_connection_id = "${local.migration_id}-${var.datastream_params.private_connectivity.vpc_name}"

  labels = {
    "migration_id" = local.migration_id
  }

  vpc_peering_config {
    vpc    = "projects/${var.common_params.project}/global/networks/${var.datastream_params.private_connectivity.vpc_name}"
    subnet = var.datastream_params.private_connectivity.range
  }
}

# PostgreSQL Source Connection Profile
resource "google_datastream_connection_profile" "source_postgresql" {
  depends_on            = [google_project_service.enabled_apis]
  display_name          = "${local.migration_id}-${var.datastream_params.source_connection_profile_id}"
  location              = var.common_params.region
  connection_profile_id = "${local.migration_id}-${var.datastream_params.source_connection_profile_id}"

  postgresql_profile {
    hostname = var.datastream_params.postgresql_host
    username = var.datastream_params.postgresql_username
    password = var.datastream_params.postgresql_password
    port     = var.datastream_params.postgresql_port
    database = var.datastream_params.postgresql_database.database
  }

  # Dynamically add private_connectivity block based on private connectivity
  # resource creation.
  dynamic "private_connectivity" {
    for_each = google_datastream_private_connection.datastream_private_connection.*.id
    content {
      private_connection = private_connectivity.value
    }
  }

  # If an existing private connectivity configuration is provided, use that.
  # If nothing is specified on private connectivity, IP allowlisting is
  # assumed.
  dynamic "private_connectivity" {
    for_each = var.datastream_params.private_connectivity_id != null ? [1] : []
    content {
      private_connection = "projects/${var.common_params.project}/locations/${var.common_params.region}/privateConnections/${var.datastream_params.private_connectivity_id}"
    }
  }

  labels = {
    "migration_id" = local.migration_id
  }
}

# GCS Bucket for Datastream
resource "google_storage_bucket" "datastream_bucket" {
  depends_on                  = [google_project_service.enabled_apis]
  name                        = "${local.migration_id}-${var.datastream_params.gcs_bucket_name}"
  location                    = var.common_params.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels = {
    "migration_id" = local.migration_id
  }
}

# upload local session file to the created GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  count        = var.dataflow_params.template_params.local_session_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "session.json"
  source       = var.dataflow_params.template_params.local_session_file_path
  content_type = "application/json"
  bucket       = google_storage_bucket.datastream_bucket.id
}

# upload local schema overrides file to the created GCS bucket
resource "google_storage_bucket_object" "schema_overrides_file_object" {
  count        = var.dataflow_params.template_params.local_schema_overrides_file_path != null ? 1 : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "schema-overrides.json"
  source       = var.dataflow_params.template_params.local_schema_overrides_file_path
  content_type = "application/json"
  bucket       = google_storage_bucket.datastream_bucket.id
}

# Pub/Sub Topic for Datastream
resource "google_pubsub_topic" "datastream_topic" {
  depends_on = [google_project_service.enabled_apis]
  name       = "${local.migration_id}-${var.datastream_params.pubsub_topic_name}"
  project    = var.common_params.project
  labels = {
    "migration_id" = local.migration_id
  }
}

# Configure permissions to publish Pub/Sub notifications
resource "google_pubsub_topic_iam_member" "gcs_publisher_role" {
  count      = var.common_params.add_policies_to_service_account ? 1 : 0
  depends_on = [google_project_service.enabled_apis]
  topic      = google_pubsub_topic.datastream_topic.name
  role       = "roles/pubsub.publisher"
  member     = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

# Pub/Sub Notification on GCS Bucket
resource "google_storage_notification" "bucket_notification" {
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_topic_iam_member.gcs_publisher_role
  ] # Create a bucket notification using the created pubsub topic.
  bucket             = google_storage_bucket.datastream_bucket.name
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
    "migration_id" = local.migration_id
  }
}

# GCS Target Connection Profile
resource "google_datastream_connection_profile" "target_gcs" {
  depends_on = [
    google_project_service.enabled_apis,
    google_storage_notification.bucket_notification
  ] # Create the target profile once the bucket and its notification is created.
  display_name          = "${local.migration_id}-${var.datastream_params.target_connection_profile_id}"
  location              = var.common_params.region
  connection_profile_id = "${local.migration_id}-${var.datastream_params.target_connection_profile_id}"

  gcs_profile {
    bucket    = google_storage_bucket.datastream_bucket.name
    root_path = var.datastream_params.gcs_root_path
  }
  labels = {
    "migration_id" = local.migration_id
  }
}

# Datastream Stream (PostgreSQL to GCS)
resource "google_datastream_stream" "postgresql_to_gcs" {
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_subscription.datastream_subscription
  ]
  # Create the stream once the source and target profiles are created along with the subscription.
  stream_id     = "${local.migration_id}-${var.datastream_params.stream_id}"
  location      = var.common_params.region
  display_name  = "${local.migration_id}-${var.datastream_params.stream_id}"
  desired_state = "RUNNING"
  dynamic "backfill_all" {
    for_each = var.datastream_params.enable_backfill ? [1] : []
    content {}
  }

  dynamic "backfill_none" {
    for_each = var.datastream_params.enable_backfill ? [] : [1]
    content {}
  }

  source_config {
    source_connection_profile = google_datastream_connection_profile.source_postgresql.id

    postgresql_source_config {
      publication      = var.datastream_params.postgresql_publication
      replication_slot = var.datastream_params.postgresql_replication_slot
      # max_concurrent_cdc_tasks      = var.datastream_params.max_concurrent_cdc_tasks
      max_concurrent_backfill_tasks = var.datastream_params.max_concurrent_backfill_tasks
      include_objects {
        dynamic "postgresql_schemas" {
          for_each = var.datastream_params.postgresql_database.schemas
          content {
            schema = postgresql_schemas.value.schema_name
            dynamic "postgresql_tables" {
              for_each = postgresql_schemas.value.tables != null ? postgresql_schemas.value.tables : []
              content {
                table = postgresql_tables.value
              }
            }
          }
        }
      }
    }
  }

  destination_config {
    destination_connection_profile = google_datastream_connection_profile.target_gcs.id
    gcs_destination_config {
      path             = var.datastream_params.stream_prefix_path
      file_rotation_mb = 5
      avro_file_format {
      }
    }
  }
  labels = {
    "migration_id" = local.migration_id
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
  count = var.dataflow_params.skip_dataflow ? 0 : 1
  depends_on = [
    google_project_service.enabled_apis, google_project_iam_member.live_migration_roles
  ] # Launch the template once the stream is created.
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Cloud_Datastream_to_Spanner"

  # Parameters from Dataflow Template
  parameters = {
    inputFileFormat                 = "avro"
    inputFilePattern                = "gs://replaced-by-pubsub-notification"
    sessionFilePath                 = var.dataflow_params.template_params.local_session_file_path != null ? "gs://${google_storage_bucket_object.session_file_object[0].bucket}/${google_storage_bucket_object.session_file_object[0].name}" : null
    instanceId                      = var.dataflow_params.template_params.spanner_instance_id
    databaseId                      = var.dataflow_params.template_params.spanner_database_id
    projectId                       = var.dataflow_params.template_params.spanner_project_id != null ? var.dataflow_params.template_params.spanner_project_id : var.common_params.project
    spannerHost                     = var.dataflow_params.template_params.spanner_host
    gcsPubSubSubscription           = google_pubsub_subscription.datastream_subscription.id
    streamName                      = google_datastream_stream.postgresql_to_gcs.id
    shadowTablePrefix               = var.dataflow_params.template_params.shadow_table_prefix
    shouldCreateShadowTables        = tostring(var.dataflow_params.template_params.create_shadow_tables)
    rfcStartDateTime                = var.dataflow_params.template_params.rfc_start_date_time
    fileReadConcurrency             = tostring(var.dataflow_params.template_params.file_read_concurrency)
    deadLetterQueueDirectory        = "${google_storage_bucket.datastream_bucket.url}/dlq"
    dlqRetryMinutes                 = tostring(var.dataflow_params.template_params.dlq_retry_minutes)
    dlqMaxRetryCount                = tostring(var.dataflow_params.template_params.dlq_max_retry_count)
    dataStreamRootUrl               = var.dataflow_params.template_params.datastream_root_url
    datastreamSourceType            = var.dataflow_params.template_params.datastream_source_type
    roundJsonDecimals               = tostring(var.dataflow_params.template_params.round_json_decimals)
    runMode                         = var.dataflow_params.template_params.run_mode
    directoryWatchDurationInMinutes = tostring(var.dataflow_params.template_params.directory_watch_duration_in_minutes)
    spannerPriority                 = var.dataflow_params.template_params.spanner_priority
    dlqGcsPubSubSubscription        = var.dataflow_params.template_params.dlq_gcs_pub_sub_subscription
    transformationJarPath           = var.dataflow_params.template_params.transformation_jar_path
    transformationClassName         = var.dataflow_params.template_params.transformation_class_name
    transformationCustomParameters  = var.dataflow_params.template_params.transformation_custom_parameters
    filteredEventsDirectory         = var.dataflow_params.template_params.filtered_events_directory
    tableOverrides                  = var.dataflow_params.template_params.table_overrides
    columnOverrides                 = var.dataflow_params.template_params.column_overrides
    schemaOverridesFilePath         = var.dataflow_params.template_params.local_schema_overrides_file_path != null ? "gs://${google_storage_bucket_object.schema_overrides_file_object[0].bucket}/${google_storage_bucket_object.schema_overrides_file_object[0].name}" : null
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
  subnetwork                   = var.dataflow_params.runner_params.subnetwork != null ? var.common_params.host_project != null ? "https://www.googleapis.com/compute/v1/projects/${var.common_params.host_project}/regions/${var.common_params.region}/subnetworks/${var.dataflow_params.runner_params.subnetwork}" : "https://www.googleapis.com/compute/v1/projects/${var.common_params.project}/regions/${var.common_params.region}/subnetworks/${var.dataflow_params.runner_params.subnetwork}" : null
  temp_location                = var.dataflow_params.runner_params.temp_location
  on_delete                    = var.dataflow_params.runner_params.on_delete
  region                       = var.common_params.region
  ip_configuration             = var.dataflow_params.runner_params.ip_configuration
  labels = {
    "migration_id" = local.migration_id
  }
}

