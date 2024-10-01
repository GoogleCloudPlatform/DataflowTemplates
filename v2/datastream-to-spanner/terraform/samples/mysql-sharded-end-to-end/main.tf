resource "random_pet" "migration_id" {
  count  = length(var.shard_list)
  prefix = "smt"
}
# Create a private connectivity configuration if needed.
resource "google_datastream_private_connection" "datastream_private_connection" {
  depends_on            = [google_project_service.enabled_apis]
  count                 = var.common_params.datastream_params.private_connectivity != null ? 1 : 0
  display_name          = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.common_params.datastream_params.private_connectivity.private_connectivity_id}"
  location              = var.common_params.region
  private_connection_id = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.common_params.datastream_params.private_connectivity.vpc_name}"

  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }

  vpc_peering_config {
    vpc    = var.common_params.host_project != null ? "projects/${var.common_params.host_project}/global/networks/${var.common_params.datastream_params.private_connectivity.vpc_name}" : "projects/${var.common_params.project}/global/networks/${var.common_params.datastream_params.private_connectivity.vpc_name}"
    subnet = var.common_params.datastream_params.private_connectivity.range
  }
}

# MySQL Source Connection Profile
resource "google_datastream_connection_profile" "source_mysql" {
  count                 = length(var.shard_list)
  depends_on            = [google_project_service.enabled_apis]
  display_name          = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.source_connection_profile_id}"
  location              = var.common_params.region
  connection_profile_id = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.source_connection_profile_id}"

  mysql_profile {
    hostname = var.shard_list[count.index].datastream_params.mysql_host
    username = var.shard_list[count.index].datastream_params.mysql_username
    password = var.shard_list[count.index].datastream_params.mysql_password
    port     = var.shard_list[count.index].datastream_params.mysql_port
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
    for_each = var.common_params.datastream_params.private_connectivity_id != null ? [1] : []
    content {
      private_connection = "projects/${var.common_params.project}/locations/${var.common_params.region}/privateConnections/${var.common_params.datastream_params.private_connectivity_id}"
    }
  }

  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }
}

# GCS Bucket for Datastream
resource "google_storage_bucket" "datastream_bucket" {
  count                       = length(var.shard_list)
  depends_on                  = [google_project_service.enabled_apis]
  name                        = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.gcs_bucket_name}"
  location                    = var.common_params.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }
}

# upload local session file to the created GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  count        = var.common_params.dataflow_params.template_params.local_session_file_path != null ? length(var.shard_list) : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "session.json"
  source       = var.common_params.dataflow_params.template_params.local_session_file_path
  content_type = "application/json"
  bucket       = google_storage_bucket.datastream_bucket[count.index].id
}

# upload local schema overrides file to the created GCS bucket
resource "google_storage_bucket_object" "schema_overrides_file_object" {
  count        = var.common_params.dataflow_params.template_params.local_schema_overrides_file_path != null ? length(var.shard_list) : 0
  depends_on   = [google_project_service.enabled_apis]
  name         = "schema-overrides.json"
  source       = var.common_params.dataflow_params.template_params.local_schema_overrides_file_path
  content_type = "application/json"
  bucket       = google_storage_bucket.datastream_bucket[count.index].id
}

# if the transformation context file for the shard is specified, use that, otherwise
# auto-generate transformation context on basis of MySQL host IP and logical
# shard names.
resource "google_storage_bucket_object" "transformation_context_file_object" {
  for_each     = { for idx, shard in var.shard_list : idx => shard }
  depends_on   = [google_project_service.enabled_apis]
  name         = "transformationContext.json"
  content_type = "application/json"
  bucket       = google_storage_bucket.datastream_bucket[each.key].id
  content = (
    each.value.dataflow_params.template_params.local_transformation_context_path == null
    ? jsonencode({
      "SchemaToShardId" : {
        for db in var.common_params.datastream_params.mysql_databases :
        db.database => "${replace(each.value.datastream_params.mysql_host, ".", "-")}-${db.database}"
      }
    })
    : file(each.value.dataflow_params.template_params.local_transformation_context_path)
  )
}

# Pub/Sub Topic for Datastream
resource "google_pubsub_topic" "datastream_topic" {
  count      = length(var.shard_list)
  depends_on = [google_project_service.enabled_apis]
  name       = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.pubsub_topic_name}"
  project    = var.common_params.project
  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }
}

# Configure permissions to publish Pub/Sub notifications
resource "google_pubsub_topic_iam_member" "gcs_publisher_role" {
  count      = var.common_params.add_policies_to_service_account ? length(var.shard_list) : 0
  depends_on = [google_project_service.enabled_apis]
  topic      = google_pubsub_topic.datastream_topic[count.index].name
  role       = "roles/pubsub.publisher"
  member     = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

# Pub/Sub Notification on GCS Bucket
resource "google_storage_notification" "bucket_notification" {
  count = length(var.shard_list)
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_topic_iam_member.gcs_publisher_role
  ] # Create a bucket notification using the created pubsub topic.
  bucket             = google_storage_bucket.datastream_bucket[count.index].name
  object_name_prefix = var.common_params.datastream_params.stream_prefix_path
  payload_format     = "JSON_API_V1"
  topic              = google_pubsub_topic.datastream_topic[count.index].id
  event_types        = ["OBJECT_FINALIZE"]
}

# Pub/Sub Subscription for the created notification
resource "google_pubsub_subscription" "datastream_subscription" {
  count = length(var.shard_list)
  depends_on = [
    google_project_service.enabled_apis,
    google_storage_notification.bucket_notification
  ] # Create the subscription once the notification is created.
  name  = "${google_pubsub_topic.datastream_topic[count.index].name}-sub"
  topic = google_pubsub_topic.datastream_topic[count.index].id
  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }
}

# GCS Target Connection Profile
resource "google_datastream_connection_profile" "target_gcs" {
  count = length(var.shard_list)
  depends_on = [
    google_project_service.enabled_apis,
    google_storage_notification.bucket_notification
  ] # Create the target profile once the bucket and its notification is created.
  display_name          = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.target_connection_profile_id}"
  location              = var.common_params.region
  connection_profile_id = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.target_connection_profile_id}"

  gcs_profile {
    bucket    = google_storage_bucket.datastream_bucket[count.index].name
    root_path = var.shard_list[count.index].datastream_params.gcs_root_path
  }
  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }
}

# Datastream Stream (MySQL to GCS)
resource "google_datastream_stream" "mysql_to_gcs" {
  count = length(var.shard_list)
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_subscription.datastream_subscription
  ]
  # Create the stream once the source and target profiles are created along with the subscription.
  stream_id     = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.stream_id}"
  location      = var.common_params.region
  display_name  = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.shard_list[count.index].datastream_params.stream_id}"
  desired_state = "RUNNING"
  backfill_all {
  }

  source_config {
    source_connection_profile = google_datastream_connection_profile.source_mysql[count.index].id

    mysql_source_config {
      max_concurrent_cdc_tasks      = var.common_params.datastream_params.max_concurrent_cdc_tasks
      max_concurrent_backfill_tasks = var.common_params.datastream_params.max_concurrent_backfill_tasks
      include_objects {
        dynamic "mysql_databases" {
          for_each = var.common_params.datastream_params.mysql_databases
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
    destination_connection_profile = google_datastream_connection_profile.target_gcs[count.index].id
    gcs_destination_config {
      path             = var.common_params.datastream_params.stream_prefix_path
      file_rotation_mb = 5
      avro_file_format {
      }
    }
  }
  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
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
  member  = var.common_params.dataflow_params.runner_params.service_account_email != null ? "serviceAccount:${var.common_params.dataflow_params.runner_params.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}
# Dataflow Flex Template Job (for CDC to Spanner)
resource "google_dataflow_flex_template_job" "live_migration_job" {
  count = length(var.shard_list)
  depends_on = [
    google_project_service.enabled_apis, google_project_iam_member.live_migration_roles
  ] # Launch the template once the stream is created.
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Cloud_Datastream_to_Spanner"

  # Parameters from Dataflow Template
  parameters = {
    inputFileFormat                 = "avro"
    inputFilePattern                = "gs://replaced-by-pubsub-notification"
    sessionFilePath                 = var.common_params.dataflow_params.template_params.local_session_file_path != null ? "gs://${google_storage_bucket_object.session_file_object[count.index].bucket}/${google_storage_bucket_object.session_file_object[count.index].name}" : null
    instanceId                      = var.common_params.dataflow_params.template_params.spanner_instance_id
    databaseId                      = var.common_params.dataflow_params.template_params.spanner_database_id
    projectId                       = var.common_params.dataflow_params.template_params.spanner_project_id != null ? var.common_params.dataflow_params.template_params.spanner_project_id : var.common_params.project
    spannerHost                     = var.common_params.dataflow_params.template_params.spanner_host
    gcsPubSubSubscription           = google_pubsub_subscription.datastream_subscription[count.index].id
    streamName                      = google_datastream_stream.mysql_to_gcs[count.index].id
    shadowTablePrefix               = var.common_params.dataflow_params.template_params.shadow_table_prefix
    shouldCreateShadowTables        = tostring(var.common_params.dataflow_params.template_params.create_shadow_tables)
    rfcStartDateTime                = var.common_params.dataflow_params.template_params.rfc_start_date_time
    fileReadConcurrency             = tostring(var.common_params.dataflow_params.template_params.file_read_concurrency)
    deadLetterQueueDirectory        = "${google_storage_bucket.datastream_bucket[count.index].url}/dlq"
    dlqRetryMinutes                 = tostring(var.common_params.dataflow_params.template_params.dlq_retry_minutes)
    dlqMaxRetryCount                = tostring(var.common_params.dataflow_params.template_params.dlq_max_retry_count)
    dataStreamRootUrl               = var.common_params.dataflow_params.template_params.datastream_root_url
    datastreamSourceType            = var.common_params.dataflow_params.template_params.datastream_source_type
    roundJsonDecimals               = tostring(var.common_params.dataflow_params.template_params.round_json_decimals)
    runMode                         = var.shard_list[count.index].dataflow_params.template_params.run_mode
    transformationContextFilePath   = "gs://${google_storage_bucket_object.transformation_context_file_object[count.index].bucket}/${google_storage_bucket_object.transformation_context_file_object[count.index].name}"
    directoryWatchDurationInMinutes = tostring(var.common_params.dataflow_params.template_params.directory_watch_duration_in_minutes)
    spannerPriority                 = var.common_params.dataflow_params.template_params.spanner_priority
    dlqGcsPubSubSubscription        = var.shard_list[count.index].dataflow_params.template_params.dlq_gcs_pub_sub_subscription
    transformationJarPath           = var.common_params.dataflow_params.template_params.transformation_jar_path
    transformationClassName         = var.common_params.dataflow_params.template_params.transformation_class_name
    transformationCustomParameters  = var.common_params.dataflow_params.template_params.transformation_custom_parameters
    filteredEventsDirectory         = var.common_params.dataflow_params.template_params.filtered_events_directory
    tableOverrides                  = var.common_params.dataflow_params.template_params.table_overrides
    columnOverrides                 = var.common_params.dataflow_params.template_params.column_overrides
    schemaOverridesFilePath         = var.common_params.dataflow_params.template_params.local_session_file_path != null ? "gs://${google_storage_bucket_object.schema_overrides_file_object[count.index].bucket}/${google_storage_bucket_object.schema_overrides_file_object[count.index].name}" : null
  }

  # Additional Job Configurations
  additional_experiments       = var.common_params.dataflow_params.runner_params.additional_experiments
  autoscaling_algorithm        = var.common_params.dataflow_params.runner_params.autoscaling_algorithm
  enable_streaming_engine      = var.common_params.dataflow_params.runner_params.enable_streaming_engine
  kms_key_name                 = var.common_params.dataflow_params.runner_params.kms_key_name
  launcher_machine_type        = var.common_params.dataflow_params.runner_params.launcher_machine_type
  machine_type                 = var.shard_list[count.index].dataflow_params.runner_params.machine_type != null ? var.shard_list[count.index].dataflow_params.runner_params.machine_type : var.common_params.dataflow_params.runner_params.machine_type
  max_workers                  = var.shard_list[count.index].dataflow_params.runner_params.max_workers != null ? var.shard_list[count.index].dataflow_params.runner_params.num_workers : var.common_params.dataflow_params.runner_params.max_workers
  name                         = "${var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id}-${var.common_params.dataflow_params.runner_params.job_name}"
  network                      = var.common_params.dataflow_params.runner_params.network
  num_workers                  = var.shard_list[count.index].dataflow_params.runner_params.num_workers != null ? var.shard_list[count.index].dataflow_params.runner_params.max_workers : var.common_params.dataflow_params.runner_params.num_workers
  sdk_container_image          = var.common_params.dataflow_params.runner_params.sdk_container_image
  service_account_email        = var.common_params.dataflow_params.runner_params.service_account_email
  skip_wait_on_job_termination = var.common_params.dataflow_params.runner_params.skip_wait_on_job_termination
  staging_location             = var.common_params.dataflow_params.runner_params.staging_location
  subnetwork                   = var.common_params.dataflow_params.runner_params.subnetwork != null ? var.common_params.host_project != null ? "https://www.googleapis.com/compute/v1/projects/${var.common_params.host_project}/regions/${var.common_params.region}/subnetworks/${var.common_params.dataflow_params.runner_params.subnetwork}" : "https://www.googleapis.com/compute/v1/projects/${var.common_params.project}/regions/${var.common_params.region}/subnetworks/${var.common_params.dataflow_params.runner_params.subnetwork}" : null
  temp_location                = var.common_params.dataflow_params.runner_params.temp_location
  on_delete                    = var.common_params.dataflow_params.runner_params.on_delete
  region                       = var.common_params.region
  ip_configuration             = var.common_params.dataflow_params.runner_params.ip_configuration
  labels = {
    "migration_id" = var.shard_list[count.index].shard_id != null ? var.shard_list[count.index].shard_id : random_pet.migration_id[count.index].id
  }
}

