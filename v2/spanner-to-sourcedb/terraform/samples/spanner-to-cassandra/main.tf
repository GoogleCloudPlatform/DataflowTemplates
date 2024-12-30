resource "random_pet" "migration_id" {
  prefix = "spanner-csdr"
}

resource "null_resource" "replace_keys" {
  triggers = {
    always_run = timestamp()
  }
  provisioner "local-exec" {

    command = <<EOT
      sed "s~##host##~${var.shard_config.host}~g; s~##port##~${var.shard_config.port}~g; s~##keyspace##~${var.shard_config.keyspace}~g; s~##dataCenter##~${var.shard_config.dataCenter}~g; s~##localPoolSize##~${var.shard_config.localPoolSize}~g; s~##remotePoolSize##~${var.shard_config.remotePoolSize}~g; s~##username##~${var.shard_config.username}~g; s~##password##~${var.shard_config.password}~g; s~##sslOptions##~${var.shard_config.sslOptions}~g; s~##protocolVersion##~${var.shard_config.protocolVersion}~g; s~##consistency##~${var.shard_config.consistency}~g;" cassandra-config-template.conf > cassandra-config.conf
    EOT
  }
}

# Upload the modified .conf file to the GCS bucket
resource "google_storage_bucket_object" "shard_config" {
  name       = "cassandra-config.conf"
  bucket     = google_storage_bucket.reverse_replication_bucket.id
  source     = "./cassandra-config.conf"
  depends_on = [google_project_service.enabled_apis, null_resource.replace_keys]
}

locals {
  migration_id  = var.common_params.migration_id != null ? var.common_params.migration_id : random_pet.migration_id.id
  change_stream = replace(local.migration_id, "-", "_")
}

# Setup network firewalls rules to enable Dataflow access to source.
resource "google_compute_firewall" "allow_dataflow_to_source" {
  count       = var.common_params.target_tags != null ? 1 : 0
  depends_on  = [google_project_service.enabled_apis]
  project     = var.common_params.host_project != null ? var.common_params.host_project : var.common_params.project
  name        = "allow-dataflow-to-source"
  network     = var.dataflow_params.runner_params.network != null ? var.common_params.host_project != null ? "projects/${var.common_params.host_project}/global/networks/${var.dataflow_params.runner_params.network}" : "projects/${var.common_params.project}/global/networks/${var.dataflow_params.runner_params.network}" : "default"
  description = "Allow traffic from Dataflow to source databases"

  allow {
    protocol = "tcp"
    ports    = ["9042"] # Cassandra port
  }
  source_tags = ["dataflow"]
  target_tags = var.common_params.target_tags
}

# GCS bucket for holding configuration objects
resource "google_storage_bucket" "reverse_replication_bucket" {
  depends_on                  = [google_project_service.enabled_apis]
  name                        = "${local.migration_id}-${var.common_params.replication_bucket}"
  location                    = var.common_params.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels = {
    "migration_id" = local.migration_id
  }
}

# upload local session file to the created GCS bucket
resource "google_storage_bucket_object" "session_file_object" {
  depends_on   = [google_project_service.enabled_apis]
  name         = "session.json"
  source       = "session.json"
  content_type = "application/json"
  bucket       = google_storage_bucket.reverse_replication_bucket.id
}

# Auto-generate the source shards file from the Terraform configuration and
# upload it to GCS.
resource "google_storage_bucket_object" "source_shards_file_object" {
  depends_on   = [google_project_service.enabled_apis]
  name         = "source_shards.json"
  content_type = "application/json"
  bucket       = google_storage_bucket.reverse_replication_bucket.id
  content      = jsonencode(var.shard_list)
}

# Pub/Sub topic for reverse replication DLQ
resource "google_pubsub_topic" "dlq_pubsub_topic" {
  depends_on = [google_project_service.enabled_apis]
  name       = "${local.migration_id}-dlq-topic"
  project    = var.common_params.project
  labels = {
    "migration_id" = local.migration_id
  }
}

# Configure permissions to publish Pub/Sub notifications
resource "google_pubsub_topic_iam_member" "gcs_publisher_role" {
  depends_on = [google_project_service.enabled_apis]
  topic      = google_pubsub_topic.dlq_pubsub_topic.name
  role       = "roles/pubsub.publisher"
  member     = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}

# Pub/Sub Notification on GCS Bucket
resource "google_storage_notification" "dlq_bucket_notification" {
  depends_on = [
    google_project_service.enabled_apis,
    google_pubsub_topic_iam_member.gcs_publisher_role
  ] # Create a bucket notification using the created pubsub topic.
  bucket             = google_storage_bucket.reverse_replication_bucket.name
  object_name_prefix = "dlq"
  payload_format     = "JSON_API_V1"
  topic              = google_pubsub_topic.dlq_pubsub_topic.id
  event_types        = ["OBJECT_FINALIZE"]
}

# Pub/Sub subscription for the created notification
resource "google_pubsub_subscription" "dlq_pubsub_subscription" {
  depends_on = [
    google_project_service.enabled_apis,
    google_storage_notification.dlq_bucket_notification
  ] # Create the subscription once the notification is created.
  name  = "${google_pubsub_topic.dlq_pubsub_topic.name}-sub"
  topic = google_pubsub_topic.dlq_pubsub_topic.id
  labels = {
    "migration_id" = local.migration_id
  }
}

resource "google_spanner_database" "reverse_replication_metadata_database" {
  instance            = var.dataflow_params.template_params.instance_id
  name                = var.dataflow_params.template_params.metadata_database_id != null ? var.dataflow_params.template_params.metadata_database_id : local.change_stream
  deletion_protection = false
}

resource "null_resource" "create_spanner_change_stream" {
  count = var.dataflow_params.template_params.change_stream_name == null ? 1 : 0
  triggers = {
    database_id   = var.dataflow_params.template_params.database_id
    instance_id   = var.dataflow_params.template_params.instance_id
    change_stream = local.change_stream
    project       = var.common_params.project
  }
  provisioner "local-exec" {
    command = <<EOT
gcloud spanner databases ddl update ${self.triggers.database_id} --instance=${self.triggers.instance_id} --project=${self.triggers.project} --ddl="CREATE CHANGE STREAM ${self.triggers.change_stream} FOR ALL OPTIONS (retention_period = '7d', value_capture_type = 'NEW_ROW');"
EOT
  }
  provisioner "local-exec" {
    when    = destroy
    command = <<EOT
gcloud spanner databases ddl update ${self.triggers.database_id} --instance=${self.triggers.instance_id} --ddl="DROP CHANGE STREAM ${self.triggers.change_stream}"
EOT
  }
}

# Add roles to the service account that will run Dataflow for reverse replication
resource "google_project_iam_member" "reverse_replication_roles" {
  depends_on = [null_resource.create_spanner_change_stream]
  for_each = var.common_params.add_policies_to_service_account ? toset([
    "roles/spanner.databaseUser",
    "roles/secretmanager.secretAccessor",
    "roles/secretmanager.viewer"
  ]) : toset([])
  project = data.google_project.project.id
  role    = each.key
  member  = var.dataflow_params.runner_params.service_account_email != null ? "serviceAccount:${var.dataflow_params.runner_params.service_account_email}" : "serviceAccount:${data.google_compute_default_service_account.gce_account.email}"
}

# Dataflow Flex Template Job (for Spanner to Cassandra)
resource "google_dataflow_flex_template_job" "reverse_replication_job" {
  depends_on = [
    google_project_service.enabled_apis, google_project_iam_member.reverse_replication_roles, google_spanner_database.reverse_replication_metadata_database, null_resource.create_spanner_change_stream, google_pubsub_subscription.dlq_pubsub_subscription
  ] # Launch the template once the stream is created.
  provider                = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.common_params.region}/latest/flex/Spanner_to_SourceDb"
  # container_spec_gcs_path = "gs://reverse-replication-dataflow-templates/templates/flex/Spanner_to_SourceDb"
  # Parameters from Dataflow Template
  parameters = {
    changeStreamName         = var.dataflow_params.template_params.change_stream_name != null ? var.dataflow_params.template_params.change_stream_name : local.change_stream
    instanceId               = var.dataflow_params.template_params.instance_id
    databaseId               = var.dataflow_params.template_params.database_id
    spannerProjectId         = var.dataflow_params.template_params.spanner_project_id != null ? var.dataflow_params.template_params.spanner_project_id : var.common_params.project
    metadataInstance         = var.dataflow_params.template_params.metadata_instance_id != null ? var.dataflow_params.template_params.metadata_instance_id : var.dataflow_params.template_params.instance_id
    metadataDatabase         = var.dataflow_params.template_params.metadata_database_id != null ? var.dataflow_params.template_params.metadata_database_id : local.change_stream
    sourceShardsFilePath     = "gs://${google_storage_bucket_object.source_shards_file_object.bucket}/${google_storage_bucket_object.source_shards_file_object.name}"
    startTimestamp           = var.dataflow_params.template_params.start_timestamp
    endTimestamp             = var.dataflow_params.template_params.end_timestamp
    shadowTablePrefix        = var.dataflow_params.template_params.shadow_table_prefix
    sessionFilePath          = "gs://${google_storage_bucket_object.session_file_object.bucket}/${google_storage_bucket_object.session_file_object.name}"
    filtrationMode           = var.dataflow_params.template_params.filtration_mode
    shardingCustomJarPath    = var.dataflow_params.template_params.sharding_custom_jar_path
    shardingCustomClassName  = var.dataflow_params.template_params.sharding_custom_class_name
    shardingCustomParameters = var.dataflow_params.template_params.sharding_custom_parameters
    sourceDbTimezoneOffset   = var.dataflow_params.template_params.source_db_timezone_offset
    dlqGcsPubSubSubscription = google_pubsub_subscription.dlq_pubsub_subscription.id
    skipDirectoryName        = var.dataflow_params.template_params.skip_directory_name
    maxShardConnections      = var.dataflow_params.template_params.max_shard_connections
    deadLetterQueueDirectory = "${google_storage_bucket.reverse_replication_bucket.url}/dlq"
    dlqMaxRetryCount         = var.dataflow_params.template_params.dlq_max_retry_count
    runMode                  = var.dataflow_params.template_params.run_mode
    dlqRetryMinutes          = var.dataflow_params.template_params.dlq_retry_minutes
    # targetDatabase           = "ecommerce" # Cassandra keyspace name
    # targetHost               = "10.0.0.2" # Cassandra host IP
    # targetPort               = "9042"    # Cassandra port
    # targetUser               = "ollion"  # Cassandra user
    # targetPassword           = "Ollion@2023" # Cassandra password
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