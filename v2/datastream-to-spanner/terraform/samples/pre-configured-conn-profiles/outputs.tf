# Resource IDs
output "resource_ids" {
  description = "IDs of resources created during migration setup."
  value = {
    datastream_source_connection_profile = var.datastream_params.source_connection_profile_id
    datastream_target_connection_profile = var.datastream_params.target_connection_profile_id
    datastream_stream                    = google_datastream_stream.mysql_to_gcs.stream_id
    gcs_bucket                           = var.datastream_params.target_gcs_bucket_name
    pubsub_topic                         = google_pubsub_topic.datastream_topic.name
    pubsub_subscription                  = google_pubsub_subscription.datastream_subscription.name
    dataflow_job                         = google_dataflow_flex_template_job.live_migration_job.job_id
  }
}

# Resource URLs (for easy access in Google Cloud Console)
output "resource_urls" {
  description = "URLs to access resources in the Google Cloud Console."
  value = {
    datastream_source_connection_profile = "https://console.cloud.google.com/datastream/connection-profiles/locations/${var.common_params.region}/instances/${var.datastream_params.source_connection_profile_id}?project=${var.common_params.project}"
    datastream_target_connection_profile = "https://console.cloud.google.com/datastream/connection-profiles/locations/${var.common_params.region}/instances/${var.datastream_params.target_connection_profile_id}?project=${var.common_params.project}"
    datastream_stream                    = "https://console.cloud.google.com/datastream/streams/locations/${var.common_params.region}/instances/${google_datastream_stream.mysql_to_gcs.stream_id}?project=${var.common_params.project}"
    gcs_bucket                           = "https://console.cloud.google.com/storage/browser/${var.datastream_params.target_gcs_bucket_name}?project=${var.common_params.project}"
    pubsub_topic                         = "https://console.cloud.google.com/cloudpubsub/topic/detail/${google_pubsub_topic.datastream_topic.name}?project=${var.common_params.project}"
    pubsub_subscription                  = "https://console.cloud.google.com/cloudpubsub/subscription/detail/${google_pubsub_subscription.datastream_subscription.name}?project=${var.common_params.project}"
    dataflow_job                         = "https://console.cloud.google.com/dataflow/jobs/${var.common_params.region}/${google_dataflow_flex_template_job.live_migration_job.job_id}?project=${var.common_params.project}"
  }
  depends_on = [
    google_datastream_stream.mysql_to_gcs,
    google_pubsub_topic.datastream_topic,
    google_pubsub_subscription.datastream_subscription,
    google_dataflow_flex_template_job.live_migration_job
  ]
}
