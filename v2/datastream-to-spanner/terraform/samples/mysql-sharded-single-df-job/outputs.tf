# Resource IDs (Structured by Shard ID)
output "resource_ids" {
  description = "IDs of resources created, organized by shard ID."
  value = merge(
    {
      for idx, shard in var.shard_list :
      (shard.shard_id != null ? shard.shard_id : random_pet.migration_id[idx].id) => {
        datastream_source_connection_profile = google_datastream_connection_profile.source_mysql[idx].connection_profile_id
        datastream_stream                    = google_datastream_stream.mysql_to_gcs[idx].stream_id
      }
    },
    {
      datastream_target_connection_profile = google_datastream_connection_profile.target_gcs.connection_profile_id
      gcs_bucket                           = google_storage_bucket.datastream_bucket.name
      pubsub_topic                         = google_pubsub_topic.datastream_topic.name
      pubsub_subscription                  = google_pubsub_subscription.datastream_subscription.name
      dataflow_job                         = var.common_params.dataflow_params.skip_dataflow ? "" : google_dataflow_flex_template_job.live_migration_job[0].job_id
    }
  )

  depends_on = [
    random_pet.migration_id,
    google_datastream_connection_profile.source_mysql,
    google_datastream_connection_profile.target_gcs,
    google_datastream_stream.mysql_to_gcs,
    google_storage_bucket.datastream_bucket,
    google_pubsub_topic.datastream_topic,
    google_pubsub_subscription.datastream_subscription,
    google_dataflow_flex_template_job.live_migration_job
  ]
}


# Resource URLs (Structured by Shard ID)
output "resource_urls" {
  description = "URLs to access resources in the Google Cloud Console, organized by shard ID."
  value = merge({
    for idx, shard in var.shard_list :
    (shard.shard_id != null ? shard.shard_id : random_pet.migration_id[idx].id) => {
      datastream_source_connection_profile = "https://console.cloud.google.com/datastream/connection-profiles/locations/${var.common_params.region}/instances/${google_datastream_connection_profile.source_mysql[idx].connection_profile_id}?project=${var.common_params.project}"
      datastream_stream                    = "https://console.cloud.google.com/datastream/streams/locations/${var.common_params.region}/instances/${google_datastream_stream.mysql_to_gcs[idx].stream_id}?project=${var.common_params.project}"
    }
    },
    {
      datastream_target_connection_profile = "https://console.cloud.google.com/datastream/connection-profiles/locations/${var.common_params.region}/instances/${google_datastream_connection_profile.target_gcs.connection_profile_id}?project=${var.common_params.project}"
      gcs_bucket                           = "https://console.cloud.google.com/storage/browser/${google_storage_bucket.datastream_bucket.name}?project=${var.common_params.project}"
      pubsub_topic                         = "https://console.cloud.google.com/cloudpubsub/topic/detail/${google_pubsub_topic.datastream_topic.name}?project=${var.common_params.project}"
      pubsub_subscription                  = "https://console.cloud.google.com/cloudpubsub/subscription/detail/${google_pubsub_subscription.datastream_subscription.name}?project=${var.common_params.project}"
      dataflow_job                         = var.common_params.dataflow_params.skip_dataflow ? "" : "https://console.cloud.google.com/dataflow/jobs/${var.common_params.region}/${google_dataflow_flex_template_job.live_migration_job[0].job_id}?project=${var.common_params.project}"
  })

  depends_on = [
    random_pet.migration_id,
    google_datastream_connection_profile.source_mysql,
    google_datastream_connection_profile.target_gcs,
    google_datastream_stream.mysql_to_gcs,
    google_storage_bucket.datastream_bucket,
    google_pubsub_topic.datastream_topic,
    google_pubsub_subscription.datastream_subscription,
    google_dataflow_flex_template_job.live_migration_job
  ]
}