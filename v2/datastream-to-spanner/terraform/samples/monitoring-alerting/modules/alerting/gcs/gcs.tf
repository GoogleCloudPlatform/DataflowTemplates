# Alert policy for monitoring the object count in Dead Letter Queue (DLQ) directories within GCS buckets
resource "google_monitoring_alert_policy" "object_count_dlq" {
  # Display name for the alert policy, includes prefix for easy identification
  display_name = format("${var.prefix} - GCS Object Count in DLQ Directories Alert")

  conditions {
    display_name = "Object Count in DLQ Exceeds Threshold"

    # Condition to trigger alert when object count in DLQ directory surpasses a specified threshold
    condition_threshold {
      # Filter to target GCS buckets with names matching a specific prefix and containing "dlq" 
      # Monitors the "storage/object_count" metric in those buckets
      filter = "resource.type = \"gcs_bucket\" AND (resource.labels.project_id = ${var.alerting_project_id} AND resource.labels.bucket_name = starts_with(${var.prefix}) AND resource.labels.bucket_name = monitoring.regex.full_match(\"dlq\")) AND metric.type = \"storage.googleapis.com/storage/object_count\""

      # Aggregation settings to define data processing intervals and alignment method
      aggregations {
        alignment_period     = "300s"        # Align data points into 5-minute intervals
        cross_series_reducer = "REDUCE_NONE" # No cross-series aggregation
        per_series_aligner   = "ALIGN_MEAN"  # Use the mean value within each interval
      }

      # Trigger alert if object count exceeds threshold immediately (no duration buffer)
      comparison     = "COMPARISON_GT"
      duration       = "0s"
      threshold_value = var.gcs_object_count_dlq_threshold

      # Trigger alert based on the threshold being exceeded in at least 1 data point
      trigger {
        count = 1
      }
    }
  }

  # Combiner set to "OR" for single condition
  combiner = "OR"
  enabled  = true  # Enable the alert policy

  # Notification channels for sending alert notifications
  notification_channels = var.notification_channels
}

# Alert policy for monitoring read and write throttles in GCS buckets
resource "google_monitoring_alert_policy" "read_write_throttles" {
  # Display name for the alert policy, includes prefix for easy identification
  display_name = format("${var.prefix} - GCS Read/Write Throttles Alert")

  conditions {
    display_name = "Read/Write Throttles Exceed Threshold"

    # Condition to trigger alert when read/write throttling in GCS exceeds threshold
    condition_threshold {
      # Filter to target GCS buckets with names matching a specific prefix
      # Monitors the "network/received_bytes_count" metric for throttling
      filter = "resource.type = \"gcs_bucket\" AND (resource.labels.project_id = ${var.alerting_project_id} AND resource.labels.bucket_name = starts_with(${var.prefix})) AND metric.type = \"storage.googleapis.com/network/received_bytes_count\""

      # Aggregation settings to define data processing intervals and alignment method
      aggregations {
        alignment_period     = "300s"        # Align data points into 5-minute intervals
        cross_series_reducer = "REDUCE_NONE" # No cross-series aggregation
        per_series_aligner   = "ALIGN_MEAN"  # Use the mean value within each interval
      }

      # Trigger alert if read/write throttling exceeds threshold immediately (no duration buffer)
      comparison     = "COMPARISON_GT"
      duration       = "0s"
      threshold_value = var.gcs_read_write_throttles_threshold

      # Trigger alert based on the threshold being exceeded in at least 1 data point
      trigger {
        count = 1
      }
    }
  }

  # Combiner set to "OR" for single condition
  combiner = "OR"

  # Notification channels for sending alert notifications
  notification_channels = var.notification_channels
  enabled = true  # Enable the alert policy
}
