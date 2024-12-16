# Alert policy for tracking the age of the oldest unacknowledged message in Pub/Sub subscriptions
resource "google_monitoring_alert_policy" "age_of_oldest_message" {
  # Display name for the alert policy, which can be prefixed for identification
  display_name = format("${var.prefix} - Pub/Sub Age of Oldest Message Alert Policy")

  conditions {
    display_name = "Oldest Message Age Exceeds Threshold"

    # Condition settings to trigger an alert based on message age threshold
    condition_threshold {
      # Filter to track the 'oldest_unacked_message_age' metric for Pub/Sub subscriptions in a specific project
      # Only consider subscriptions associated with topics that start with a specified prefix
      filter = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\" AND resource.labels.project_id = ${var.alerting_project_id} AND metadata.system_labels.topic_id = starts_with(${var.prefix})"

      comparison      = "COMPARISON_GT"  # Trigger alert if metric exceeds the threshold
      threshold_value = var.pubsub_age_of_oldest_message_threshold  # Threshold value in seconds (e.g., 1 hour)
      duration        = "300s"           # Trigger alert if condition persists for 5 minutes

      aggregations {
        alignment_period   = "60s"         # Aggregate data points into 1-minute intervals
        per_series_aligner = "ALIGN_MAX"   # Use the maximum value within each 1-minute interval
      }
    }
  }

  # Combiner set to "OR" (not significant here since there's only one condition)
  combiner = "OR"

  # Notification channels to receive alert notifications
  notification_channels = var.notification_channels
  enabled               = true  # Enable the alert policy
}
