# Log-based metric for tracking Dataflow conversion errors separately
resource "google_logging_metric" "dataflow_conversion_errors_metric" {
  name = "dataflow_conversion_errors_metric"

  # Filter to capture only conversion errors with severity of ERROR or higher for Dataflow jobs
  filter = "resource.type=\"dataflow_job\" AND severity>=ERROR AND textPayload:\"conversion error\""

  # Metric descriptor settings to define the metric type and value type
  metric_descriptor {
    metric_kind = "DELTA"  # Tracks change over time
    value_type  = "INT64"  # Counts integer values
  }

}

# Log-based metric for other (non-conversion) Dataflow errors
resource "google_logging_metric" "dataflow_other_errors_metric" {
  name        = "dataflow_other_errors_metric"
  description = "Metric to track other Dataflow errors (excluding conversion errors)"

  # Filter to capture all Dataflow errors except conversion errors
  filter      = "resource.type=\"dataflow_job\" AND severity>=ERROR AND NOT textPayload:\"conversion error\""

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }

}

# Log-based metric for total Dataflow errors (both conversion and other errors)
resource "google_logging_metric" "dataflow_total_errors_metric" {
  name        = "dataflow_total_errors_metric"
  description = "Metric to track the total number of Dataflow errors"

  # Filter to capture all Dataflow errors with severity of ERROR or higher
  filter      = "resource.type=\"dataflow_job\" AND severity>=ERROR"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }

}

# Alert policy for conversion errors in Dataflow
resource "google_monitoring_alert_policy" "dataflow_errors" {
  # Display name for the alert policy, with an optional prefix for custom naming
  display_name = format("${var.prefix} - Dataflow Conversion Errors Alert Policy")

  conditions {
    display_name = "Dataflow Conversion Errors Condition"

    # Condition threshold settings, including metric filter and threshold value
    condition_threshold {
      # Filter to include only conversion errors with specified project ID
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/dataflow_conversion_errors_metric\" AND resource.labels.project_id = ${var.alerting_project_id}"
      comparison      = "COMPARISON_GT"  # Triggers alert when threshold exceeded
      threshold_value = var.dataflow_conversion_errors_threshold
      duration        = "60s"  # Alert triggered if error rate persists for 60 seconds
    }
  }

  # Notification channels to receive the alert notifications
  notification_channels = var.notification_channels

  # Ensure alert creation after the metric is defined
  depends_on = [google_logging_metric.dataflow_conversion_errors_metric]
  combiner   = "OR"  # Trigger alert if any condition in the policy is met
}

# Alert policy for other Dataflow errors
resource "google_monitoring_alert_policy" "dataflow_other_errors_alert" {
  display_name = format("${var.prefix} - Dataflow Other Errors Alert")

  conditions {
    display_name = "Dataflow Other Errors Condition"

    condition_threshold {
      # Filter for non-conversion errors with specified project ID
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/dataflow_other_errors_metric\" AND resource.labels.project_id = ${var.alerting_project_id}"
      comparison      = "COMPARISON_GT"
      threshold_value = var.dataflow_other_errors_threshold
      duration        = "60s"
    }
  }

  notification_channels = var.notification_channels

  depends_on = [google_logging_metric.dataflow_other_errors_metric]
  combiner   = "OR"
}

# Alert policy for total Dataflow errors
resource "google_monitoring_alert_policy" "dataflow_total_errors_alert" {
  display_name = format("${var.prefix} - Dataflow Total Errors Alert")

  conditions {
    display_name = "Dataflow Total Errors Condition"

    condition_threshold {
      # Filter for all types of Dataflow errors with specified project ID
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/dataflow_total_errors_metric\" AND resource.labels.project_id = ${var.alerting_project_id}"
      comparison      = "COMPARISON_GT"
      threshold_value = var.dataflow_total_errors_threshold
      duration        = "60s"
    }
  }

  notification_channels = var.notification_channels

  depends_on = [google_logging_metric.dataflow_total_errors_metric]
  combiner   = "OR"
}
