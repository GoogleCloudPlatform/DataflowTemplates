# Module for Dashboard Deployment
# This module sets up a Cloud Monitoring dashboard for visualizing metrics.
module "dashboard" {
  source = "../modules/dashboard"  # Path to the dashboard module source.
  project_id = var.project_id  # ID of the project where the dashboard will be deployed.
  prefix = var.prefix  # prefix associated with the monitored resources.
  spanner_project_id = var.spanner_project_id  # ID of the project containing Spanner metrics to display on the dashboard.
}

# Module for Notification Channels
# This module sets up the notification channels for alerting, e.g., email.
module "notification_channels" {
  source = "../modules/notification_channels"  # Path to the notification channels module.
  email_address = var.email_address  # Email address to receive alert notifications.
}

# Module for Google Cloud Storage (GCS) Alerts
# This module defines alerting rules for Google Cloud Storage metrics, like DLQ object count and read/write throttling.
module "gcs_alerts" {
  source = "../modules/alerting/gcs"  # Path to the GCS alerting module.

  project_id = var.project_id  # Project ID for general resource monitoring.
  alerting_project_id = var.alerting_project_id  # ID for the alerting-specific project (if different from project_id).
  prefix= var.prefix  # prefix for the resources being monitored.

  # Thresholds for triggering GCS alerts
  gcs_object_count_dlq_threshold = var.gcs_object_count_dlq_threshold  # DLQ object count threshold.
  gcs_read_write_throttles_threshold = var.gcs_read_write_throttles_threshold  # Threshold for read/write throttles.

  notification_channels = module.notification_channels.notification_channels  # Links notification channels from the notification_channels module.
}

# Module for Pub/Sub Alerts
# This module defines alerting rules for Pub/Sub metrics, such as the age of the oldest message.
module "pubsub_alerts" {
  source = "../modules/alerting/pubsub"  # Path to the Pub/Sub alerting module.

  project_id = var.project_id  # Project ID for general resource monitoring.
  alerting_project_id = var.alerting_project_id  # ID for the alerting-specific project (if different from project_id).
  prefix = var.prefix  # prefix for the resources being monitored.

  # Threshold for triggering Pub/Sub alerts
  pubsub_age_of_oldest_message_threshold = var.pubsub_age_of_oldest_message_threshold  # Threshold for the age of the oldest message in Pub/Sub.

  notification_channels = module.notification_channels.notification_channels  # Links notification channels from the notification_channels module.
}

# Module for Dataflow Alerts
# This module defines alerting rules for Dataflow metrics, including conversion errors, other errors, and total errors.
module "dataflow_alerts" {
  source = "../modules/alerting/dataflow"  # Path to the Dataflow alerting module.

  project_id = var.project_id  # Project ID for general resource monitoring.
  alerting_project_id = var.alerting_project_id  # ID for the alerting-specific project (if different from project_id).
  prefix = var.prefix  # prefix for the resources being monitored.

  # Thresholds for triggering Dataflow alerts
  dataflow_conversion_errors_threshold = var.dataflow_conversion_errors_threshold  # Threshold for conversion errors.
  dataflow_other_errors_threshold = var.dataflow_other_errors_threshold  # Threshold for other (non-conversion) errors.
  dataflow_total_errors_threshold = var.dataflow_total_errors_threshold  # Total errors threshold for Dataflow jobs.

  notification_channels = module.notification_channels.notification_channels  # Links notification channels from the notification_channels module.
}
