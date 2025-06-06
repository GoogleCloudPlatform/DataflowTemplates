# Alerting Configuration Variables
# Alert Thresholds, define the threshold values for alerting metrics, which trigger alerts when exceeded.

# Google Cloud Storage Alert Thresholds
gcs_object_count_dlq_threshold = 50  # Threshold for dead-letter queue (DLQ) object count in Cloud Storage.
gcs_read_write_throttles_threshold = 200000  # Threshold for read/write throttles on Cloud Storage.

# Dataflow Alert Thresholds
dataflow_conversion_errors_threshold = 10  # Threshold for the number of conversion errors in Dataflow.
dataflow_other_errors_threshold = 15  # Threshold for other (non-conversion) errors in Dataflow.
dataflow_total_errors_threshold = 25  # Total errors threshold for Dataflow jobs.

# Pub/Sub Alert Thresholds
pubsub_age_of_oldest_message_threshold = 120  # Maximum age of the oldest message in Pub/Sub in seconds.

# Notification Configuration
email_address = "nehamodgil@google.com"  # Email address to receive alert notifications.
notification_channels = [
  "projects/<ALERTING_PROJECT_ID>/notificationChannels/email"  # Specify the notification channel using the correct project ID.
]