variable "gcs_object_count_dlq_threshold" {
  description = "Threshold for GCS object count in DLQ directory."
  type        = number
}

variable "gcs_read_write_throttles_threshold" {
  description = "Threshold for GCS read/write throttles."
  type        = number
}

variable "project_id" {
  type        = string
  description = "The ID of the Google Cloud project"
}

variable "alerting_project_id" {
  type = string
  description = "The ID of the Google Cloud Project where alerting is created "
}

variable "prefix" {
  type        = string
  description = "The service ID for the GCS buckets"
}

variable "notification_channels" {
  description = "List of notification channels"
  type        = list(string)
}