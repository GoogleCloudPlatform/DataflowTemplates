variable "project_id" {
  type        = string
  description = "The ID of the Google Cloud project"
}

variable "prefix" {
  type        = string
  description = "The service ID for the GCS buckets"
}

variable "alerting_project_id" {
  type = string
  description = "The ID of the Google Cloud Project where alerting is created "
}


variable "pubsub_age_of_oldest_message_threshold" {
  description = "Threshold for Pub/Sub age of oldest message."
  type        = number
}

variable "notification_channels" {
  description = "List of notification channels"
  type        = list(string)
}