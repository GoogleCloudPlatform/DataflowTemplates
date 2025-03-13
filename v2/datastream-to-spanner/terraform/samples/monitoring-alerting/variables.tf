# Project-level Variables
variable "project_id" {
  type        = string
  description = "The ID of the Google Cloud project"
}

variable "spanner_project_id" {
  type        = string
  description = "The ID of the Google Cloud Project where spanner instances are created"
}

variable "prefix" {
  type        = string
  description = "The service ID for the GCS buckets"
}

 variable "alerting_project_id" {
   type = string
   description = "The ID of the Google Cloud Project where alerting is created "
 }

 variable "email_address" {
   description = "The email address for email notifications."
   type        = string
 }

 variable "notification_channels" {
   description = "List of notification channels"
   type        = list(string)
 }

 variable "gcs_object_count_dlq_threshold" {
   description = "Threshold for GCS object count in DLQ directory."
   type        = number
 }

 variable "gcs_read_write_throttles_threshold" {
   description = "Threshold for GCS read/write throttles."
   type        = number
 }

 variable "pubsub_age_of_oldest_message_threshold" {
   description = "Threshold for Pub/Sub age of oldest message."
   type        = number
 }

 variable "dataflow_conversion_errors_threshold" {
   description = "Threshold for conversion errors"
   type        = number
 }

 variable "dataflow_other_errors_threshold" {
   description = "Threshold for other errors"
   type        = number
 }

 variable "dataflow_total_errors_threshold" {
   description = "Threshold for total errors"
   type        = number
 }
