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