variable "project_id" {
  description = "The ID of the Google Cloud project"
  type        = string
}

variable "spanner_project_id" {
  type        = string
  description = "The ID of the Google Cloud Project where spanner instances are created"
}

variable "prefix" {
  type        = string
  description = "The service ID for the GCS buckets"
}
