variable "project" {
  type        = string
  description = "Google Cloud Project ID where Dataflow will run."
}

variable "region" {
  type        = string
  description = "Google Cloud region to run Dataflow in."
}

variable "job_name" {
  type        = string
  description = "Dataflow job name."
}

variable "add_policies_to_service_account" {
  type        = bool
  description = "Terraform will add the required permission to the dataflow service account."
  default     = true
}

variable "service_account_email" {
  type        = string
  description = "Service account email for Dataflow workers. If not set, the default Compute Engine service account is used."
  default     = null
}

variable "network" {
  type        = string
  description = "Network for Dataflow workers."
  default     = null
}

variable "subnetwork" {
  type        = string
  description = "Subnetwork for Dataflow workers."
  default     = null
}

variable "machine_type" {
  type        = string
  description = "Machine type for Dataflow worker VMs."
  default     = "n1-standard-4"
}

variable "max_workers" {
  type        = number
  description = "Maximum number of Dataflow worker VMs."
  default     = 10
}

variable "gcs_input_directory" {
  type        = string
  description = "The GCS directory containing the data for validation. Example: gs://my-bucket/path/"
}

variable "instance_id" {
  type        = string
  description = "Cloud Spanner instance ID."
}

variable "database_id" {
  type        = string
  description = "Cloud Spanner database ID."
}

variable "spanner_project_id" {
  type        = string
  description = "Google Cloud Project ID containing the Spanner instance. Defaults to the Dataflow project if not provided."
  default     = null
}

variable "bigquery_dataset" {
  type        = string
  description = "The BigQuery dataset ID where the validation results will be stored. Example: validation_report_dataset"
}

variable "spanner_host" {
  type        = string
  description = "Custom Spanner host endpoint."
  default     = null
}

variable "spanner_priority" {
  type        = string
  description = "Priority for Spanner RPCs (e.g. HIGH, MEDIUM, LOW)."
  default     = null
}

variable "session_file_path" {
  type        = string
  description = "GCS path to the session file."
  default     = null
}

variable "schema_overrides_file_path" {
  type        = string
  description = "GCS path to the schema overrides file."
  default     = null
}

variable "table_overrides" {
  type        = string
  description = "Table name overrides from source to Spanner."
  default     = null
}

variable "column_overrides" {
  type        = string
  description = "Column name overrides from source to Spanner."
  default     = null
}

variable "run_id" {
  type        = string
  description = "A unique identifier for the validation run."
  default     = null
}

variable "transformation_jar_path" {
  type        = string
  description = "GCS path to the transformation JAR file."
  default     = null
}

variable "transformation_class_name" {
  type        = string
  description = "Fully qualified transformation class name."
  default     = null
}

variable "transformation_custom_parameters" {
  type        = string
  description = "Custom parameters for the transformation."
  default     = null
}

variable "additional_experiments" {
  type        = list(string)
  description = "Additional Dataflow experiments."
  default     = []
}
