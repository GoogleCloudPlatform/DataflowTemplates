variable "project" {
  type        = string
  description = "Google Cloud Project ID where Dataflow will run."
}

variable "host_project" {
  type        = string
  description = "Project id hosting the network in case of a shared vpc setup."
  default     = ""
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
  default     = ""
}

variable "network" {
  type        = string
  description = "Network for Dataflow workers."
  default     = ""
}

variable "subnetwork" {
  type        = string
  description = "Subnetwork for Dataflow workers."
  default     = ""
}

variable "machine_type" {
  type        = string
  description = "Machine type for Dataflow worker VMs."
  default     = "n1-standard-4"
}

variable "max_workers" {
  type        = number
  description = "Maximum number of Dataflow worker VMs."
  default     = null
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
  default     = ""
}

variable "bigquery_dataset" {
  type        = string
  description = "The BigQuery dataset ID where the validation results will be stored. Example: validation_report_dataset"
}

variable "spanner_host" {
  type        = string
  description = "Custom Spanner host endpoint."
  default     = ""
}

variable "spanner_priority" {
  type        = string
  description = "Priority for Spanner RPC invocations"
  default     = ""

  validation {
    condition     = var.spanner_priority == null ? true : contains(["HIGH", "MEDIUM", "LOW"], var.spanner_priority)
    error_message = "spanner_priority must be one of 'HIGH', 'MEDIUM', 'LOW', or null."
  }
}

variable "session_file_path" {
  type        = string
  description = "GCS path to the session file."
  default     = ""
}

variable "schema_overrides_file_path" {
  type        = string
  description = "GCS path to the schema overrides file."
  default     = ""
}

variable "table_overrides" {
  type        = string
  description = "Table name overrides from source to Spanner."
  default     = ""
}

variable "column_overrides" {
  type        = string
  description = "Column name overrides from source to Spanner."
  default     = ""
}

variable "run_id" {
  type        = string
  description = "A unique identifier for the validation run."
  default     = ""
}

variable "transformation_jar_path" {
  type        = string
  description = "GCS path to the transformation JAR file."
  default     = ""
}

variable "transformation_class_name" {
  type        = string
  description = "Fully qualified transformation class name."
  default     = ""
}

variable "transformation_custom_parameters" {
  type        = string
  description = "Custom parameters for the transformation."
  default     = ""
}

variable "additional_experiments" {
  type        = list(string)
  description = "Additional Dataflow experiments."
  default     = []
}

variable "ip_configuration" {
  type        = string
  description = "IP configuration for Dataflow workers (e.g. 'WORKER_IP_PRIVATE')."
  default     = ""
}

variable "launcher_machine_type" {
  type        = string
  description = "Machine type for the Dataflow launcher VM."
  default     = ""
}

variable "num_workers" {
  type        = number
  description = "Initial number of Dataflow worker VMs."
  default     = null
}

variable "additional_pipeline_options" {
  type        = map(string)
  description = "Additional Dataflow pipeline options."
  default     = {}
}

variable "labels" {
  type        = map(string)
  description = "Labels to apply to the Dataflow job."
  default     = {}
}

variable "kms_key_name" {
  type        = string
  description = "Cloud KMS key name for data encryption."
  default     = ""
}
