variable "add_policies_to_service_account" {
  type        = bool
  description = "Terraform will add the required permission to the dataflow service account."
  default     = true
}

variable "job_name" {
  type        = string
  description = "Dataflow job name."
}

variable "project" {
  type        = string
  description = "Google Cloud Project ID where Dataflow will run."
}

variable "host_project" {
  type        = string
  description = "Project id hosting the network in case of a shared vpc setup."
  default     = null
}

variable "region" {
  type        = string
  description = "Google Cloud region to run Dataflow in."
}

variable "gcs_input_directory" {
  type        = string
  description = "This directory is used to read the AVRO files of the records read from source. For example, `gs://your-bucket/your-path`"
  default     = null
}

variable "spanner_project_id" {
  type        = string
  description = "Google Cloud Project ID (for Spanner). This is the name of the Cloud Spanner project."
  default     = null
}

variable "spanner_host" {
  type        = string
  description = "The Cloud Spanner endpoint to call in the template. For example, `https://batch-spanner.googleapis.com`."
  default     = null
}

variable "instance_id" {
  type        = string
  description = "The destination Cloud Spanner instance."
}

variable "database_id" {
  type        = string
  description = "The destination Cloud Spanner database."
}

variable "spanner_priority" {
  type        = string
  description = "The request priority for Cloud Spanner calls. The value must be one of: [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `HIGH`."
  default     = null
}

variable "session_file_path" {
  type        = string
  description = "Session file path in Cloud Storage that contains mapping information from Spanner Migration Tool. Defaults to empty."
  default     = null
}

variable "schema_overrides_file_path" {
  type        = string
  description = "A file which specifies the table and the column name overrides from source to spanner. Defaults to empty."
  default     = null
}

variable "table_overrides" {
  type        = string
  description = "These are the table name overrides from source to spanner. They are written in the following format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]. Defaults to empty."
  default     = null
}

variable "column_overrides" {
  type        = string
  description = "These are the column name overrides from source to spanner. They are written in the following format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]. Defaults to empty."
  default     = null
}

variable "bigquery_dataset" {
  type        = string
  description = "The BigQuery dataset ID where the validation results will be stored. For example, `validation_report_dataset`"
}

variable "run_id" {
  type        = string
  description = "A unique identifier for the validation run. If not provided, the Dataflow Job Name will be used. For example, `run_20230101_120000`"
  default     = null
}

# Dataflow runtime parameters
variable "additional_experiments" {
  type        = list(string)
  description = "Additional Dataflow experiments."
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

variable "service_account_email" {
  type        = string
  description = "Service account email for Dataflow workers."
  default     = null
}

variable "launcher_machine_type" {
  type        = string
  description = "Machine type for the Dataflow launcher VM."
  default     = null
}

variable "machine_type" {
  type        = string
  description = "Machine type for Dataflow worker VMs."
  default     = null
}

variable "max_workers" {
  type        = number
  description = "Maximum number of Dataflow worker VMs."
  default     = null
}

variable "ip_configuration" {
  type        = string
  description = "IP configuration for Dataflow workers (e.g. 'WORKER_IP_PRIVATE')."
  default     = null
}

variable "num_workers" {
  type        = number
  description = "Initial number of Dataflow worker VMs."
  default     = null
}

variable "additional_pipeline_options" {
  type        = list(string)
  description = "Additional Dataflow pipeline options."
  default     = []
}
