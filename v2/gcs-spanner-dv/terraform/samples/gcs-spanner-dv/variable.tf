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

variable "region" {
  type        = string
  description = "Google Cloud region to run Dataflow in."
}

# Dataflow runtime parameters
variable "additional_experiments" {
  type        = list(string)
  description = "Additional Dataflow experiments. 'disable_runner_v2' is required for bulk jobs."
  default = [
    "disable_runner_v2", "use_network_tags=allow-dataflow", "use_network_tags_for_flex_templates=allow-dataflow"
  ]
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
  description = "Machine type for the Dataflow launcher VM. Recommend >= 16 vCPUs."
  default     = "n1-highmem-32"
}

variable "machine_type" {
  type        = string
  description = "Machine type for Dataflow worker VMs."
  default     = "n1-highmem-4"
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

variable "default_log_level" {
  type        = string
  description = "Default log level for Dataflow jobs (e.g., 'INFO', 'DEBUG')."
  default     = null
}

variable "gcs_input_directory" {
  type        = string
  description = "GCS input directory for the job."
  default     = null
}

variable "instance_id" {
  type        = string
  description = "Spanner instance id"
  default     = null
}

variable "database_id" {
  type        = string
  description = "Spanner database id"
  default     = null
}

variable "spanner_host" {
  type        = string
  description = "Spanner API endpoint"
  default     = null
}

variable "spanner_priority" {
  type        = string
  description = "Spanner RPC priority"
  default     = null
}

variable "big_query_dataset" {
  type        = string
  description = "Name of the BigQuery dataset where results will be reported"
  default     = null
}

variable "additional_pipeline_options" {
  type        = list(string)
  description = "Additional Dataflow pipeline options."
  default     = []
}