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

variable "working_directory_bucket" {
  type        = string
  description = "GCS bucket for uploading sesion file and creating output directory. Example: 'test-bucket'"
}

variable "working_directory_prefix" {
  type        = string
  description = "Prefix within the GCS bucket for Dataflow working directory. Should not start or end with a '/'."
}

variable "jdbc_driver_jars" {
  type        = string
  description = "Comma-separated list of JDBC driver JAR file paths in GCS."
  default     = null
}

variable "jdbc_driver_class_name" {
  type        = string
  description = "Fully qualified JDBC driver class name."
  default     = null
}

variable "source_config_url" {
  type        = string
  description = "JDBC connection url for the source database. Ex- jdbc:mysql://127.4.5.30:3306/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8"
}

variable "username" {
  type        = string
  description = "Username to log in to the specified source database"
}

variable "password" {
  type        = string
  description = "Password to log in to the specified source database"
}

variable "num_partitions" {
  type        = number
  description = "Number of partitions for parallel processing."
  default     = null
}

variable "max_connections" {
  type        = number
  description = "Maximum number of database connections."
  default     = null
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
  description = "Google Cloud Project ID (for Spanner)."
}

variable "spanner_host" {
  type        = string
  description = "Custom Spanner host endpoint."
  default     = null
}

variable "local_session_file_path" {
  type        = string
  description = "Local path to the session file."
  default     = null
}

variable "transformation_jar_path" {
  type        = string
  description = "GCS path to the transformation JAR file."
  default     = null
}

variable "transformation_custom_parameters" {
  type        = string
  description = "Custom parameters for the transformation."
  default     = null
}

variable "transformation_class_name" {
  type        = string
  description = "Fully qualified transformation class name."
  default     = null
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

variable "fetch_size" {
  type        = number
  description = "Fetch size for the JDBC connection."
  default     = null
}

variable "gcs_output_directory" {
  type        = string
  description = "GCS output directory for the job."
  default     = null
}

variable "additional_pipeline_options" {
  type        = list(string)
  description = "Additional Dataflow pipeline options."
  default     = []
}