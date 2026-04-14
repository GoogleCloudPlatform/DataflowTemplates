variable "project_id" {
  description = "The GCP project ID to deploy resources into"
  type        = string
}

variable "region" {
  description = "The GCP region to use"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The GCP zone to use"
  type        = string
  default     = "us-central1-a"
}

variable "network" {
  description = "The VPC network to use"
  type        = string
  default     = "default"
}

variable "subnetwork" {
  description = "The subnetwork to use"
  type        = string
  default     = "default"
}

variable "spanner_instance_name" {
  description = "The name of the Spanner instance"
  type        = string
  default     = "it-infra-spanner"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
  default     = ""
}

variable "datastream_private_connection_id" {
  description = "The ID of the Datastream private connection"
  type        = string
  default     = "datastream-connect-2"
}

variable "github_repo_owner" {
  description = "The owner of the GitHub repo for the runner"
  type        = string
  default     = "GoogleCloudPlatform"
}

variable "github_repo_name" {
  description = "The name of the GitHub repo for the runner"
  type        = string
  default     = "DataflowTemplates"
}

variable "github_token" {
  description = "The GitHub token for the runner"
  type        = string
  sensitive   = true
  default     = ""
}

variable "gh_runner_version" {
  description = "The version of the GitHub runner"
  type        = string
  default     = "2.311.0"
}

# New variables for hardcoded names

variable "private_ip_range_name" {
  description = "The name of the reserved IP range for Private Service Access"
  type        = string
  default     = "it-infra-private-ip-range"
}



variable "postgres_instance_name" {
  description = "The name of the PostgreSQL instance"
  type        = string
  default     = "it-infra-pg-db-instance"
}



variable "mysql_instance_name" {
  description = "The name of the MySQL instance"
  type        = string
  default     = "it-infra-mysql-db-instance"
}



variable "it_infra_vm_name" {
  description = "The name of the consolidated VM for tests and proxy"
  type        = string
  default     = "it-infra-vm"
}