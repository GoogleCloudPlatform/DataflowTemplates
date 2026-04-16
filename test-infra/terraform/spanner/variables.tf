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





# New variables for hardcoded names



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