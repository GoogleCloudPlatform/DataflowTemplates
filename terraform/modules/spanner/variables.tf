variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region"
  type        = string
}

variable "name" {
  description = "Spanner Instance Name"
  type        = string
}

variable "display_name" {
  description = "Spanner Instance Display Name"
  type        = string
}

variable "num_nodes" {
  description = "Number of Nodes"
  type        = number
}

variable "config" {
  description = "Config of Spanner"
  type        = string
}

variable "edition" {
  description = "Edition of Spanner"
  type        = string
}


