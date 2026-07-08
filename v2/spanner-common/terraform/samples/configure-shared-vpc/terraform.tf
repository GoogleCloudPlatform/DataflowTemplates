terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = "~>1.2"
}

provider "google" {
  project = var.common_params.host_project
  region  = var.common_params.region
}

#Enable the APIs
resource "google_project_service" "enabled_apis_service_project" {
  for_each = toset([
    "dataflow.googleapis.com",
    "datastream.googleapis.com",
    "compute.googleapis.com"
  ])
  service            = each.key
  project            = var.common_params.service_project
  disable_on_destroy = false
}
# To fetch project number
data "google_project" "service_project" {
  project_id = var.common_params.service_project
}