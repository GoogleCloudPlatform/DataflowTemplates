terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = "~>1.2"
}

#Set the project
provider "google-beta" {
  project = var.common_params.project
  region  = var.common_params.region
}

provider "google" {
  project = var.common_params.project
  region  = var.common_params.region
}

#Enable the APIs
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "spanner.googleapis.com",
    "compute.googleapis.com"
  ])
  service            = each.key
  project            = var.common_params.project
  disable_on_destroy = false
}
# To fetch project number
data "google_project" "project" {
}

# Fetch the default service account for Compute Engine (used by Dataflow)
data "google_compute_default_service_account" "gce_account" {
  depends_on = [google_project_service.enabled_apis]
}