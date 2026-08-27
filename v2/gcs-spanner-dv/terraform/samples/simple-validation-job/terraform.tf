terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 4.0"
    }
  }
  required_version = "~> 1.2"
}

provider "google" {
  project = var.common_params.project
  region  = var.common_params.region
}

provider "google-beta" {
  project = var.common_params.project
  region  = var.common_params.region
}

# Enable the APIs
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "dataflow.googleapis.com",
    "compute.googleapis.com",
    "storage.googleapis.com",
    "spanner.googleapis.com",
    "bigquery.googleapis.com"
  ])
  service            = each.key
  project            = var.common_params.project
  disable_on_destroy = false
}

# To fetch project number
data "google_project" "project" {
  project_id = var.common_params.project
}

# Fetch the default service account for Compute Engine (used by Dataflow)
data "google_compute_default_service_account" "gce_account" {
  project    = var.common_params.project
  depends_on = [google_project_service.enabled_apis]
}
