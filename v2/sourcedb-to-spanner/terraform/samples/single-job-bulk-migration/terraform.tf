terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0" # Or the latest compatible version
    }
  }
  required_version = "~>1.2"
}

provider "google" {
  project = var.project
  region  = var.region
}

provider "google-beta" {
  project = var.project
  region  = var.region
}

#Enable the APIs
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "spanner.googleapis.com"
  ])
  service            = each.key
  project            = var.project
  disable_on_destroy = false
}
# To fetch project number
data "google_project" "project" {
}

# Fetch the service account for GCS
data "google_storage_project_service_account" "gcs_account" {
  depends_on = [google_project_service.enabled_apis]
}

# Fetch the default service account for Compute Engine (used by Dataflow)
data "google_compute_default_service_account" "gce_account" {
  depends_on = [google_project_service.enabled_apis]
}