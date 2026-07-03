terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
  }
  required_version = ">=1.2"
}

provider "google" {
  project = var.project
  region  = var.region
}

provider "google-beta" {
  project = var.project
  region  = var.region
}

# Enable the APIs (Added compute.googleapis.com)
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "spanner.googleapis.com",
    "compute.googleapis.com"
  ])
  service            = each.key
  project            = var.project
  disable_on_destroy = false
}

# Data sources
data "google_project" "project" {}
data "google_compute_default_service_account" "gce_account" {
  depends_on = [google_project_service.enabled_apis]
}
