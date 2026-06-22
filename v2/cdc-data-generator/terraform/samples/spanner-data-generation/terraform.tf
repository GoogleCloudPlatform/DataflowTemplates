terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
  required_version = "~>1.2"
}

provider "google-beta" {
  project = var.common_params.project
  region  = var.common_params.region
}

provider "google" {
  project = var.common_params.project
  region  = var.common_params.region
}

resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "cloudprofiler.googleapis.com",
    "spanner.googleapis.com"
  ])
  service            = each.key
  project            = var.common_params.project
  disable_on_destroy = false
}

data "google_project" "project" {
}

data "google_compute_default_service_account" "gce_account" {
  depends_on = [google_project_service.enabled_apis]
}