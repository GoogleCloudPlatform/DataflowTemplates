terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 6.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.0, < 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.common_params.project
  region  = var.common_params.region
}

provider "google-beta" {
  project = var.common_params.project
  region  = var.common_params.region
}

# Get current project info
data "google_project" "project" {}

# Find default compute engine service account to assign roles if needed
data "google_compute_default_service_account" "gce_account" {
  depends_on = [google_project_service.enabled_apis]
}

# Enable required GCP service APIs
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "cloudprofiler.googleapis.com"
  ])
  service            = each.key
  project            = var.common_params.project
  disable_on_destroy = false
}