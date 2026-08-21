terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.0"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.0"
    }
  }
  required_version = ">= 1.2"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Enable needed GCP APIs for Spanner, Cloud SQL, Storage, and IAM.
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "sqladmin.googleapis.com",
    "spanner.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "compute.googleapis.com",
    "servicenetworking.googleapis.com"
  ])
  service            = each.key
  project            = var.project_id
  disable_on_destroy = false
}