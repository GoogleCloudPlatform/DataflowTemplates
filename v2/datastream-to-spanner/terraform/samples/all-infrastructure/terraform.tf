terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"   # Or the latest compatible version
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
resource "google_project_service" "dataflow_api" {
  service            = "dataflow.googleapis.com"
  project            = var.common_params.project
  disable_on_destroy = false
}

resource "google_project_service" "datastream_api" {
  service            = "datastream.googleapis.com"
  project            = var.common_params.project
  disable_on_destroy = false
}

resource "google_project_service" "storage_api" {
  service            = "storage.googleapis.com"
  project            = var.common_params.project
  disable_on_destroy = false
}

resource "google_project_service" "pubsub_api" {
  service            = "pubsub.googleapis.com"
  project            = var.common_params.project
  disable_on_destroy = false
}
