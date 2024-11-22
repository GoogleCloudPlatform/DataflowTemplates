terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0" # Or any other suitable version
    }
  }
}

provider "google" {
  project = var.project_id
}