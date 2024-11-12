provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_spanner_instance" "spanner_instance" {
  name             = var.name
  display_name     = var.display_name
  config           = var.config #"regional-asia-south1" # Can be regional, dual-regional, or multi-regional
  num_nodes        = var.num_nodes
  # processing_units = 100 # For Enterprise edition
  # edition          = var.edition
  # labels = { 
  #   env = "dev"
  # }
}