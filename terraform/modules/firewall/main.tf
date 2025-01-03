resource "google_compute_firewall" "rules" {
  project     = var.project
  name        = var.name
  network     = var.network
  description = var.description

  allow {
    protocol = var.protocol
    ports    = var.ports
  }

  source_ranges = var.source_ranges

  target_tags = var.target_tags
}