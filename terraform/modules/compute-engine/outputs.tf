#############################
##    GCP VM - Output      ##
#############################

output "vm-name" {
  value = google_compute_instance.vm_instance.name
}

output "vm-internal-ip" {
  value = var.create_internal_static_ip ? google_compute_instance.vm_instance.network_interface.0.network_ip : null
}

output "vm-external-ip" {
  value = var.create_external_static_ip ? google_compute_address.external_static_ip[0].address : null
}