output "resource_ids" {
  description = "IDs of resources created during set up."
  value = {
    host_project    = google_compute_shared_vpc_service_project.shared_vpc.host_project
    service_project = google_compute_shared_vpc_service_project.shared_vpc.service_project
  }
}