output "dataflow_job_id" {
  value       = google_dataflow_flex_template_job.reverse_replication_job.id
  description = "Job id for the created Dataflow Flex Template job."
}

output "dataflow_job_url" {
  value       = "https://console.cloud.google.com/dataflow/jobs/${var.common_params.region}/${google_dataflow_flex_template_job.reverse_replication_job.id}"
  description = "URL for the created Dataflow Flex Template job."
}

output "dlq_poller_service_id" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? google_cloud_run_v2_service.dlq_poller[0].name : ""
  description = "Name of the Cloud Run DLQ poller service."
}

output "dlq_poller_service_url" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? "https://console.cloud.google.com/run/detail/${var.common_params.region}/${google_cloud_run_v2_service.dlq_poller[0].name}/metrics?project=${var.common_params.project}" : ""
  description = "URL for the Cloud Run DLQ poller service."
}

output "dlq_poller_scheduler_id" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? google_cloud_scheduler_job.dlq_poller_scheduler[0].name : ""
  description = "Name of the Cloud Scheduler job for the DLQ poller."
}

output "dlq_poller_scheduler_url" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? "https://console.cloud.google.com/cloudscheduler/jobs/edit/${var.common_params.region}/${google_cloud_scheduler_job.dlq_poller_scheduler[0].name}?project=${var.common_params.project}" : ""
  description = "URL for the Cloud Scheduler job for the DLQ poller."
}

output "cutback_monitoring_dashboard_id" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? split("/", google_monitoring_dashboard.cutback_dashboard[0].id)[3] : ""
  description = "ID of the Cutback Monitoring Dashboard."
}

output "cutback_monitoring_dashboard_url" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? "https://console.cloud.google.com/monitoring/dashboards/custom/${split("/", google_monitoring_dashboard.cutback_dashboard[0].id)[3]}?project=${var.common_params.project}" : ""
  description = "URL for the Cutback Monitoring Dashboard."
}

