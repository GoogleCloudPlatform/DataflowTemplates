output "dataflow_job_id" {
  value       = google_dataflow_flex_template_job.reverse_replication_job.id
  description = "Job id for the created Dataflow Flex Template job."
}

output "dataflow_job_url" {
  value       = "https://console.cloud.google.com/dataflow/jobs/${var.common_params.region}/${google_dataflow_flex_template_job.reverse_replication_job.id}"
  description = "URL for the created Dataflow Flex Template job."
}

output "dlq_poller_function_id" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? google_cloudfunctions2_function.dlq_poller[0].name : ""
  description = "Name of the created GCS DLQ Poller Cloud Function."
}

output "dlq_poller_function_url" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? "https://console.cloud.google.com/functions/details/${var.common_params.region}/${google_cloudfunctions2_function.dlq_poller[0].name}?project=${var.common_params.project}" : ""
  description = "URL for the created GCS DLQ Poller Cloud Function."
}

output "dlq_poller_scheduler_id" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? google_cloud_scheduler_job.dlq_poller_scheduler[0].name : ""
  description = "Name of the created Cloud Scheduler job for the GCS DLQ Poller."
}

output "dlq_poller_scheduler_url" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? "https://console.cloud.google.com/cloudscheduler/jobs/edit/${var.common_params.region}/${google_cloud_scheduler_job.dlq_poller_scheduler[0].name}?project=${var.common_params.project}" : ""
  description = "URL for the created Cloud Scheduler job for the GCS DLQ Poller."
}

output "cutback_monitoring_dashboard_id" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? basename(google_monitoring_dashboard.cutback_dashboard[0].id) : ""
  description = "ID of the created Cloud Monitoring dashboard for cutback verification."
}

output "cutback_monitoring_dashboard_url" {
  value       = var.common_params.create_cutback_monitoring_dashboard ? "https://console.cloud.google.com/monitoring/dashboards/builder/${basename(google_monitoring_dashboard.cutback_dashboard[0].id)}?project=${var.common_params.project}" : ""
  description = "URL for the created Cloud Monitoring dashboard for cutback verification."
}
