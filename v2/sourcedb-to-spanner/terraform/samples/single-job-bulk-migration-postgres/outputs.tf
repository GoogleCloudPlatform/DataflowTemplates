output "dataflow_job_id" {
  value       = google_dataflow_flex_template_job.generated.job_id
  description = "Job id for the created Dataflow Flex Template job."
}

output "dataflow_job_url" {
  value       = "https://console.cloud.google.com/dataflow/jobs/${var.region}/${google_dataflow_flex_template_job.generated.job_id}"
  description = "URL for the created Dataflow Flex Template job."
}

