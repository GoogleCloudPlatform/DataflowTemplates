output "dataflow_job_ids" {
  value       = [for job in google_dataflow_flex_template_job.generated : job.job_id]
  description = "List of job IDs for the created Dataflow Flex Template jobs."
}

output "dataflow_job_urls" {
  value = [for job in google_dataflow_flex_template_job.generated : "https://console.cloud.google.com/dataflow/jobs/${var.common_params.region}/${job.job_id}"]
  description = "List of URLs for the created Dataflow Flex Template jobs."
}

