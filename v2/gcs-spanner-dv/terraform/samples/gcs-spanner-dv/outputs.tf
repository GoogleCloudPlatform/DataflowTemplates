output "dataflow_job_url" {
  description = "URL for the created Dataflow Flex Template job."
  value       = "https://console.cloud.google.com/dataflow/jobs/${var.region}/${google_dataflow_flex_template_job.gcs_spanner_dv_job.job_id}?project=${google_dataflow_flex_template_job.gcs_spanner_dv_job.project}"
}

output "dataflow_job_id" {
  description = "The unique ID of the created Dataflow Flex Template job."
  value       = google_dataflow_flex_template_job.gcs_spanner_dv_job.job_id
}
