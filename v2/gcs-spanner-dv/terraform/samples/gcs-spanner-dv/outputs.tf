output "dataflow_job_url" {
  description = "The URL of the created Dataflow job in the Google Cloud Console."
  value       = "https://console.cloud.google.com/dataflow/jobs/${var.region}/${google_dataflow_flex_template_job.gcs_spanner_dv_job.job_id}?project=${var.project}"
}
