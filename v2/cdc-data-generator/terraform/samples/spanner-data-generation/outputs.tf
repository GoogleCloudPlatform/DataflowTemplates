output "resource_ids" {
  description = "IDs of resources created during setup."
  value = {
    gcs_bucket   = google_storage_bucket.generator_bucket.name
    dataflow_job = google_dataflow_flex_template_job.generator_job.job_id
  }
}

output "resource_urls" {
  description = "URLs to access resources in the Google Cloud Console."
  value = {
    gcs_bucket   = "https://console.cloud.google.com/storage/browser/${google_storage_bucket.generator_bucket.name}?project=${var.common_params.project}"
    dataflow_job = "https://console.cloud.google.com/dataflow/jobs/${var.common_params.region}/${google_dataflow_flex_template_job.generator_job.job_id}?project=${var.common_params.project}"
  }
  depends_on = [
    google_storage_bucket.generator_bucket,
    google_dataflow_flex_template_job.generator_job
  ]
}
