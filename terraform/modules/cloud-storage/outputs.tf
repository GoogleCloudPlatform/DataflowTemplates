output "bucket_url" {
  description = "Storage Bucket url"
  value       = google_storage_bucket.bucket.url
}
output "bucket_name" {
  description = "Storage Bucket Name"
  value       = google_storage_bucket.bucket.name
}