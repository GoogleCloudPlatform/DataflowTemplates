module "bucket" {
  source = "../modules/cloud-storage"
  project_id                  = local.project_id
  bucket_name                 = local.bucket_name
  bucket_location             = local.region
  force_destroy               = true
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "inherited"

  random_bucket_suffix = false
}


# # Set public access to objects in the bucket
# resource "google_storage_bucket_iam_member" "public_access" {
#   bucket = module.bucket.bucket_name
#   role   = "roles/storage.objectViewer"
#   member = "allUsers"
# }