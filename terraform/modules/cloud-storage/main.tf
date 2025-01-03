resource "random_id" "suffix" {
  count       = var.random_bucket_suffix ? 1 : 0
  byte_length = 2
}

resource "google_storage_bucket" "bucket" {
  name                        = var.random_bucket_suffix ? join("-", [var.bucket_name, lower(random_id.suffix[0].id)]) : var.bucket_name
  location                    = upper(var.bucket_location)
  force_destroy               = var.force_destroy
  project                     = var.project_id
  storage_class               = upper(var.storage_class)
  public_access_prevention    = var.public_access_prevention
  labels                      = var.labels
  uniform_bucket_level_access = var.uniform_bucket_level_access

  versioning {
    enabled = var.version_enabled
  }

  dynamic "encryption" {
    for_each = var.encryption == null ? [] : [var.encryption]
    content {
      default_kms_key_name = encryption.value.default_kms_key_name
    }
  }
}