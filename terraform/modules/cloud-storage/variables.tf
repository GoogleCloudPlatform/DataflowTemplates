variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "bucket_name" {
  description = "The name of the bucket"
  type        = string
}

variable "bucket_location" {
  description = "The Bucket Location"
  type        = string
}

variable "force_destroy" {
  description = "Should Storage Bucket needs to be forcefully destroyed"
  type        = bool
  default     = true
}

variable "storage_class" {
  description = "The Storage Class of the new bucket. Supported values include: STANDARD, MULTI_REGIONAL, REGIONAL, NEARLINE, COLDLINE, ARCHIVE"
  type        = string
  default     = "STANDARD"
}

variable "labels" {
  description = "A map of key/value label pairs to assign to the bucket"
  type        = map(string)
  default     = {}
}

variable "version_enabled" {
  description = "The versioning state of the bucket"
  type        = bool
  default     = false
}

variable "encryption" {
  description = "A Cloud KMS key that will be used to encrypt objects inserted into this bucket"
  type = object({
    default_kms_key_name = string // The id of a Cloud KMS key that will be used to encrypt objects inserted into this bucket
  })
  default = null
}

variable "uniform_bucket_level_access" {
  description = "Enables Uniform bucket-level access access to a bucket"
  type        = bool
  default     = true
}

variable "public_access_prevention" {
  description = "Flag to enable or disable public access prevention"
  type        = string
  default     = "enforced"
}

variable "random_bucket_suffix" {
  description = "Flag to determine whether to include a random suffix in the bucket name"
  type        = bool
  default     = false
}