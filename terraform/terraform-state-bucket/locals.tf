locals {
  config        = yamldecode(file("../config.yaml"))
  region        = local.config.global.region
  project_id    = local.config.global.project_id
  bucket_name   = local.config.global.state_bucket
}    