locals {
  config                  = yamldecode(file("../../config.yaml"))
  region                  = local.config.global.region
  project_id              = local.config.global.project_id
  name                    = local.config.network.vpc_name
  auto_create_subnetworks = local.config.network.auto_create_subnetworks
  description             = local.config.network.description
}    