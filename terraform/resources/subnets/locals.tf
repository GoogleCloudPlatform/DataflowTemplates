locals {
  config                  = yamldecode(file("../../config.yaml"))
  region                  = local.config.global.region
  project_id              = local.config.global.project_id
  network_name            = local.config.network.vpc_name
  subnet_name             = local.config.network.subnet_name
  subnet_ip               = local.config.network.subnet_ip
  description             = local.config.network.subnet_description
}    