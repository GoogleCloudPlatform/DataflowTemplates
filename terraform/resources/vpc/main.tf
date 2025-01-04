module "vpc" {
  source = "../../modules/vpc"
  network_name              = local.name
  auto_create_subnetworks   = false
  project_id                = local.project_id
  description               = local.description
}