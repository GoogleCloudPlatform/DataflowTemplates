module "vpc_subnets" {
  source        = "../../modules/subnets"
  project_id    = local.project_id
  network_name  = local.network_name
  
  subnets = [
    {
      subnet_name           = local.subnet_name
      subnet_ip             = local.subnet_ip
      subnet_region         = local.region
      subnet_private_access = "true"
      subnet_flow_logs      = true
      description           = local.description
    }
  ]
}