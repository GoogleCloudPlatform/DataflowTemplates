module "cassandra-1" {
  source                    = "../../../modules/compute-engine"
  instance_name             = local.instance_name
  project                   = local.project_id
  network_project_id        = local.project_id
  region                    = local.region
  zone                      = local.zone
  instance                  = local.instance
  network                   = local.network
  subnetwork                = local.subnetwork
  instance_type             = local.instance_type
  disk_size_gb              = local.disk_size_gb
  disk_type                 = local.disk_type
  image                     = local.image
  # service_account           = local.service_account
  create_internal_static_ip = local.create_internal_static_ip
  create_external_static_ip = local.create_external_static_ip
  allow_stopping_for_update = local.allow_stopping_for_update

  tags   = ["cassandra-1"]
   labels = { environment = "dev", creation-mode = "tf", os = "ubuntu" }
}

data "google_project" "project" {
  project_id = local.project_id 
}
