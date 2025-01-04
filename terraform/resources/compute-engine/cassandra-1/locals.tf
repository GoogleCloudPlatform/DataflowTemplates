locals {
  config                    = yamldecode(file("../../../config.yaml"))
  region                    = local.config.global.region
  project_id                = local.config.global.project_id
  instance_name             = local.config.cassandra.instance_name
  network                   = local.config.network.vpc_name
  zone                      = local.config.cassandra.zone
  instance                  = local.config.cassandra.instance
  subnetwork                = local.config.network.subnet_name
  instance_type             = local.config.cassandra.instance_type
  disk_size_gb              = local.config.cassandra.disk_size_gb
  disk_type                 = local.config.cassandra.disk_type 
  image                     = local.config.cassandra.image
  create_internal_static_ip = local.config.cassandra.create_internal_static_ip
  create_external_static_ip = local.config.cassandra.create_external_static_ip
  allow_stopping_for_update = local.config.cassandra.allow_stopping_for_update

  service_account = {
    email = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}    