module "firewall-cassandra" {
  source = "../../modules/firewall"

  project       = local.project_id
  name          = local.cassandra_name
  network       = local.network
  description   = local.cassandra_description 
  protocol      = local.cassandra_protocol
  ports         = local.cassandra_ports
  source_ranges = local.cassandra_source_ranges
  target_tags   = local.cassandra_target_tags
}

module "firewall-mysql" {
  source = "../../modules/firewall"

  project       = local.project_id
  name          = local.mysql_name
  network       = local.network
  description   = local.mysql_description 
  protocol      = local.mysql_protocol
  ports         = local.mysql_ports
  source_ranges = local.mysql_source_ranges
  target_tags   = local.mysql_target_tags
}

module "firewall-ssh" {
  source = "../../modules/firewall"

  project       = local.project_id
  name          = local.ssh_name
  network       = local.network
  description   = local.ssh_description 
  protocol      = local.ssh_protocol
  ports         = local.ssh_ports
  source_ranges = local.ssh_source_ranges
  target_tags   = local.ssh_target_tags
}

