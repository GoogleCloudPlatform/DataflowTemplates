locals {
  config                 = yamldecode(file("../../config.yaml"))
  region                 = local.config.global.region
  network                = local.config.network.vpc_name  
  project_id             = local.config.global.project_id
  cassandra_name         = local.config.firewall-cassandra.name  
  cassandra_description  = local.config.firewall-cassandra.description 
  cassandra_protocol     = local.config.firewall-cassandra.protocol
  cassandra_ports        = local.config.firewall-cassandra.ports
  cassandra_source_ranges = local.config.firewall-cassandra.source_ranges
  cassandra_target_tags  = local.config.firewall-cassandra.target_tags

  mysql_name             = local.config.firewall-mysql.name  
  mysql_description      = local.config.firewall-mysql.description 
  mysql_protocol         = local.config.firewall-mysql.protocol
  mysql_ports            = local.config.firewall-mysql.ports
  mysql_source_ranges     = local.config.firewall-mysql.source_ranges
  mysql_target_tags      = local.config.firewall-mysql.target_tags

  ssh_name             = local.config.firewall-ssh.name  
  ssh_description      = local.config.firewall-ssh.description 
  ssh_protocol         = local.config.firewall-ssh.protocol
  ssh_ports            = local.config.firewall-ssh.ports
  ssh_source_ranges     = local.config.firewall-ssh.source_ranges
  ssh_target_tags      = local.config.firewall-ssh.target_tags
}    