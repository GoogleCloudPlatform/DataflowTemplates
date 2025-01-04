locals {
  config          = yamldecode(file("../../config.yaml"))
  region          = local.config.global.region
  project_id      = local.config.global.project_id
  name            = local.config.spanner.name
  display_name    = local.config.spanner.display_name
  num_nodes       = local.config.spanner.num_nodes
  spanner_config  = local.config.spanner.config
  edition         = local.config.spanner.edition
}    