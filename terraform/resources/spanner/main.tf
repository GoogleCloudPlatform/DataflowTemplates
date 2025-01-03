module "spanner" {
  source = "../../modules/spanner"

  project_id    = local.project_id
  region        = local.region
  name          = local.name
  display_name  = local.display_name
  num_nodes     = local.num_nodes
  config        = local.spanner_config
  edition       = local.edition
}