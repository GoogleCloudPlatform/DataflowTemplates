module "pubsub" {
  source = "../../modules/pubsub"

  project_id = local.project_id
  topic_name = local.topic
  sub_name   = local.sub
}