resource "google_pubsub_topic" "topic" {
  name    = var.topic_name
  project = var.project_id 
}

resource "google_pubsub_subscription" "sub" {
  name    = var.sub_name
  topic   = google_pubsub_topic.topic.name
  project = var.project_id
}