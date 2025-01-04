output "topic_id" {
  description = "Topic ID"
  value       = google_pubsub_topic.topic.id
}

output "topic_name" {
  description = "Topic Name"
  value       = google_pubsub_topic.topic.name
}

output "sub_name" {
  description = "Subscription Name"
  value       = google_pubsub_subscription.sub.name
}

output "sub_id" {
  description = "Subscription ID"
  value       = google_pubsub_subscription.sub.id
}
