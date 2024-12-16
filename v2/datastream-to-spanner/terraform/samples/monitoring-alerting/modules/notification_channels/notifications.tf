# Define a notification channel for Google Cloud Monitoring to send alerts via email
resource "google_monitoring_notification_channel" "email" {
  # Display name for the notification channel in the Monitoring dashboard
  display_name = "Email Notification"

  # Specify the type of notification channel, in this case, email
  type = "email"

  # Configure the email address to receive the notifications, using the variable passed in
  labels = {
    email_address = var.email_address
  }
}

# Output the notification channel ID, which can be referenced by other resources or modules
output "notification_channels" {
  value = [
    google_monitoring_notification_channel.email.id
  ]
}