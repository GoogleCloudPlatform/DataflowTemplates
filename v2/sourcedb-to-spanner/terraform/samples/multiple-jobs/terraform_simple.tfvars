# Below is a simplified version of terraform.tfvars which only configures the
# most commonly set properties.
# It creates two bulk migration jobs with public IPs in the default network.

common_params = {
  on_delete             = "cancel" # Or "cancel" if you prefer
  project               = "your-google-cloud-project-id"
  region                = "us-central1" # Or your desired region
  projectId             = "your-google-cloud-project-id"
  service_account_email = "your-project-id-compute@developer.gserviceaccount.com"
}

jobs = [
  {
    instanceId   = "your-spanner-instance-id"
    databaseId   = "your-spanner-database-id"
    sourceDbURL  = "jdbc:mysql://127.0.0.1/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8"
    username     = "your-db-username"
    password     = "your-db-password"
    DLQDirectory = "gs://your-dlq-bucket/dlq1"
    max_workers  = 2
    num_workers  = 2
  },
  {
    instanceId   = "your-spanner-instance-id"
    databaseId   = "your-spanner-database-id"
    sourceDbURL  = "jdbc:mysql://127.0.0.1/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8"
    username     = "your-db-username"
    password     = "your-db-password"
    DLQDirectory = "gs://your-dlq-bucket/dlq2"
    max_workers  = 2
    num_workers  = 2
  },
]
