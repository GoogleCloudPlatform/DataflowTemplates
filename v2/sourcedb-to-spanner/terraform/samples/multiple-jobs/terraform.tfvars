common_params = {
  on_delete                    = "drain"  # Or "cancel" if you prefer
  project                      = "your-google-cloud-project-id"
  region                       = "us-central1"  # Or your desired region
  jdbcDriverJars               = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
  jdbcDriverClassName          = "com.mysql.jdbc.Driver"
  projectId                    = "your-cloud-spanner-project-id"
  spannerHost                  = "https://batch-spanner.googleapis.com"
  sessionFilePath              = "gs://your-bucket/session-file.json"
  extraFilesToStage            = "gs://your-bucket/extra-file.txt"
  additional_experiments       = ["enable_stackdriver_agent_metrics"]
  autoscaling_algorithm        = "THROUGHPUT_BASED"
  enable_streaming_engine      = true
  network                      = "default"
  subnetwork                   = "regions/us-central1/subnetworks/your-subnetwork"
  sdk_container_image          = "gcr.io/dataflow-templates/latest/flex/java11"
  service_account_email        = "your-service-account-email@your-project-id.iam.gserviceaccount.com"
  skip_wait_on_job_termination = false
  staging_location             = "gs://your-staging-bucket"
  temp_location                = "gs://your-temp-bucket"
}

jobs = [
  {
    instanceId            = "your-spanner-instance-id"
    databaseId            = "your-spanner-database-id"
    sourceDbURL           = "jdbc:mysql://127.0.0.1/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8"
    username              = "your-db-username"
    password              = "your-db-password"
    tables                = "table1,table2"
    numPartitions         = 200
    maxConnections        = 50
    DLQDirectory          = "gs://your-dlq-bucket/job1-dlq"
    defaultLogLevel       = "INFO"
    ip_configuration      = "WORKER_IP_PRIVATE"
    kms_key_name          = "projects/your-project-id/locations/global/keyRings/your-key-ring/cryptoKeys/your-key"
    launcher_machine_type = "n1-standard-2"
    machine_type          = "n1-standard-2"
    max_workers           = 10
    name                  = "bulk-migration-job"
    num_workers           = 5
  },
  {
    instanceId            = "your-spanner-instance-id"
    databaseId            = "your-spanner-database-id"
    sourceDbURL           = "jdbc:mysql://another-db-host:3306/different-db"
    username              = "another-username"
    password              = "another-password"
    tables                = "table1,table2"
    numPartitions         = 200
    maxConnections        = 25
    DLQDirectory          = "gs://your-dlq-bucket/job2-dlq"
    defaultLogLevel       = "DEBUG"
    ip_configuration      = "WORKER_IP_PRIVATE"
    kms_key_name          = "projects/your-project-id/locations/global/keyRings/your-key-ring/cryptoKeys/your-key"
    launcher_machine_type = "n1-standard-4"
    machine_type          = "n1-standard-4"
    max_workers           = 20
    name                  = "job2-orders-migration"
    num_workers           = 10
  }
  # ... Add more job configurations as needed
]
