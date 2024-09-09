# Common Parameters
common_params = {
  project = "<YOUR_PROJECT_ID>" # Replace with your GCP project ID
  region  = "<YOUR_GCP_REGION>" # Replace with your desired GCP region
  datastream_params = {
    mysql_databases = [
      {
        database = "<YOUR_DATABASE_NAME>"
        tables   = [] # List specific tables to replicate (optional)
      }
      # Add more database objects if needed
    ]
  }
  dataflow_params = {
    template_params = {
      spanner_instance_id     = "<YOUR_SPANNER_INSTANCE_ID>" # Spanner instance ID
      spanner_database_id     = "<YOUR_SPANNER_DATABASE_ID>" # Spanner database ID
      local_session_file_path = "session.json"
    }
    runner_params = {
      max_workers      = "<YOUR_MAX_WORKERS>"         # Maximum number of worker VMs
      num_workers      = "<YOUR_NUM_WORKERS>"         # Initial number of worker VMs
      network          = "<YOUR_VPC_NETWORK>"         # VPC network for the Dataflow job
      subnetwork       = "<YOUR-FULL-PATH-SUBNETWORK" # Give the full path to the subnetwork
      ip_configuration = "WORKER_IP_PRIVATE"
    }
  }
}

# Shards
shard_list = [
  {
    datastream_params = {
      mysql_host     = "<YOUR_MYSQL_HOST>"     # MySQL host address
      mysql_username = "<YOUR_MYSQL_USERNAME>" # MySQL username
      mysql_password = "<YOUR_MYSQL_PASSWORD>" # MySQL password
      mysql_port     = "<YOUR_MYSQL_PORT>"     # MySQL port (typically 3306)
    }
  }
]
