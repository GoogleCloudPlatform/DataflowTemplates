# Common Parameters
common_params = {
  project = "<YOUR_PROJECT_ID>" # Replace with your GCP project ID
  region  = "<YOUR_GCP_REGION>" # Replace with your desired GCP region
}

# Dataflow parameters
dataflow_params = {
  template_params = {
    instance_id             = "<YOUR_SPANNER_INSTANCE_ID>" # Spanner instance ID
    database_id             = "<YOUR_SPANNER_DATABASE_ID>" # Spanner database ID
    local_session_file_path = "session.json"
  }
  runner_params = {
    max_workers      = "<YOUR_MAX_WORKERS>"         # Maximum number of worker VMs
    num_workers      = "<YOUR_NUM_WORKERS>"         # Initial number of worker VMs
    machine_type     = "<YOUR_MACHINE_TYPE>"        # Machine type for worker VMs (e.g., "n2-standard-2")
    network          = "<YOUR_VPC_NETWORK>"         # VPC network for the Dataflow job
    subnetwork       = "<YOUR-FULL-PATH-SUBNETWORK" # Give the full path to the subnetwork
    ip_configuration = "WORKER_IP_PRIVATE"
  }
}

# Shards
shard_list = [
  {
    logicalShardId = "<YOUR_SHARD_ID1>"            # Value of the shard populated in Spanner
    host           = "<YOUR_IP_ADDRESS"            # Public or private IP address of shard
    user           = "<YOUR_USERNAME>"             # user of the source MySQL shard
    password       = "<YOUR_PASSWORD>",            # password of the source MySQL shard. For production migrations, use the secretManagerUri variable instead.
    port           = "<YOUR_PORT>"                 # Port, usually 3306
    dbName         = "<YOUR_SOURCE_DATABASE_NAME>" # name of the source database to replicate to
  },
  {
    logicalShardId = "<YOUR_SHARD_ID2>"
    host           = "<YOUR_IP_ADDRESS>"
    user           = "<YOUR_USERNAME>"
    password       = "<YOUR_PASSWORD>",
    port           = "<YOUR_PORT>"
    dbName         = "<YOUR_SOURCE_DATABASE_NAME>"
  }
]
