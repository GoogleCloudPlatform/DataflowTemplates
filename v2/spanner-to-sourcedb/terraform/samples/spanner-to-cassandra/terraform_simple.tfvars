#Dataflow Template Bucket location
dataflow_template_bucket_location = "gs://<YOUR_DATAFLOW_BUCKET_PATH>" # Replace with your dataflow template bucket path

# Common Parameters
common_params = {
  project     = "<YOUR_PROJECT_ID>"   # Replace with your GCP project ID
  region      = "<YOUR_REGION"        # Replace with your desired GCP region
  target_tags = ["<YOUR_TARGET_TAGS"] # Target tags for the firewall rule (e.g. cassandra vm tags)
}

# Dataflow parameters
dataflow_params = {
  template_params = {
    instance_id = "<YOUR_SPANNER_INSTANCE_ID>"  # Spanner instance ID
    database_id = "<YOUR_CASSANDRA_DATABASE_ID" # Spanner database ID
    source_type = "<YOUR_SOURCE_TYPE>"          # Source type of database. Should be cassandra
  }
  runner_params = {
    max_workers      = "<YOUR_MAX_WORKERS>"          # Maximum number of worker VMs
    num_workers      = "<YOUR_NUM_WORKER>"           # Initial number of worker VMs
    machine_type     = "<YOUR_MACHINE_TYPE>"         # Machine type for worker VMs (e.g., "n2-standard-2")
    network          = "<YOUR_VPC_NETWORK>"          # VPC network for the Dataflow job
    subnetwork       = "<YOUR-FULL-PATH-SUBNETWORK>" # Give the full path to the subnetwork
    ip_configuration = "WORKER_IP_PUBLIC"
    subnetwork_cidr  = "<YOUR_SUBNETWORK_CIDR>"
  }
}

shard_config = {
  host             = "<YOUR_CASSANDRA_HOST>"
  port             = "<YOUR_CASSANDRA_PORT>"
  username         = "<YOUR_CASSANDRA_USERNAME>"
  password         = "<YOUR_CASSaNDRA_PASSWORD>"
  keyspace         = "<YOUR_CASSANDra_KEYSPACE>"
  consistencyLevel = "LOCAL_QUORUM"
  sslOptions       = false
  protocolVersion  = "v5"
  dataCenter       = "datacenter1"
  localPoolSize    = "2"
  remotePoolSize   = "1"
}

cassandra_template_config_file = "./cassandra-config-template.conf"