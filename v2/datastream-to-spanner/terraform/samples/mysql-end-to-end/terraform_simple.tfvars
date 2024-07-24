# Common Parameters
common_params = {
  project = "<YOUR_PROJECT_ID>" # Replace with your GCP project ID
  region  = "<YOUR_GCP_REGION>" # Replace with your desired GCP region
}

# Datastream Parameters
datastream_params = {
  mysql_host = "<YOUR_MYSQL_HOST_IP_ADDRESS>"
  # Use the Public IP if using IP allowlisting and Private IP if using
  # private connectivity.
  mysql_username = "<YOUR_MYSQL_USERNAME>"
  mysql_password = "<YOUR_MYSQL_PASSWORD>"
  mysql_port     = 3306
  mysql_database = {
    database = "<YOUR_DATABASE_NAME>"
    tables   = []
    # Optionally list specific tables, or remove "tables" all together for all tables
  }
  private_connectivity_id = "<YOUR_PRIVATE_CONNECTIVITY_ID>"
  # Only one of `private_connectivity_id` or `private_connectivity` block
  # may exist. Use `private_connectivity_id` to specify an existing
  # private connectivity configuration, and the `private_connectivity` to
  # create a new one via Terraform.
  private_connectivity = {
    private_connectivity_id = "<YOUR_PRIVATE_CONNECTIVITY_ID>"
    # ID of the private connection you want to create in Datastream.
    vpc_name = "<YOUR_VPC_NAME>"
    # The pre-existing VPC which will be peered to Datastream.
    range = "<YOUR_RESERVED_RANGE>"
    # The IP range to be reserved for Datastream.
  }
  # If the private_connectivity block or private_connectivity_id is not specified,
  # IP allowlisting will be assumed.
}

# Dataflow Parameters
dataflow_params = {
  template_params = {
    spanner_database_id = "<YOUR_SPANNER_DATABASE_ID>"
    # ID of the target Cloud Spanner database
    spanner_instance_id = "<YOUR_SPANNER_INSTANCE_ID>"
    # ID of the target Cloud Spanner instance
  }
  runner_params = {
    max_workers = 10
    num_workers = 4
    on_delete   = "cancel"
    network     = "<YOUR_VPC_NETWORK>"
    subnetwork  = "<YOUR_SUBNETWORK_NAME>"
    # subnetwork is passed "as-is". This is intentionally kept like so to
    # allow for shared VPC configurations. Learn more about subnetwork
    # configuration at: https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter
    ip_configuration = "WORKER_IP_PRIVATE"
    # Keep this WORKER_IP_PRIVATE to disable public IPs for Dataflow workers.
    # This will require enabling private google access for the subnetwork being
    # used. Otherwise remove this configuration to enable public IPs.
  }
}
