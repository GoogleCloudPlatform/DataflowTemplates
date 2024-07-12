common_params = {
  project = "<YOUR_PROJECT_ID>" # Replace with your GCP project ID
  region  = "<YOUR_GCP_REGION>" # Replace with your desired GCP region
}

# VPC, MySQL and Spanner will be created with default names specified in variables.tf
vpc_params = {}
mysql_params = [
  {
    vm_name = "<YOUR_MYSQL_SHARD_NAME>"
  }
]
spanner_params = {}


