common_params = {
  project = "<YOUR_GCP_PROJECT_ID>"
  region  = "<YOUR_DESIRED_REGION>"
}

vpc_params = {
  vpc_name              = "<YOUR_CUSTOM_VPC_NAME>"
  subnetwork_name       = "<YOUR_CUSTOM_SUBNET_NAME>"
  subnetwork_cidr_range = "<YOUR_CUSTOM_SUBNET_CIDR_RANGE>"
}

mysql_params = [
  {
    vm_name      = "<YOUR_MYSQL_VM_NAME>"
    machine_type = "<YOUR_MYSQL_MACHINE_TYPE>"
    zone         = "<YOUR_MYSQL_ZONE>"
  }
]

spanner_params = {
  config           = "<YOUR_SPANNER_INSTANCE_CONFIG>"
  name             = "<YOUR_SPANNER_INSTANCE_NAME>"
  display_name     = "<YOUR_SPANNER_DISPLAY_NAME>"
  processing_units = "<YOUR_DESIRED_PROCESSING_UNITS>"
  database_name    = "<YOUR_SPANNER_DATABASE_NAME>"
  ddl              = []
}