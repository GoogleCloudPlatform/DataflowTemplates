## Scenario

This Terraform example illustrates launching multiple bulk migration Datafllow jobs for a MySQL to Spanner migration with the following assumptions -

1. MySQL source can establish network connectivity with Dataflow.
2. Appropriate permissions are added to the service account running Terraform to allow resource creation.
3. Appropriate permissions are provided to the service account running Dataflow to write to Spanner.
4. A GCS bucket has been provided to write the DLQ records to.

Given these assumptions, it copies data from multiple source MySQL databases to the configured Spanner database(s).

## Description

This sample contains the following files -

1. `main.tf` - This contains the Terraform resources which will be created.
2. `outputs.tf` - This declares the outputs that will be output as part of running this terraform example.
3. `variables.tf` - This declares the input variables that are required to configure the resources.
4. `terraform.tf` - This contains the required providers for this sample.
5. `terraform.tfvars` - This contains the dummy inputs that need to be populated to run this example.

## How to run 

1. Clone this repository or the sample locally. 
2. Edit the `terraform.tfvars` file and replace the dummy variables with real values. Extend the configuration to meet your needs.
3. Run the following commands - 

### Initialise Terraform

```shell
# Initialise terraform - You only need to do this once for a directory.
terraform init
```

### Run `plan` and `apply` 

Validate the terraform files with - 

```shell
terraform plan
```

Run the terraform script with - 

```shell
terraform apply
```

This will launch the configured jobs and produce an output like below -

```shell
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.

Outputs:

dataflow_job_ids = [
  "2024-06-05_00_41_11-4759981257849547781",
]
dataflow_job_urls = [
  "https://console.cloud.google.com/dataflow/jobs/us-central1/2024-06-05_00_41_11-4759981257849547781",
]
```

**Note:** Each of the jobs will have a random suffix added to it to prevent name collisions. 

### Cleanup

Once the jobs have finished running, you can cleanup by running - 

```shell
terraform destroy
```
