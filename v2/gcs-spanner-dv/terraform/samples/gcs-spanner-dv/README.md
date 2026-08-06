## Sample Scenario: GCS Spanner Data Validation

> **_SCENARIO:_** This Terraform example illustrates launching GCS Spanner Data Validation Dataflow job.

## Terraform permissions

In order to create the resources in this sample,
the `Service account`/`User account` being used to run Terraform
should have the
required [permissions](https://cloud.google.com/iam/docs/manage-access-service-accounts#multiple-roles-console).
There are two ways to add permissions -

1. Adding pre-defined roles to the service account running Terraform.
2. Creating a custom role with the granular permissions and attaching it to the
   service account running Terraform.

### Using custom role and granular permissions (recommended)

Following permissions are required -

```shell
- dataflow.jobs.cancel
- dataflow.jobs.create
- dataflow.jobs.updateContents
- iam.roles.get
- iam.serviceAccounts.actAs
- resourcemanager.projects.setIamPolicy
- storage.objects.create
- storage.objects.get
- storage.objects.list
- bigquery.datasets.get
- bigquery.tables.create
- bigquery.tables.get
- bigquery.tables.updateData
- bigquery.jobs.create
- serviceusage.services.use
- serviceusage.services.enable
```

**Note**: Add the `roles/viewer` role as well to the service account.

### Using pre-defined roles

Following roles are required -

```shell
roles/dataflow.admin 
roles/iam.securityAdmin
roles/iam.serviceAccountUser
roles/storage.admin
roles/bigquery.admin
roles/viewer
```

## Dataflow permissions

The Dataflow service account needs to be provided the `roles/storage.objectViewer`, `roles/spanner.databaseReader`, `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` roles.

## Assumptions

It makes the following assumptions -

1. Appropriate permissions are added to the service account running Terraform to allow resource creation.
2. Appropriate permissions are provided to the service account running Dataflow to read from GCS and Spanner, and write results to BigQuery.
3. GCS directory containing source AVRO files is already present.
4. Destination Spanner instance and database are already created.
5. A BigQuery dataset has been created where the validation results will be stored.

## Description

This sample contains the following files -

1. `main.tf` - This contains the Terraform resources which will be created.
2. `outputs.tf` - This declares the outputs that will be output as part of
   running this terraform example.
3. `variables.tf` - This declares the input variables that are required to
   configure the resources.
4. `terraform.tf` - This contains the required providers and APIs/project
   configurations for this sample.
5. `terraform.tfvars` - This contains the dummy inputs that need to be populated
   to run this example.
6. `terraform_simple.tfvars` - This contains the minimal list of dummy inputs
   that need to be populated to run this example.

## Resources Created

It creates the following resources -

1. **Dataflow job** - The Dataflow job which reads from GCS and Spanner and compares them.
2. **Permissions** - It adds the required roles to the specified (or the
   default) service accounts for the validation job to work.

## How to run

1. Clone this repository or the sample locally.
2. Edit the `terraform.tfvars` file and replace the dummy variables with real values. Extend the configuration to meet
   your needs. It is recommended to get started with `terraform_simple.tfvars`.
3. Run the following commands -

### Initialise Terraform

```shell
# Initialise terraform - You only need to do this once for a directory.
terraform init
```

### Run `plan` and `apply`

Validate the terraform files with -

```shell
terraform plan --var-file=terraform_simple.tfvars
```

Run the terraform script with -

```shell
terraform apply --var-file=terraform_simple.tfvars
```

This will launch the configured jobs and produce an output like below -

```shell
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.

Outputs:

dataflow_job_id = "2024-06-05_00_41_11-4759981257849547781"
dataflow_job_url = "https://console.cloud.google.com/dataflow/jobs/us-central1/2024-06-05_00_41_11-4759981257849547781"
```

### Cleanup

Once the jobs have finished running, you can cleanup by running -

```shell
terraform destroy
```
