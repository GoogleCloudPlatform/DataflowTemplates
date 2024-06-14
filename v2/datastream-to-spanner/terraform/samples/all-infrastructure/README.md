## Sample Scenario: MySQL to Spanner using MySQL source configuration

> **_SCENARIO:_** This Terraform example illustrates launching a live migration
> job
> for a MySQL
> source, setting up all the required cloud infrastructure. **Only the source
> details are needed as input.**

It takes the following assumptions -

1. `Service account`/`User account` being used to run Terraform
   has [permissions](https://cloud.google.com/iam/docs/manage-access-service-accounts#multiple-roles-console)
   to create and destroy -
    1. Datastream connection profiles
    2. Datastream streams
    3. GCS buckets
    4. Pubsub topics
    5. Pubsub subscriptions
    6. Dataflow jobs
2. MySQL source has whitelisted Datastream public IPs for access via
   the [IP Whitelisting guide](https://cloud.google.com/datastream/docs/network-connectivity-options#ipallowlists).
3. MySQL source has been configured to be read by Datastream by following
   the [configure your source MySQL guide](https://cloud.google.com/datastream/docs/configure-your-source-mysql-database).
4. Service account used to run Dataflow has permissions to write to Spanner, in
   addition
   to [other required permissions](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions).

Given these assumptions, it uses a supplied source database connection
configuration and creates the following resources -

1. **Source datastream connection profile** - This allows Datastream to connect
   to the MySQL instance (using IP whitelisting).
2. **GCS bucket** - A GCS bucket to for Datastream to write the source data to.
3. **Target datastream connection profile** - The connection profile to
   configure the created bucket in Datastream.
4. **Pubsub topic and subscription** - This contains GCS object notifications as
   files are written to GCS for consumption by the Dataflow job.
5. **Bucket notification** - Creates the GCS bucket notification which publish
   to the pubsub topic created.
6. **Dataflow job** - The Dataflow job which reads from GCS and writes to
   Spanner.

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

## How to run

1. Clone this repository or the sample locally.
2. Edit the `terraform.tfvars` or `terraform_simple.tfvars` file and replace the
   dummy variables with real values. Extend the configuration to meet your
   needs. It is recommended to get started with `terraform_simple.tfvars`.
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
Apply complete! Resources: 13 added, 0 changed, 0 destroyed.

Outputs:

resource_ids = {
  "dataflow_job" = "2024-06-14_03_01_00-3421054840094926119"
  "datastream_source_connection_profile" = "source-mysql-thorough-wombat"
  "datastream_stream" = "mysql-stream-thorough-wombat"
  "datastream_target_connection_profile" = "target-gcs-thorough-wombat"
  "gcs_bucket" = "live-migration-thorough-wombat"
  "pubsub_subscription" = "live-migration-thorough-wombat-sub"
  "pubsub_topic" = "live-migration-thorough-wombat"
}
resource_urls = {
  "dataflow_job" = "https://console.cloud.google.com/dataflow/jobs/us-central1/2024-06-14_03_01_00-3421054840094926119?project=your-project-here"
  "datastream_source_connection_profile" = "https://console.cloud.google.com/datastream/connection-profiles/locations/us-central1/instances/source-mysql-thorough-wombat?project=your-project-here"
  "datastream_stream" = "https://console.cloud.google.com/datastream/streams/locations/us-central1/instances/mysql-stream-thorough-wombat?project=your-project-here"
  "datastream_target_connection_profile" = "https://console.cloud.google.com/datastream/connection-profiles/locations/us-central1/instances/target-gcs-thorough-wombat?project=your-project-here"
  "gcs_bucket" = "https://console.cloud.google.com/storage/browser/live-migration-thorough-wombat?project=your-project-here"
  "pubsub_subscription" = "https://console.cloud.google.com/cloudpubsub/subscription/detail/live-migration-thorough-wombat-sub?project=your-project-here"
  "pubsub_topic" = "https://console.cloud.google.com/cloudpubsub/topic/detail/live-migration-thorough-wombat?project=your-project-here"
}
```

**Note:** Each of the jobs will have a random suffix added to it to prevent name
collisions.

### Cleanup

Once the jobs have finished running, you can clean up by running -

```shell
terraform destroy
```

#### Note on GCS buckets

The GCS bucket that is created will be cleaned up during `terraform destroy`.
If you want to exclude the GCS bucket from deletion due to any reason, you
can exclude it from the state file using `terraform state rm` command.