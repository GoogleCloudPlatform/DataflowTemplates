## Sample Scenario: MySQL to Spanner using MySQL source configuration

> **_SCENARIO:_** This Terraform example illustrates launching a live migration
> job for a MySQL source, setting up all the required cloud infrastructure.
> **Only the source details are needed as input.**

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
2. MySQL source is accessible via Datastream either via
   [IP Whitelisting guide](https://cloud.google.com/datastream/docs/network-connectivity-options#ipallowlists)
   or [Private connectivity](https://cloud.google.com/datastream/docs/create-a-private-connectivity-configuration).
3. If using a VPC, VPC has already been configured to work with Datastream.
4. MySQL source has been configured to be read by Datastream by following
   [configure your source MySQL guide](https://cloud.google.com/datastream/docs/configure-your-source-mysql-database).
5. A Spanner instance with database containing the data-migration compatible
   schema is created.

> **_NOTE:_**
[SMT](https://googlecloudplatform.github.io/spanner-migration-tool/quickstart.html)
> can be used for converting a MySQL schema to a Spanner compatible schema.

Given these assumptions, it uses a supplied source database connection
configuration and creates the following resources -

1. **Datastream private connection** - If configured, a Datastream private
   connection will be deployed for your configured VPC. If not configured, IP
   whitelisting will be assumed as the mode of Datastream access.
2. **Source datastream connection profile** - This allows Datastream to connect
   to the MySQL instance (using IP whitelisting).
3. **GCS bucket** - A GCS bucket to for Datastream to write the source data to.
4. **Target datastream connection profile** - The connection profile to
   configure the created bucket in Datastream.
5. **Pubsub topic and subscription** - This contains GCS object notifications as
   files are written to GCS for consumption by the Dataflow job.
6. **Datastream stream** - A datastream stream which reads from the source
   specified in the source connection profile and writes the data to the bucket
   specified in the target connection profile. Note that it uses a mandatory
   prefix path inside the bucket where it will write the data to. The default
   prefix path is `data` (can be overridden).
7. **Bucket notification** - Creates the GCS bucket notification which publish
   to the pubsub topic created. Note that the bucket notification is created on
   the mandatory prefix path specified for the stream above.
8. **Dataflow job** - The Dataflow job which reads from GCS and writes to
   Spanner.
9. **Permissions** - It adds the required roles to the specified (or the
   default) service accounts for the live migration to work.

> **_NOTE:_** A label is attached to all the resources created via Terraform.
> The key is `migration_id` and the value is auto-generated. The auto-generated
> value is used as a global identifier for a migration job across resources. The
> auto-generated value is always pre-fixed with a `smt-`.

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

## FAQ

### Configuring to run using a VPC

#### Datastream

> **_NOTE:_** By default, **IP Whitelisting** based connectivity is assumed.

There is a variable of the type below in `variables.tf` -

```shell
private_connectivity = optional(object({
      private_connectivity_id = string
      vpc_name                = string
      range                   = string
    }))
```

To enable private connectivity based access for Datastream, specify this
configuration in your `*.tfvars`. For example -

```shell
 ...
 mysql_databases = [
    {
      database = "lorem"
    }
  ]
  private_connectivity = {
    private_connectivity_id = "abc"
    range                   = "xyz"
    vpc_name                = "pqr"
  }
  ...
```

Note that `vpc_name` and `range` are mandatory and for the private connectivity
configuration.

In the `mysql_host` configuration, specify the private IP instead of the
public IP.

This configuration creates a private connection configuration in Datastream
and configures it in the source profile created for the Datastream stream.

> **_NOTE:_** Private connectivity resource creation can take a long time to
> create.


> **ALERT:** Private connectivity resource destruction is currently not
> supported in Terraform due to the ability to delete nested
> resources: [#17920](https://github.com/hashicorp/terraform-provider-google/issues/17290),
> [#13054](https://github.com/hashicorp/terraform-provider-google/issues/13054).
> Until this is supported, the private connectivity resource will need to be
> manually deleted via the console or the gcloud CLI before running
> `terraform destroy`.
>
> Example -
>
>  `gcloud datastream private-connections delete 'private-conn-name' --location=us-central1 --force --quiet`
>
> You can run `terraform destroy` after deleting the private connection from the
> UI or the gcloud CLI to clean up the remaining resources.

If this is not specified, configurations are created assuming **IP Whitelisting
**.

#### Dataflow

1. Set the `network` and the `subnetwork` parameters to run the Dataflow job
   inside a VPC.
   Specify [network](https://cloud.google.com/dataflow/docs/guides/specifying-networks#network_parameter)
   and [subnetwork](https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter)
   according to the
   linked guidelines.
2. Set the `ip_configuration` to `WORKER_IP_PRIVATE` to disable public IP
   addresses for the worker VMs.

Note that the VPC should already exist. This template does not create a VPC.

### Updating template parameters for an existing job

Template parameters can be updated in place. Terraform and Dataflow will take
care of `UPDATING` a Dataflow job. This works internally by terminating the
existing job with an `UPDATED` state and creating a new job in its place. All
of this is done seamlessly by Dataflow and there is no risk to the fidelity of
an already executing job.

Example update: Changing `round_json_decimals` to `true` from `false`.

Look for the following log during `terraform apply` -

```shell
  # google_dataflow_flex_template_job.live_migration_job will be updated in-place
```

### Updating workers of a Dataflow job

Currently, the Terraform `google_dataflow_flex_template_job` resource does not
support updating the workers of a Dataflow job.
If the worker counts are changed in `tfvars` and a Terraform apply is run,
Terraform will attempt to cancel/drain the existing Dataflow job and replace it
with a new one.
**This is not recommended**. Instead use the `gcloud` CLI to update the worker
counts of a launched Dataflow job.

```shell
gcloud dataflow jobs update-options \                                                                                                                                                                                                                                                                                                                      (base) 
        --region=us-central1 \
        --min-num-workers=5 \
        --max-num-workers=20 \
      2024-06-17_01_21_44-12198433486526363702
```

### Long time to delete SMT bucket

A GCS bucket can only be deleted if all its comprising objects are deleted
first. This template attempts to delete all objects in the created GCS bucket
and then deletes the bucket as well.
**This may take a long time, depending on the size of the data in the bucket.**
If you want Terraform to only create the GCS bucket but skip its deletion
during `terraform destroy`, you will have to use the `terraform state rm` API to
delete GCS resource from being traced by Terraform after the `apply` command.

### Configuring Databases and Tables in Datastream

Which databases and tables to replicate can be configured via the following
variable definition -

In `variables.tf`, following definition exists -

```shell
mysql_databases = list(object({
      database = string
      tables   = optional(list(string))
    }))
```

To configure, create `*.tfvars` as follows -

```shell
mysql_databases = [
    {
      database = "<YOUR_DATABASE_NAME>"
      tables   = ["TABLE_1", "TABLE_2"]
      # Optionally list specific tables, or remove "tables" all together for all tables
    },
    {
      database = "<YOUR_DATABASE_NAME>"
      tables   = ["TABLE_1", "TABLE_2"]
      # Optionally list specific tables, or remove "tables" all together for all tables
    },
  ]
```