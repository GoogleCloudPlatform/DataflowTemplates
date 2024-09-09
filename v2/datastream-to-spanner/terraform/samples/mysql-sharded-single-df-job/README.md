## Sample Scenario: Sharded MySQL to Spanner production live migration using MySQL source configuration and a singlr dataflow job

> **_SCENARIO:_** This Terraform example illustrates launching a live migration
> jobs for a sharded MySQL source using a single dataflow job, setting up all the required cloud infrastructure.
> **Only the source details of physical shards are needed as input.**

## Terraform permissions

In order to create the resources in this sample,
the`Service account`/`User account` being used to run Terraform
should have the
required [permissions](https://cloud.google.com/iam/docs/manage-access-service-accounts#multiple-roles-console).
There are two ways to add permissions -

1. Adding pre-defined roles to the service account running Terraform.
2. Creating a custom role with the granular permissions and attaching it to the
   service account running Terraform.

### Using custom role and granular permissions (recommended)

Following permissions are required - 

```shell
- compute.globalAddresses.create
- compute.globalAddresses.createInternal
- compute.globalAddresses.delete
- compute.globalAddresses.deleteInternal
- compute.globalAddresses.get
- compute.globalOperations.get
- compute.networks.addPeering
- compute.networks.get
- compute.networks.listPeeringRoutes
- compute.networks.removePeering
- compute.networks.use
- compute.routes.get
- compute.routes.list
- compute.subnetworks.get
- compute.subnetworks.list
- dataflow.jobs.cancel
- dataflow.jobs.create
- dataflow.jobs.updateContents
- datastream.connectionProfiles.create
- datastream.connectionProfiles.delete
- datastream.privateConnections.create
- datastream.privateConnections.delete
- datastream.streams.create
- datastream.streams.delete
- datastream.streams.update
- iam.roles.get
- iam.serviceAccounts.actAs
- pubsub.subscriptions.create
- pubsub.subscriptions.delete
- pubsub.topics.attachSubscription
- pubsub.topics.create
- pubsub.topics.delete
- pubsub.topics.getIamPolicy
- pubsub.topics.setIamPolicy
- resourcemanager.projects.setIamPolicy
- storage.buckets.create
- storage.buckets.delete
- storage.buckets.update
- storage.objects.delete
- storage.objects.create
- serviceusage.services.use
- serviceusage.services.enable
```

**Note**: Add the `roles/viewer` role as well to the service account.

> **_Note on IAM:_** 
>
> 1. For ease of use, this sample automatically adds the
> required
> roles to the service account used for running the migration. In order to
> do this, we need the `resourcemanager.projects.setIamPolicy` permission. If
> granting
> this role is unacceptable, please set
> the `var.common_params.add_policies_to_service_account`
> to **false**. This will skip adding the roles.
> They will have to be added manually. Note that if they are not added, **the
> migration will fail.**
> Two service accounts will need to be modified manually -
>    1. Dataflow service account - The list of roles can be found in the `main.tf`
        file, in the `live_migration_roles` resource.
>    2. GCS service account - The list of roles can be found in the `main.tf` file,
        in the `gcs_publisher_role` resource.
> 
> 
> 2. In order to create private connectivity configuration for Datastream, 
> `compute.*` permissions are required, [as documented here](https://cloud.google.com/datastream/docs/create-a-private-connectivity-configuration#shared-vpc).
> Private connectivity cannot be created without these permissions. If you don't want to grant these permissions,
> you can use the [pre-configured connection profiles template](../pre-configured-conn-profiles/README.md). This template
> assumes you have created connection profiles outside of Terraform.

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these permissions to an existing service account.

### Using pre-defined roles

Following roles are required -

```shell
roles/dataflow.admin
roles/datastream.admin
roles/iam.securityAdmin
roles/iam.serviceAccountUser
roles/pubsub.admin
roles/storage.admin
roles/viewer
roles/compute.networkAdmin
```

> **_Note on IAM:_** 
> 
> 1. For ease of use, this sample automatically adds the
> required
> roles to the service account used for running the migration. In order to
> do this, we need the `roles/iam.securityAdmin` role. If granting
> this role is unacceptable, please set
> the `var.common_params.add_policies_to_service_account`
> to **false**. This will skip adding the roles.
> They will have to be added manually. Note that if they are not added, **the
> migration will fail.**
> Two service accounts will need to be modified manually -
>    1. Dataflow service account - The list of roles can be found in the `main.tf`
        file, in the `live_migration_roles` resource.
>    2. GCS service account - The list of roles can be found in the `main.tf` file,
        in the `gcs_publisher_role` resource.
>
>
>2. In order to create private connectivity configuration for Datastream,
>`networkAdmin` role is required, [as documented here](https://cloud.google.com/datastream/docs/create-a-private-connectivity-configuration#shared-vpc).
> Private connectivity cannot be created without these permissions. If you don't want to grant these permissions,
> you can use the [pre-configured connection profiles template](../pre-configured-conn-profiles/README.md). This template
> assumes you have created connection profiles outside of Terraform.

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these roles to an existing service account.

## Assumptions

It takes the following assumptions -

1. MySQL source is accessible via Datastream either via
   [IP Allowlisting guide](https://cloud.google.com/datastream/docs/network-connectivity-options#ipallowlists)
   or [Private connectivity](https://cloud.google.com/datastream/docs/create-a-private-connectivity-configuration).
2. If using a VPC, VPC has already been configured to work with Datastream.
3. MySQL source has been configured to be read by Datastream by following
   [configure your source MySQL guide](https://cloud.google.com/datastream/docs/configure-your-source-mysql-database).
4. A Spanner instance with database containing the data-migration compatible
   schema is created.

> **_NOTE:_**
[SMT](https://googlecloudplatform.github.io/spanner-migration-tool/quickstart.html)
> can be used for converting a MySQL schema to a Spanner compatible schema.

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

Given these assumptions, it uses a supplied source database connection
configuration and creates the following resources -

1. **Datastream private connection** - If configured, a Datastream private
   connection will be deployed for your configured VPC. If not configured, IP
   allowlisting will be assumed as the mode of Datastream access. A single private connection is created for all shards.
2. **Source datastream connection profiles** - This allows Datastream to connect
   to the MySQL instance (using IP allowlisting or private connectivity). A source connection profile is created per physical shard.
3. **GCS buckets** - A GCS bucket for Datastream to write the source data to. Each datastream job writes to a separate 
folder in `data/` directory, therefore, only 1 GCS bucket is created.
4. **Target datastream connection profiles** - The connection profile to
   configure the created bucket in Datastream.
5. **Pubsub topics and subscriptions** - This contains GCS object notifications as
   files are written to GCS for consumption by the Dataflow job.
6. **Datastream streams** - A datastream stream which reads from the source
   specified in the source connection profile and writes the data to the GCS bucket(shard specific directory inside the GCS bucket)
   specified in the target connection profile. Note that it uses a mandatory
   prefix path inside the bucket where it will write the data to. The default
   prefix path is `data/` (can be overridden). A stream is created per physical shard.
7. **Bucket notifications** - Creates the GCS bucket notification which publish
   to the pubsub topic created. Note that the bucket notification is created on
   the mandatory prefix path specified for the stream above.
8. **Dataflow job** - The Dataflow job which reads from GCS and writes to
   Spanner. A single dataflow job is created per migration.
9. **Permissions** - It adds the required roles to the specified (or the
   default) service accounts for the live migration to work.

> **_NOTE:_** A label is attached to all the resources created via Terraform.
> The key is `migration_id` and the value is auto-generated. The auto-generated
> value is used as a global identifier for a migration job across resources. The
> auto-generated value is always pre-fixed with a `smt-`.

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
resource_ids = {
  "dataflow_job" = "2024-09-05_08_21_04-2102092253544982231"
  "datastream_target_connection_profile" = "smt-glorious-lionfish-trgt-profile-1"
  "gcs_bucket" = "smt-glorious-lionfish-terraform-test"
  "pubsub_subscription" = "smt-glorious-lionfish-terraform-test-topic-sub"
  "pubsub_topic" = "smt-glorious-lionfish-terraform-test-topic"
  "shard1" = {
    "datastream_source_connection_profile" = "shard1-source-conn1"
    "datastream_stream" = "shard1-stream1"
  }
  "shard2" = {
    "datastream_source_connection_profile" = "shard2-source-conn2"
    "datastream_stream" = "shard2-stream2"
  }
}
resource_urls = {
  "dataflow_job" = "https://console.cloud.google.com/dataflow/jobs/us-central1/2024-09-05_08_21_04-2102092253544982231?project=span-cloud-ck-testing-external"
  "datastream_target_connection_profile" = "https://console.cloud.google.com/datastream/connection-profiles/locations/us-central1/instances/smt-glorious-lionfish-trgt-profile-1?project=span-cloud-ck-testing-external"
  "gcs_bucket" = "https://console.cloud.google.com/storage/browser/smt-glorious-lionfish-terraform-test?project=span-cloud-ck-testing-external"
  "pubsub_subscription" = "https://console.cloud.google.com/cloudpubsub/subscription/detail/smt-glorious-lionfish-terraform-test-topic-sub?project=span-cloud-ck-testing-external"
  "pubsub_topic" = "https://console.cloud.google.com/cloudpubsub/topic/detail/smt-glorious-lionfish-terraform-test-topic?project=span-cloud-ck-testing-external"
  "shard1" = {
    "datastream_source_connection_profile" = "https://console.cloud.google.com/datastream/connection-profiles/locations/us-central1/instances/shard1-source-conn1?project=span-cloud-ck-testing-external"
    "datastream_stream" = "https://console.cloud.google.com/datastream/streams/locations/us-central1/instances/shard1-stream1?project=span-cloud-ck-testing-external"
  }
  "shard2" = {
    "datastream_source_connection_profile" = "https://console.cloud.google.com/datastream/connection-profiles/locations/us-central1/instances/shard2-source-conn2?project=span-cloud-ck-testing-external"
    "datastream_stream" = "https://console.cloud.google.com/datastream/streams/locations/us-central1/instances/shard2-stream2?project=span-cloud-ck-testing-external"
  }
}
```

**Note:** Each of the jobs will have a random suffix added to it to prevent name
collisions.

### Cleanup

Once the jobs have finished running, you can clean up by running -

```shell
terraform destroy --var-file=terraform_simple.tfvars
```

#### Note on GCS buckets

The GCS bucket that is created will be cleaned up during `terraform destroy`.
If you want to exclude the GCS bucket from deletion due to any reason, you
can exclude it from the state file using `terraform state rm` command.

## FAQ

### Configuring to run using a VPC

#### Specifying a shared VPC

You can specify the shared VPC using the `host_project` configuration.
This will result in -

1. Datastream private connectivity link will be created in the shared VPC.
2. Dataflow jobs will be launched inside the shared VPC.

> **_NOTE:_** Usage of shared VPC requires cross-project permissions. They
> are available as a Terraform template [here](../../../../spanner-common/terraform/samples/configure-shared-vpc/README.md).
>
> 1. Datastream service account permissions are documented [here](https://cloud.google.com/datastream/docs/create-a-private-connectivity-configuration#shared-vpc).
> 2. Dataflow service account permissions are documented [here](https://cloud.google.com/dataflow/docs/guides/specifying-networks#shared).

#### Datastream Private Connectivity

> **_NOTE:_** By default, **IP Allowlisting** based connectivity is assumed.

There are two variables of the type below in `variables.tf` -

```shell
private_connectivity = optional(object({
      private_connectivity_id = string
      vpc_name                = string
      range                   = string
    }))
```

and

```shell
private_connectivity_id = optional(string)
```

You have the option of either specifying the `id` of an existing private
connectivity configuration or letting Terraform create one for you.

To re-use an existing private connectivity configuration, specify the `id` in
the `private_connectivity_id` variable.

Alternatively, to create a private connectivity configuration via Terraform for Datastream, specify this
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

Note that `vpc_name` and `range` are mandatory and for the [private connectivity
configuration](https://cloud.google.com/datastream/docs/create-a-private-connectivity-configuration).

In the `mysql_host` configuration, specify the private IP instead of the
public IP.

This configuration creates a private connection configuration in Datastream
and configures it in the source profile created for the Datastream stream.

> **_NOTE:_** Private connectivity resource creation can take a long time to
> create.


> **ALERT:** Private connectivity resource destruction is currently not
> supported in Terraform due to the ability to delete nested
>resources: [#17920](https://github.com/hashicorp/terraform-provider-google/issues/17290),
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

If this is not specified, configurations are created assuming **IP Allowlisting
**.

If you are facing issue with Datastream connectivity, check the following
Datastream [guide](https://cloud.google.com/datastream/docs/diagnose-issues#connectivity-errors)
to debug common networking issues.

#### Dataflow

1. Set the `network` and the `subnetwork` parameters to run the Dataflow job
   inside a VPC.
   Specify [network](https://cloud.google.com/dataflow/docs/guides/specifying-networks#network_parameter)
   and [subnetwork](https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter)
   according to the
   linked guidelines.
2. Set the `ip_configuration` to `WORKER_IP_PRIVATE` to disable public IP
   addresses for the worker VMs.

> **_NOTE:_** The VPC should already exist. This template does not create a VPC.

If you are facing issue with VPC connectivity, check the following Dataflow
[guide](https://cloud.google.com/dataflow/docs/guides/troubleshoot-networking)
to debug common networking issues.

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

### Specifying schema overrides

By default, the Dataflow job performs a like-like mapping between
source and Spanner. Any schema changes between source and Spanner can be
specified using the `session file`. To specify a session file -

1. Copy the SMT generated `session file` to the Terraform working directory. 
   Name this file `session.json`.
2. Set
   the `var.common_params.dataflow_params.template_params.local_session_file_path`
   variable to `"session.json"` (or the relative path to the name of the
   session file).

This will automatically upload the GCS bucket and configure it in the Dataflow
job.

### Specifying sharding context

Sharding context is used to populate the `migration_shard_id` column
added by SMT to each table of your schema. This allows the customer to trace
the source of each record in Spanner, back to the source shards.

By default, `migration_shard_id` is populated with
the `"${mysql_host_ip}-${dbName}"`.
Alternatively, for each shard, a sharding context file can be specified.
To specify a sharding context file -

1. Create a sharding context file with the following structure:
   ```
   {
     "StreamToDbAndShardMap": {
       "host-ip": {
         "db1": "shard_id1",
         "db2": "shard_id2"
       },
       ... Repeat for other source shards
     }
   }
   ```
2. Set
   the `var.common_params.dataflow_params.template_params.local_sharding_context_path`
   variable to the relative path from working directory to the sharding
   context file.

### Cross project writes to Spanner

The dataflow job can write to Spanner in a different project. In order to do so,
the service account running the Dataflow job needs to have the
`roles/spanner.databaseAdmin`role (or the corresponding permissions to write
data to Spanner).

After adding these permissions, configure the
`var.dataflow_params.template_params.spanner_project_id` variable.

### Adding access to Terraform service account

#### Using custom role and granular permissions (recommended)

You can run the following gcloud command to create a custom role in your GCP
project. 

```shell
gcloud iam roles create live_migrations_role --project=<YOUR-PROJECT-ID> --file=perms.yaml --quiet
```

The `YAML` file required for the above will be like so - 

```shell
title: "Live Migrations Custom Role"
description: "Custom role for Spanner live migrations."
stage: "GA"
includedPermissions:
- iam.roles.get
- iam.serviceAccounts.actAs
- datastream.connectionProfiles.create
....add all permissions from the list defined above.
```

Then attach the role to the service account - 

```shell
gcloud iam service-accounts add-iam-policy-binding <YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com \
    --member=<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com --role=projects/<YOUR-PROJECT-ID>/roles/live_migrations_role \
    --condition=CONDITION
```

#### Using pre-defined roles

You can run the following shell script to add roles to the service account
being used to run Terraform. This will have to done by a user which has the
authority to grant the specified roles to a service account -

```shell
#!/bin/bash

# Service account to be granted roles
SERVICE_ACCOUNT="<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com"

# Project ID where roles will be granted
PROJECT_ID="<YOUR-PROJECT-ID>"

# Array of roles to grant
ROLES=(
  "roles/<role1>"
  "roles/<role2>"
)

# Loop through each role and grant it to the service account
for ROLE in "${ROLES[@]}"
do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="$ROLE"
done
```

### Verifying access in the Terraform service account

#### Using custom role and granular permissions (recommended)

Verify that the custom role is attached to the service account - 

```shell
gcloud projects get-iam-policy <YOUR-PROJECT-ID>  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com"
```

Verify that the role has the correct set of permissions

```shell
gcloud iam roles describe live_migrations_role --project=<YOUR-PROJECT-ID> 
```

##### Using pre-defined roles
Once the roles are added, run the following command to verify them -

```shell
gcloud projects get-iam-policy <YOUR-PROJECT-ID>  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com"
```

Sample output -

```shell
ROLE
roles/dataflow.admin
roles/datastream.admin
roles/iam.securityAdmin
roles/iam.serviceAccountUser
roles/pubsub.admin
roles/storage.admin
roles/viewer
```

### Impersonating the Terraform service account

#### Using GCE VM instance (recommended)

A GCE VM created using the service account setup above will automatically
use the service account for all API requests triggered by Terraform. Running
terraform from such a GCE VM does not require downloading service keys and is
the recommended approach.

#### Using key file

1. Activate the service account -
   ```shell
   gcloud auth activate-service-account <YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com --key-file=path/to/key_file --project=project_id
   ```
2. Impersonate service account while fetching the ADC credentials -
   ```shell
   gcloud auth application-default login --impersonate-service-account <YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com
   ```

## Advanced Topics

### Specifying parallelism for Terraform

Terraform limits the number of concurrent operations while walking the graph.
[The default](https://developer.hashicorp.com/terraform/cli/commands/apply#parallelism-n)
is 10.

Increasing this value can
potentially speed up orchestration execution
when orchestrating a large sharded migration. We strongly recommend against
setting this value > 20. In most cases, the default value should suffice.

### Correlating the `count.index` with the `shard_id`

In the Terraform output, you should see resources being referred to by their
index. This is how Terraform works internally when it has to create multiple
resources of the same type. 

In order to correlate the `count.index` with the `shard_id` that is either
user specified or auto-generated, `terraform.tf.state` can be used. 

For example, a snippet of XX looks like - 

```json
{
      "mode": "managed",
      "type": "google_datastream_connection_profile",
      "name": "source_mysql",
      "provider": "provider[\"registry.terraform.io/hashicorp/google\"]",
      "instances": [
        {
          "index_key": 0,
          "schema_version": 0,
          "attributes": {
            "bigquery_profile": [],
            "connection_profile_id": "smt-elegant-ape-source-mysql",
            "create_without_validation": false,
            "display_name": "smt-elegant-ape-source-mysql",
            "effective_labels": {
               "migration_id": "smt-elegant-ape"
            }
          }
        }
      ]
    }
```

As you can see in this example, the `index_key` = `0`, correlates with the
auto-generated `shard_id` = `smt-elegant-ape`.