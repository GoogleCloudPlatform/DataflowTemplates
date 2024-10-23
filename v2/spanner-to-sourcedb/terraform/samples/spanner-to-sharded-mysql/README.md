## Sample Scenario: Spanner to Sharded MySQL reverse replication

> **_SCENARIO:_** This Terraform example illustrates launching a reverse replication
> jobs to replicate spanner writes for a sharded MySQL source, setting up all the required cloud infrastructure.
> **Details of MySQL shards are needed as input.**

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
- compute.firewalls.create
- compute.firewalls.delete
- compute.firewalls.update
- dataflow.jobs.cancel
- dataflow.jobs.create
- dataflow.jobs.updateContents
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
> For ease of use, this sample automatically adds the
> required
> roles to the service account used for running the migration. In order to
> do this, we need the `resourcemanager.projects.setIamPolicy` permission. If granting
> this role is unacceptable, please set
> the `var.common_params.add_policies_to_service_account`
> to **false**. This will skip adding the roles.
> They will have to be added manually. Note that if they are not added, **the
> migration will fail.**
> Two service accounts will need to be modified manually -
>    1. Dataflow service account - The list of roles can be found in the `main.tf`
      file, in the `reverse_replication_roles` resource.
>    2. GCS service account - The list of roles can be found in the `main.tf` file,
        in the `gcs_publisher_role` resource.

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these permissions to an existing service account.

### Using pre-defined roles

Following roles are required -

```shell
roles/dataflow.developer
roles/iam.securityAdmin
roles/iam.serviceAccountUser
roles/pubsub.admin
roles/storage.admin
roles/viewer
roles/compute.networkAdmin
roles/spanner.databaseUser
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
       file, in the `reverse_replication_roles` resource.

>    2. GCS service account - The list of roles can be found in the `main.tf` file,
  in the `gcs_publisher_role` resource.

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these roles to an existing service account.

## Assumptions

It takes the following assumptions -

1. Ensure that the MySQL instance is correctly setup.  
     1. Check that the MySQL credentials are correctly specified in the `tfvars` file. 
     2. Check that the MySQL server is up. 
     3. The MySQL user configured in the `tfvars` file should have [INSERT](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_insert), [UPDATE](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_update) and [DELETE](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_delete) privileges on the database. 
2. Ensure that the MySQL instance and Dataflow workers can establish connectivity with each other. Template automatically adds networking firewalls rules to enable this access. This can differ depending on the source configuration. Please validate the template rules and ensure that network connectivity can be established.
3. The MySQL instance with database containing reverse-replication compatible
   schema is created.
4. A session file has been generated to perform the spanner to MySQL schema mapping.

> **_NOTE:_**
[SMT](https://googlecloudplatform.github.io/spanner-migration-tool/quickstart.html)
> can be used to generate the session file.

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

1. **Firewall rules** - These rules allow Dataflow VMs to connect to each other
   and allow Dataflow VMs to connect to the source MySQL shards.
2. **GCS buckets** - A GCS bucket to hold reverse replication metadata, such as
   session and source shards files.
3. **GCS objects** - Generates source shards configuration file to upload to
   GCS.
4. **Pubsub topic and subscription** - This contains GCS object notifications as
   files are written to GCS for DLQ retrials.
5. **Spanner metadata database** - A Spanner metadata database to keep track of
   Spanner change streams.
6. **Spanner change stream** - A Spanner change stream to capture change capture
   data from the Spanner database under replication
7. **Dataflow job** - The Dataflow job which reads from Spanner change streams
   and writes to MySQL
8. **Permissions** - It adds the required roles to the specified (or the
   default) service accounts for the reverse replication to work.

> **_NOTE:_** A label is attached to all the resources created via Terraform.
> The key is `migration_id` and the value is auto-generated. The auto-generated
> value is used as a global identifier for a migration job across resources. The
> auto-generated value is always pre-fixed with a `smt-rev`.

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

dataflow_job_ids = [
  "<JOB_ID>",
]
dataflow_job_urls = [
  "https://console.cloud.google.com/dataflow/jobs/us-central1/<JOB_ID>",
]
```

**Note:** Each of the jobs will have a random prefix added to it to prevent name
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
This will result in Dataflow jobs will be launched inside the shared VPC.

> **_NOTE:_** Usage of shared VPC requires cross-project permissions. They
> are available as a Terraform
>template [here](../../../../spanner-common/terraform/samples/configure-shared-vpc/README.md).

> Dataflow service account permissions are
> documented [here](https://cloud.google.com/dataflow/docs/guides/specifying-networks#shared).

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

Look for the following log during `terraform apply` -

```shell
  # google_dataflow_flex_template_job.reverse_replication_job will be updated in-place
```

### Updating workers of a Dataflow job

Currently, the Terraform `google_dataflow_flex_template_job` resource does not
support updating the workers of a Dataflow job.
If the worker counts are changed in `tfvars` and a Terraform apply is run,
Terraform will attempt to cancel/drain the existing Dataflow job and replace it
with a new one.
**This is not recommended**. Instead, use the `gcloud` CLI to update the worker
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

### Source shard configuration file

Source shard configuration file that is supplied to the dataflow is automatically
created by Terraform. A sample file that this uploaded to GCS looks like
below - 

#### Using Secrets Manager to specify password 

```json
[
    {
    "logicalShardId": "shard1",
    "host": "10.11.12.13",
    "user": "root",
    "secretManagerUri":"projects/123/secrets/rev-cmek-cred-shard1/versions/latest",
    "port": "3306",
    "dbName": "db1"
    },
    {
    "logicalShardId": "shard2",
    "host": "10.11.12.14",
    "user": "root",
    "secretManagerUri":"projects/123/secrets/rev-cmek-cred-shard2/versions/latest",
    "port": "3306",
    "dbName": "db2"
    }
]
```

#### Specifying plaintext password

```json
[
    {
    "logicalShardId": "shard1",
    "host": "10.11.12.13",
    "user": "root",
    "password":"<YOUR_PWD_HERE>",
    "port": "3306",
    "dbName": "db1"
    },
    {
    "logicalShardId": "shard2",
    "host": "10.11.12.14",
    "user": "root",
    "password":"<YOUR_PWD_HERE>",
    "port": "3306",
    "dbName": "db2"
    }
]
```

This file is generated from the configuration provided in the `var.shard_list`
parameter.

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

## Observe, tune and troubleshoot

Follow the instructions provided [here](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/spanner-to-sourcedb/README.md#observe-tune-and-troubleshoot).

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