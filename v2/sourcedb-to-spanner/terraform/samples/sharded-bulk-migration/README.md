## Sample Scenario: Sharded MySQL to Spanner production bulk migration

> **_SCENARIO:_** This Terraform example illustrates launching bulk (backfill/historical) migration Dataflow jobs for a
> Sharded MySQL to Spanner, setting up all the required sharding config files appropriately.

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
- compute.firewalls.create
- compute.firewalls.delete
- compute.firewalls.update
- dataflow.jobs.cancel
- dataflow.jobs.create
- dataflow.jobs.updateContents
- iam.roles.get
- iam.serviceAccounts.actAs
- resourcemanager.projects.setIamPolicy
- storage.objects.delete
- storage.objects.create
- serviceusage.services.use
- serviceusage.services.enable
```

**Note**: Add the `roles/viewer` role as well to the service account.

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these permissions to an existing service account.

### Using pre-defined roles

Following roles are required -

```shell
roles/dataflow.admin 
roles/iam.securityAdmin
roles/iam.serviceAccountUser
roles/storage.admin
roles/viewer
roles/compute.networkAdmin
```

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these roles to an existing service account.

## Dataflow permissions

The Dataflow service account needs to be provided the `roles/storage.objectAdmin` and `roles/spanner.databaseAdmin`
roles. Also ensure that the workers can access the source shards.

## Assumptions

It makes the following assumptions -

> **_NOTE:_**
[SMT](https://googlecloudplatform.github.io/spanner-migration-tool/quickstart.html)
> can be used for converting a MySQL schema to a Spanner compatible schema.

1. Dataflow can establish network connectivity with the MySQL source shards.
2. Appropriate permissions are added to the service account running Terraform to allow resource creation.
3. Appropriate permissions are provided to the service account running Dataflow to write to Spanner.
4. A GCS working directory has been created for the terraform template. The dataflow jobs will write the output
   records (DLQ/filtered events) in this working directory.
5. A Spanner instance with database containing the data-migration compatible schema is created. This includes a
   migration_shard_id column being present in the Spanner schema.
6. An SMT generated session file is provided containing the schema mapping information is created locally. This is
   mandatory for sharded migrations. Check out the FAQ
   on [how to generate a session file](#specifying-schema-overrides).
7. A sharding config containing source connection information of all the physical and logical shards is created locally.
8. The Source Schema should be the same across all shards.

Given these assumptions, it copies data from multiple source MySQL databases to the configured Spanner database.

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
7. `shardingConfig.json` - This contains a sample sharding config that need to be populated with the source shards
   connection details.

## Resources Created

Given these assumptions, it uses a supplied source database connection
configuration and creates the following resources -

1. **Dataflow job(s)** - The Dataflow job(s) which reads from source shards and writes to
   Spanner. One dataflow job can migrate multiple physical shards based on the `batch_size` parameter.
2. **GCS Session File** - The GCS object created by uploading the local session file.
3. **GCS Source Configs** - The GCS object(s) created by splitting the local sharding config into various batches, with
   1 source config per dataflow job.
4. **Permissions** - It adds the required roles to the specified (or the
   default) service accounts for the bulk migration to work.

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

dataflow_job_ids = [
  "2024-06-05_00_41_11-4759981257849547781",
]
dataflow_job_urls = [
  "https://console.cloud.google.com/dataflow/jobs/us-central1/2024-06-05_00_41_11-4759981257849547781",
]
```

**Note 1:** Each of the jobs will have a random name which will be migrating 1 or more physical shards.

**Note 2:** Terraform, by default, creates at most 10 resources in parallel. If you want to run more than 10 dataflow
jobs in parallel, run terraform using the `-parallelism=n` flag. However, more parallel dataflow jobs linearly increases
load on Spanner.
If Spanner is not sized sufficiently, all the jobs will slow down.

```shell
terraform apply -parallelism=30
```

### Cleanup

Once the jobs have finished running, you can cleanup by running -

```shell
terraform destroy
```

## Observability

TODO

## FAQ

### Dataflow job is failing with "Timeout in polling result file"

Dataflow has a 10min timeout within which the launcher VM logic should complete. There could be multiple reasons for it
to take over 10 mins:

- **Job logs not present after the log "launcher VM started":** A sign would be there are only 3-4 log statements in the
  job logs. This likely due to private google access is not enabled for the
  subnetwork. Please enable private google access in your network.
- **Job logs not present after "Discovering tables for DataSource":** This means dataflow is unable to access the mysql
  shard. Please check your network configuration and credentials.
- If you are still getting a Timeout after this, this means the job graph construction is taking too long. You could
  potentially have too many tables allocated in a single Dataflow job. The recommendation is to set the `batch_size`
  such that the total number of tables in a single job does not exceed `150`. This means if a logical shard has 15
  tables, and a physical shard has 2 logical shards, do not set the batch_size more than 5.

### Job graph is not loading/Custom counters not visible on Dataflow panel

For very large graphs, this can happen. The graph section would be empty and the counters won't load. But worry not, the
migration should progress nonetheless. In such cases, the Dataflow custom metrics can be directly viewed
on [Cloud monitoring](https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf).

### Data being written to Spanner is too slow

There can be multiple reasons for this.

- **Check source shard metrics:** Are memory/CPU limits being hit? Consider increasing the shard resources.
- **Check Spanner metrics:** Are memory/CPU limits being hit? Consider increasing the number of nodes.
- **Check Dataflow metrics:** Are memory/CPU limits being hit? Dataflow should autoscale to required number of
  nodes.
    - CPU/memory limits being hit means the `max-workers` parameter might be too low. It is recommended to use smaller
      machines (`Ex: n1-highmem-4`) for most workloads. However, if you have tall tables (singe table larger than 1tb),
      we recommend using larger machine sizes (`n1-highmem-32`).
    - For tables with `string` primary keys, the throughput is expected to be lower than tables with `int` primary keys.
    - Consider bumping up `maxConnection` and `numPartition` parameters when launching the jobs. We recommend `320` for
      maxConnections and `10000` for numPartitions.

### Allowing network connectivity

Ensure Dataflow VMs are able to access the MySQL instance. This would require:

- Configuring MySQL to allow connections from the Dataflow IPs.
- Configuring the firewall to allow ingress TCP connection on the MySQL port
  from the Dataflow IPs.

#### Configuring connectivity on MySQL on GCE

We recommend running such instances and dataflow inside a VPC ensuring Google
private access is enabled.
If the target does not allow the dataflow IPs, we recommend the following configurations
for allowing connectivity:

- Using network tags to configure MySQL VM. Apply the `databases` network
  tag on the GCE VM.
- Add firewall rule that allows tcp ingress on port 3306 from resources
  with the `dataflow` [network tag](https://cloud.google.com/vpc/docs/add-remove-network-tags) on targets
  with the `databases` tag.

### Configuring to run using a VPC

#### Dataflow

1. Set the `network` and the `subnetwork` parameters to run the Dataflow job
   inside a VPC.
   Specify [network](https://cloud.google.com/dataflow/docs/guides/specifying-networks#network_parameter)
   and [subnetwork](https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter)
   according to the linked guidelines.
2. Set the `ip_configuration` to `WORKER_IP_PRIVATE` to disable public IP
   addresses for the worker VMs.
3. If only certain source network tags are allowlisted via a firewall, specify the  
   network tags via
   the [additional-experiments flag](https://cloud.google.com/dataflow/docs/guides/routes-firewall#network-tags-flex).
   Dataflow automatically assigns
   the `dataflow` network tag if any network tag is additionally specified. You need
   specify the tag for both worker VMs and launcher VMs.

> **_NOTE:_** You can use a shared VPC by specifying the `host_project` in the subnet path.
> This will result in the Dataflow jobs being launched inside the shared VPC.
> Usage of shared VPC requires cross-project permissions. They
> are available as a Terraform
> template [here](../../../../spanner-common/terraform/samples/configure-shared-vpc/README.md).
> Dataflow service account permissions are
> documented [here](https://cloud.google.com/dataflow/docs/guides/specifying-networks#shared).

If you are facing issue with VPC connectivity, check the following Dataflow
[guide](https://cloud.google.com/dataflow/docs/guides/troubleshoot-networking)
to debug common networking issues.

#### Specifying a VPC network for source

The VPC subnet specified in Dataflow should be able to access the source database.
There are different VPC network configurations possible on the source, for each of which,
different methods exists to allow connectivity.

For example, for MySQL running on GCE, one can specify the VPC network and subnetwork
of the GCE instance hosting MySQL.
Ensure the firewall rules are configured so that other addresses can connect
to the source.

### Updating workers of a Dataflow job

Currently, the Terraform `google_dataflow_flex_template_job` resource does not
support updating the workers of a Dataflow job.
If the worker counts are changed in `tfvars` and a Terraform apply is run,
Terraform will attempt to cancel the existing Dataflow job and replace it
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

### Specifying schema overrides

By default, the bulk job performs a like-like mapping between
source and Spanner. However, for a sharded migration, a session file is mandatory for migration.
Any schema changes between source and Spanner can be
specified using the `session file`.

To generate a session file:

1. Setup SMT
   and [launch the UI](https://googlecloudplatform.github.io/spanner-migration-tool/ui#launching-the-web-ui-for-spanner-migration-tool).
2. Perform
   a [sharded schema conversion](https://googlecloudplatform.github.io/spanner-migration-tool/ui/schema-conv/sharded-migration.html#sharded-migration-schema-changes)
   and download the session file locally.

To specify a session file -

1. Copy the SMT generated `session file` to the Terraform working directory.
   Name this file `session.json`.
2. Set
   the `var.common_params.local_session_file_path`
   variable to `"session.json"` (or the relative/absolute path to the name of the
   session file).

This will automatically upload it to the working directory and configure it in the Dataflow
job.

### Overriding Dataflow workers per shard/job

Currently, per job configurations are not supported in this terraform template.

### Cross project writes to Spanner

The dataflow job can write to Spanner in a different project. In order to do so,
the service account running the Dataflow job needs to have the
`roles/spanner.databaseAdmin` role (or the corresponding permissions to write
data to Spanner).

After adding these permissions, configure the
`var.common_params.projectId` variable denoting the Cloud Spanner project.

### Adding access to Terraform service account

#### Using custom role and granular permissions (recommended)

You can run the following gcloud command to create a custom role in your GCP
project.

```shell
gcloud iam roles create bulk_migrations_role --project=<YOUR-PROJECT-ID> --file=perms.yaml --quiet
```

The `YAML` file required for the above will be like so -

```shell
title: "Bulk Migrations Custom Role"
description: "Custom role for Spanner Bulk migrations."
stage: "GA"
includedPermissions:
- iam.roles.get
- iam.serviceAccounts.actAs
....add all permissions from the list defined above.
```

Then attach the role to the service account -

```shell
gcloud iam service-accounts add-iam-policy-binding <YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com \
    --member=<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com --role=projects/<YOUR-PROJECT-ID>/roles/bulk_migrations_role \
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
gcloud iam roles describe bulk_migrations_role --project=<YOUR-PROJECT-ID> 
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
roles/iam.serviceAccountUser
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
