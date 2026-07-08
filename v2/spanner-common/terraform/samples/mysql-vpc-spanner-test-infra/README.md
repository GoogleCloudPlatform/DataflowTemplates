## Sample Scenario: MySQL in a VPC with Spanner

> **_SCENARIO:_** This Terraform example illustrates launching multiple MySQL
> databases in GCE Compute instances inside a custom VPC subnet. It adds firewall
> rules to ensure that 1) Datastream can connect to the MySQL via private
> connectivity and 2) Dataflow VMs can communicate with each other. It also
> creates a Spanner instance and database to migrate
> data to.

## Terraform permissions

In order to create the resources in this sample,
the`Service account` being used to run Terraform
should have the
required [permissions](https://cloud.google.com/iam/docs/manage-access-service-accounts#multiple-roles-console).
There are two ways to add permissions -

1. Adding pre-defined roles to the service account running Terraform.
2. Creating a custom role with the granular permissions and attaching it to the
   service account running Terraform.

### Using custom role and granular permissions (recommended)

Following permissions are required -

```shell
- compute.disks.create
- compute.disks.setLabels
- compute.firewalls.create
- compute.firewalls.delete
- compute.firewalls.update
- compute.instances.create
- compute.instances.delete
- compute.instances.setMetadata
- compute.instances.setServiceAccount
- compute.instances.setTags
- compute.instances.update
- compute.networks.create
- compute.networks.delete
- compute.networks.update
- compute.networks.updatePolicy
- compute.subnetworks.create
- compute.subnetworks.delete
- compute.subnetworks.update
- compute.subnetworks.use
- compute.subnetworks.useExternalIp
- iam.roles.get
- iam.serviceAccounts.actAs
- spanner.databases.create
- spanner.databases.drop
- spanner.databases.updateDdl
- spanner.instances.create
- spanner.instances.delete
```

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these permissions to an existing service account.

Note: In addition to the above, grant the `roles/viewer` role as well.

### Using pre-defined roles

Following roles are required -

```shell
roles/spanner.admin
roles/compute.admin
roles/iam.serviceAccountUser
```

[This](#adding-access-to-terraform-service-account) section in the FAQ
provides instructions to add these permissions to an existing service account.

## Assumptions

1. Service account can used for running Terraform can be granted the above
   permissions.
2. MySQL and Spanner schemas are known.

## Resources Created

Given these assumptions, it uses a supplied source database connection
configuration and creates the following resources -

1. **VPC network** - A VPC network.
2. **VPC subnetwork** - A VPC subnetwork within the VPC network created.
3. **Firewall rules** - Rules to allow Dataflow VMs to communicate with each
   other and Datastream to connect to the MySQL instance via private
   connectivity.
4. **GCE VMs with MySQL shards** - Launches GCE VMs with MySQL setup on it inside
   the specified VPC subnet.
5. **Spanner instance** - A spanner instance with the specified configuration.
6. **Spanner database** - A spanner database inside the instance created.

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
  "mysql_db_ips" = [
    "10.128.0.3",
    "10.128.0.4",
    "10.128.0.2",
  ]
  "network" = "sample-vpc-network"
  "spanner_instance" = "sample-spanner-instance"
  "subnetwork" = "sample-vpc-subnetwork"
}
resource_urls = {
  "mysql_instances" = {
    "mysql-shard1" = "https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/mysql-shard1?project=<YOUR-PROJECT-ID>"
    "mysql-shard2" = "https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/mysql-shard2?project=<YOUR-PROJECT-ID>"
    "mysql-shard3" = "https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/mysql-shard3?project=<YOUR-PROJECT-ID>"
  }
  "network" = "https://console.cloud.google.com/networking/networks/details/sample-vpc-network?project=<YOUR-PROJECT-ID>"
  "spanner_instance" = "https://console.cloud.google.com/spanner/instances/sample-spanner-instance/details/databases?project=<YOUR-PROJECT-ID>"
  "subnetwork" = "https://console.cloud.google.com/networking/subnetworks/details/us-central1/sample-vpc-subnetwork?project=<YOUR-PROJECT-ID>"
}


```

**Note:** Each of the jobs will have a random suffix added to it to prevent name
collisions.

### Cleanup

Once the jobs have finished running, you can clean up by running -

```shell
terraform destroy --var-file=terraform_simple.tfvars
```

## FAQ

### How to connect to the instance?

OsLogin is the preferred method to ssh into GCE VM. OSLogin is already
configred on these instances. So you only need to add public key and able to
ssh.

It might be good idea to validate that ssh-agent is running and keys are added
to it.

Below is quick way to get it to work.

```bash
# Add your ssh public key. Ensure ssh-add -L | grep publickey gives output.
gcloud compute os-login ssh-keys add --key="$(ssh-add -L | grep publickey)" --project=<project-name>

# SSH into the instance
ssh ${USER}_google_com@nic0.mysql-db.<zone>.c.<project-name>.internal.gcpnode.com
```

### Changing the schema of the MySQL database

Set the `ddl` parameter in the `mysql_params`.

### Adding access to Terraform service account

#### Using custom role and granular permissions (recommended)

You can run the following gcloud command to create a custom role in your GCP
project.

```shell
gcloud iam roles create custom_role --project=<YOUR-PROJECT-ID> --file=perms.yaml --quiet
```

The `YAML` file required for the above will be like so -

```shell
title: "<ABC> Custom Role"
description: "Custom role for Spanner migrations."
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
    --member=<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com --role=projects/<your-project-id>/roles/custom_role \
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
roles/spanner.admin
roles/compute.admin
roles/iam.serviceAccountUser
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
