## Sample Scenario: MySQL in a VPC with Spanner

> **_SCENARIO:_** This Terraform example illustrates launching a MySQL 5.7
> in a GCE Compute instance inside a custom VPC subnet. It adds firewall
> rules to ensure that 1) Datastream can connect to the MySQL via private
> connectivity and 2) Dataflow VMs can communicate with each other. It also creates a Spanner instance and database to migrate
> data to. 

It takes the following assumptions -

1. `Service account`/`User account` being used to run Terraform
   has [permissions](https://cloud.google.com/iam/docs/manage-access-service-accounts#multiple-roles-console)
   to create and destroy -
    1. VPC network
    2. VPC subnetwork
    3. Firewall rules
    4. Compute engine instances
   5. Spanner instance and database

> **_NOTE:_** 

Given these assumptions, it uses a supplied source database connection
configuration and creates the following resources -

1. **VPC network** - A VPC network.
2. **VPC subnetwork** - A VPC subnetwork within the VPC network created.
3. **Firewall rules** - Rules to allow Dataflow VMs to communicate with each other and Datastream to connect to the MySQL instance via private connectivity.
4. **GCE VM with MySQL** - Launches a GCE VM with MySQL 5.7 setup on it inside the specified VPC subnet.
5. **Spanner instance** - A spanner instance with the specified configuration.
6. **Spanner database** - A spanner database inside the instance created.

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


```

**Note:** Each of the jobs will have a random suffix added to it to prevent name
collisions.

### Cleanup

Once the jobs have finished running, you can clean up by running -

```shell
terraform destroy
```

## FAQ

### Changing the schema of the MySQL database
