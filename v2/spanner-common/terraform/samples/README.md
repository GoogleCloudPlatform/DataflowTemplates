## Terraform samples for common spanner setups

This repository provides samples for scenarios that emulate common customer
scenarios. These are especially useful is setting up test infrastructure that
resembles
a customers' environment, in order to run various Spanner migration tooling such
as -

1. [SMT](https://googlecloudplatform.github.io/spanner-migration-tool/)
2. [Live migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/README_Cloud_Datastream_to_Spanner.md)
3. [Bulk migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner_Flex.md)
4. [Reverse replication](TODO: Add appropriate link)

Pick a sample that is closest to your use-case, and use it as a starting point,
tailoring it to your own specific needs.

## Other Sample Repositories

The following sample repositories provide additional examples -

1. [Live migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/terraform/samples)
2. [Bulk migration](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/terraform/samples)

## List of examples

1. [MySQL in a VPC with Private Google Access + Spanner](mysql-vpc-spanner-test-infra/README.md)
2. [Configure a shared VPC with a service project](configure-shared-vpc/README.md)

## Sample structure

Each sample contains the following (and potentially more) files -

1. `main.tf` - This contains the Terraform resources which will be created.
2. `outputs.tf` - This declares the outputs that will be output as part of
   running the terraform example.
3. `variables.tf` - This declares the input variables that are required to
   configure the resources.
4. `terraform.tf` - This contains the required providers and APIs/project
   configurations for the sample.
5. `terraform.tfvars` - This contains the dummy inputs that need to be populated
   to run the example.
6. `terraform_simple.tfvars` - This contains the minimal list of dummy inputs
   that need to be populated to run the example.

## How to add a new sample

It is strongly recommended to copy an existing sample and modify it according to
the scenario you are trying to cover.
This ensures uniformity in the style in which terraform samples are written.

```shell
mkdir my-new-sample
cp -r mysql-vpc-spanner/* my-new-sample/
```