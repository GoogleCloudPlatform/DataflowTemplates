## Terraform samples for live migration

This repository provides samples for common scenarios users might have while trying to run a live migration to Spanner.

Pick a sample that is closest to your use-case, and use it as a starting point, tailoring it to your own specific needs.

## List of examples

1. [MySQL source to Spanner end-to-end](all-infrastructure/README.md)
2. [Pre-configured Datastream connection profiles](pre-configured-conn-profiles/README.md)

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

It is strongly recommended to copy an existing sample and modify it according to the scenario you are trying to cover.
This ensures uniformity in the style in which terraform samples are written.

```shell
mkdir my-new-sample
cp -r all-infrastructure/* my-new-sample/
```