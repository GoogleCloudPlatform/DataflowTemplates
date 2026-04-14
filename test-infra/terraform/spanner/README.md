# Spanner Integration Test Infrastructure (Terraform)

This directory contains Terraform scripts to provision the Google Cloud infrastructure
required to run Spanner integration tests for Dataflow Templates.

## Intended Audience

This setup is intended for **vendors** and **PSOs** who need to stand up a self-contained
test environment before running the Spanner-related integration tests in the `it/` directory.

## What Gets Provisioned

> _Add a description of the resources your Terraform scripts create, e.g.:_
> - Cloud Spanner instance and database
> - IAM service accounts and bindings
> - GCS buckets for staging artifacts
> - Any additional resources required by the tests

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) authenticated with sufficient permissions
- A Google Cloud project with billing enabled

## Usage

1. Authenticate with Google Cloud:
   ```shell
   gcloud auth application-default login
   ```

2. Navigate to this directory:
   ```shell
   cd test-infra/terraform/spanner
   ```

3. Initialize Terraform:
   ```shell
   terraform init
   ```

4. Review the planned changes:
   ```shell
   terraform plan -var="project=<YOUR_PROJECT_ID>"
   ```

5. Apply the configuration:
   ```shell
   terraform apply -var="project=<YOUR_PROJECT_ID>"
   ```

6. Once testing is complete, tear down all provisioned resources:
   ```shell
   terraform destroy -var="project=<YOUR_PROJECT_ID>"
   ```

## Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `project` | Google Cloud project ID | Yes |

> _Add additional variables here as your scripts evolve._

## Cleanup

After running tests, you can also use the cleanup scripts in `test-infra/` to remove
any residual Spanner resources:

```shell
cd test-infra/
python cleanup_spanner_databases.py --project=<YOUR_PROJECT_ID>
bash delete_spanner_instances.sh
```
