# Spanner Integration Test Infrastructure (Terraform)

This directory contains Terraform scripts to provision the Google Cloud infrastructure
required to run Spanner integration tests for Dataflow Templates.

## Intended Audience

This setup is intended for **vendors** and **PSOs** who need to stand up a self-contained
test environment before running the Spanner-related integration tests in the `it/` directory.

## What Gets Provisioned

The Terraform scripts provision the following resources:
- **Spanner Instance**: A regional Spanner instance.
- **GCS Bucket**: Used for staging artifacts (named by `gcs_bucket_name` or defaults to `<project_id>-it-infra-bucket`).
- **Datastream Private Connectivity**: For private network connection between Datastream and VPC.
- **Cloud SQL Instances**:
    - PostgreSQL instance (Private IP only)
    - MySQL instance (Private IP only)
- **Compute Instance (VM)**: A consolidated VM (`it-infra-vm`) used for running tests and proxying Datastream traffic. It runs Cloud SQL Proxy and initializes environment with Docker, Maven, Git, and OpenJDK.
- **IAM Roles**: Custom role `it_infra_role` with permissions for Dataflow, GCS, and Compute, bound to the default compute service account.
- **Firewall Rule**: Allows Datastream to access the proxy on ports 3306 and 5432.

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
   terraform plan -var="project_id=<YOUR_PROJECT_ID>"
   ```

5. Apply the configuration:
   ```shell
   terraform apply -var="project_id=<YOUR_PROJECT_ID>"
   ```

6. Once testing is complete, tear down all provisioned resources:
   ```shell
   terraform destroy -var="project_id=<YOUR_PROJECT_ID>"
   ```

## Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `project_id` | The GCP project ID to deploy resources into | Yes | N/A |
| `region` | The GCP region to use | No | `us-central1` |
| `zone` | The GCP zone to use | No | `us-central1-a` |
| `network` | The VPC network to use | No | `default` |
| `subnetwork` | The subnetwork to use | No | `default` |
| `spanner_instance_name` | The name of the Spanner instance | No | `it-infra-spanner` |
| `gcs_bucket_name` | The name of the GCS bucket | No | `""` (defaults to `project_id-it-infra-bucket`) |
| `datastream_private_connection_id` | The ID of the Datastream private connection | No | `datastream-connect-2` |
| `github_repo_owner` | The owner of the GitHub repo for the runner | No | `GoogleCloudPlatform` |
| `github_repo_name` | The name of the GitHub repo for the runner | No | `DataflowTemplates` |
| `github_token` | The GitHub token for the runner | No | `""` |
| `gh_runner_version` | The version of the GitHub runner | No | `2.311.0` |
| `private_ip_range_name` | The name of the reserved IP range for Private Service Access | No | `it-infra-private-ip-range` |
| `postgres_instance_name` | The name of the PostgreSQL instance | No | `it-infra-pg-db-instance` |
| `mysql_instance_name` | The name of the MySQL instance | No | `it-infra-mysql-db-instance` |
| `it_infra_vm_name` | The name of the consolidated VM for tests and proxy | No | `it-infra-vm` |

## Cleanup

After running tests, you can also use the cleanup scripts in `test-infra/` to remove
any residual Spanner resources:

```shell
cd test-infra/
python cleanup_spanner_databases.py --project=<YOUR_PROJECT_ID>
bash delete_spanner_instances.sh
```
