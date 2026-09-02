# Spanner Integration Test Infrastructure (Terraform)

This directory contains Terraform scripts to provision the Google Cloud infrastructure
required to run Spanner integration tests for Dataflow Templates.

## Intended Audience

This setup is intended for external contributors who need to stand up a self-contained
test environment before running the Spanner-related integration tests in the `it/` directory.

## What Gets Provisioned
The Terraform scripts provision the following resources:
- **Spanner Instance**: A regional Spanner instance.
- **GCS Bucket**: Used for staging artifacts (named by `gcs_bucket_name` or defaults to `<project_id>-it-infra-bucket`).
- **Datastream Private Connectivity**: For private network connection between Datastream and VPC. *(Note: The ID `"datastream-connect-2"` is currently hardcoded as integration tests implicitly expect this specific connection name)*.
- **Cloud SQL Instances**:
    - PostgreSQL instance (Private IP only, `ENTERPRISE_PLUS` edition, `db-perf-optimized-N-16`)
    - MySQL instance (Private IP only, `db-n1-standard-1`)
- **Compute Instance (VM)**: A consolidated VM (`it-infra-vm`) used for running tests and proxying Datastream traffic. It runs Cloud SQL Proxy and initializes environment with Docker, Maven, Git, OpenJDK, `gh` (GitHub CLI), and `jq`.
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

3. **Configure Remote State**: Before initializing, update the `bucket` property in `backend.tf` to an existing GCS bucket in your project for storing Terraform state. Alternatively, you can pass it dynamically during initialization:
   ```shell
   terraform init -backend-config="bucket=<YOUR_STATE_BUCKET>"
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
| `postgres_instance_name` | The name of the PostgreSQL instance | No | `it-infra-pg-db-instance` |
| `mysql_instance_name` | The name of the MySQL instance | No | `it-infra-mysql-db-instance` |
| `it_infra_vm_name` | The name of the consolidated VM for tests and proxy | No | `it-infra-vm` |