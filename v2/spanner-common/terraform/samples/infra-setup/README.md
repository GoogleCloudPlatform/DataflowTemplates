# Sharded Source & Spanner Target Infrastructure Setup

This directory contains production-ready Terraform configurations to securely spin up, configure, and tear down a complete sharded database environment on Google Cloud Platform. 

It provides an end-to-end sandbox establishing multiple Cloud SQL physical source instances, distributing logical source databases, automatically hydrating them with local DDL schema files, provisioning target Google Cloud Spanner instances, and producing the JSON `shard-config.json` Dataflow template payloads natively.

---

## Prerequisites

Before deploying the configurations, ensure your local environment satisfies the following requirements:

1. **Terraform CLI**: Version `>= 1.2` installed locally.
2. **Google Cloud SDK (`gcloud`)**: Installed and authenticated locally:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
3. **Google Cloud Project**: An active GCP Project with Billing enabled.

---

## Required GCP APIs
The Terraform template will attempt to automatically enable the following APIs in your GCP Project during initialization:
*   `compute.googleapis.com` (Compute Engine API)
*   `servicenetworking.googleapis.com` (Service Networking API)
*   `sqladmin.googleapis.com` (Cloud SQL Admin API)
*   `spanner.googleapis.com` (Cloud Spanner API)
*   `storage.googleapis.com` (Cloud Storage API)
*   `secretmanager.googleapis.com` (Secret Manager API)
*   `iam.googleapis.com` (Identity and Access Management API)

## Required IAM Permissions
The user or Service Account executing Terraform requires a role containing the following IAM Permissions, or a combination of the following predefined Google Cloud IAM Roles in the target project:

*   **Compute Network Admin** (`roles/compute.networkAdmin`)
*   **Service Networking Admin** (`roles/servicenetworking.networksAdmin`)
*   **Cloud SQL Admin** (`roles/cloudsql.admin`)
*   **Cloud Spanner Admin** (`roles/spanner.admin`)
*   **Storage Admin** (`roles/storage.admin`)
*   **Secret Manager Admin** (`roles/secretmanager.admin`)
*   **Service Usage Admin** (`roles/serviceusage.serviceUsageAdmin`)

---

## Directory Structure

```
infra-setup/
├── README.md          # This documentation
├── main.tf            # Core Terraform resources (Cloud SQL, Spanner, GCS, Polling Engine)
├── outputs.tf         # Output parameters & shard config JSON content
├── terraform.tf       # Upgraded Providers v6.x configuration & API activations
├── terraform.tfvars   # Variables preset configuration (example)
└── variables.tf       # Input variables definition
```

---

## Getting Started

### 1. Prepare your Local Schema SQL
Create a local file inside this directory (e.g., `schema.sql`) containing your database tables, columns, and constraints. Terraform uses this file to hydrate all newly provisioned logical shards.
Example:
```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);
```

### 2. Configure Variables
Modify `terraform.tfvars` or create your own `.tfvars` file with the desired GCP project parameters, database types, count values, and subnet constraints:
```hcl
project_id             = "your-gcp-project-id"
region                 = "us-central1"
migration_prefix       = "smt-sharded-demo"

# Shard Configuration
physical_shards_count  = 2
logical_shards_count   = 2
local_schema_file_path = "./schema.sql"

# Source Configurations (MySQL Example)
database_provider      = "MYSQL"
database_version       = "8_0"
```

### 3. Initialize and Run Terraform

Execute the standard Terraform lifecycle commands to provision your resources:

```bash
# 1. Initialize local Terraform environment and plugins
terraform init

# 2. Validate your configuration file syntax
terraform validate

# 3. Preview planned resource provisioning and changes
terraform plan

# 4. Deploy infrastructure to Google Cloud
terraform apply
```

---

## Automatic Teardown & GCP Consistency Handling

This template features built-in architecture to natively overcome GCP eventual consistency delays during `terraform destroy` operations:

*   **Spanner Backups Teardown**: Automatically searches for and deletes dynamically created, cascading Cloud Spanner instance backups during database destruction to prevent parent Instance deletion blockers.
*   **Intelligent VPC Teardown Polling**: Implements a dynamic Bash-loop provisioner ensuring Terraform seamlessly waits for Cloud SQL's invisible tenant network endpoints to release before finalizing removal of the VPC Peering connection.

---

## Outputs

Upon successful deployment, Terraform outputs details of provisioned resources and creates a `shard-config.json` file inside the root of this directory.

### Key Outputs:
- **`spanner_instance_id`**: Provisioned Target Spanner Instance.
- **`spanner_database_id`**: Provisioned Target Spanner Database name.
- **`cloudsql_instance_names`**: List of created Cloud SQL source nodes.
- **`cloudsql_instance_ips`**: Map of Cloud SQL hostnames mapped to IP addresses.
- **`shard_config_file`**: Relative path to the generated JSON configuration payload.
- **`shard_config_content`**: Printout of the configuration content.

---

## Generated Shard Config Schema

The created `shard-config.json` maps perfectly to the expected property schema utilized by the Java `Shard.java` model across all Dataflow CDC migration templates:

```json
[
  {
    "logicalShardId": "shard-0",
    "host": "10.0.0.4",
    "port": "3306",
    "user": "migration_admin",
    "dbName": "shard_db_0",
    "namespace": "public",
    "secretManagerUri": "projects/123/secrets/demo-db-password-0/versions/latest",
    "connectionProperties": "jdbcCompliantTruncation=true"
  }
]
```
This payload can be supplied directly as input parameters when executing Spanner migration Flex Templates.
