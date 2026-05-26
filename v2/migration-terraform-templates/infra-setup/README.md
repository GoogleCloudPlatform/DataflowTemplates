# Sharded Source & Spanner Target Infrastructure Setup

This directory contains Terraform configurations to set up a complete sharded source database infrastructure (using Cloud SQL instances) and a target Spanner database instance. It automates:
1. Provisioning multiple physical Cloud SQL instances (shards).
2. Provisioning database credentials.
3. Distributing multiple logical databases across physical shards.
4. Initializing the schema on all logical databases using a local SQL file.
5. Provisioning a target Spanner instance and database.
6. Generating a local `shard-config.json` configuration file matching the Java `Shard` class property schema exactly.
7. Tagging all resources with custom user-defined labels.

---

## Requirements

- **Terraform** version `>= 1.2`
- **Google Cloud SDK (gcloud)** installed locally and authenticated (`gcloud auth login`, `gcloud auth application-default login`).
- GCP Project with billing enabled.

---

## Directory Structure

```
infra-setup/
├── README.md          # This documentation
├── main.tf            # Core Terraform resources (Cloud SQL, Spanner, GCS, Local File)
├── outputs.tf         # Output parameters & shard config JSON content
├── terraform.tf       # Providers configuration & API activation
├── terraform.tfvars   # Variables preset configuration (example)
└── variables.tf       # Input variables definition
```

---

## Getting Started

### 1. Prepare your Local Schema SQL
Create a local file (e.g., `schema.sql`) containing your database tables, columns, and constraints. Example:
```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);
```

### 2. Configure Variables
Modify `terraform.tfvars` or create your own `.tfvars` file with the required GCP project, database types, counts, and paths:
```hcl
project_id = "your-gcp-project-id"
region     = "us-central1"

physical_shards_count = 2
logical_shards_count  = 2
local_schema_file_path = "./schema.sql"
```

### 3. Initialize and Run Terraform

Run the standard Terraform lifecycle commands:

```bash
# Initialize plugins and providers
terraform init

# Validate the configurations
terraform validate

# Check the execution plan
terraform plan

# Provision resources
terraform apply
```

---

## Outputs

Upon successful completion, Terraform outputs the details of the provisioned resources and automatically creates a `shard-config.json` file in the root of this subdirectory.

### Key Outputs:
- **`spanner_instance_id`**: Created Spanner instance ID.
- **`spanner_database_id`**: Created Spanner database name.
- **`cloudsql_instance_names`**: Created Cloud SQL physical instances.
- **`cloudsql_instance_ips`**: Map of Cloud SQL physical instance names to their IP addresses.
- **`shard_config_file`**: Path to the newly generated `shard-config.json` file.
- **`shard_config_content`**: Content of the shard config.

---

## Generated Shard Config Schema

The generated `shard-config.json` matches the property signature of Java's `Shard.java` model:

```json
[
  {
    "logicalShardId": "shard-0",
    "host": "35.224.10.20",
    "port": "3306",
    "user": "migration_admin",
    "password": "SuperSecurePassword123!",
    "dbName": "shard_db_0",
    "namespace": "namespace-0",
    "connectionProperties": "jdbcCompliantTruncation=true"
  },
  {
    "logicalShardId": "shard-1",
    "host": "35.224.10.20",
    "port": "3306",
    "user": "migration_admin",
    "password": "SuperSecurePassword123!",
    "dbName": "shard_db_1",
    "namespace": "namespace-1",
    "connectionProperties": "jdbcCompliantTruncation=true"
  }
]
```
This file can be supplied directly as an input parameter to Spanner migration pipelines (e.g., Sourcedb to Spanner, Datastream to Spanner).
