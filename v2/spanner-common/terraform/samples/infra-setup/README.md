# Source Database & Spanner Target Setup for Migration Testing

## Sample Scenario: Sharded Database Infra Setup for Spanner Migrations

This folder contains Terraform configuration files to automatically set up, configure, and clean up database resources on Google Cloud Platform (GCP).

This setup is designed to help you prepare and test database migration pipelines. It automatically creates:
1. One or more **source database instances** using Google Cloud SQL (either MySQL or PostgreSQL).
2. Inside those database instances, it creates multiple **logical databases (shards)**.
3. It imports a database table structure (your SQL schema) from a local file into all created logical databases.
4. A **target Cloud Spanner database instance**.
5. Two **sharding configuration files** (`shard-config.json` and `bulk-config.json`) that list the host IP, database name, and credentials for all created database shards. You can pass either file directly as an input parameter to your Dataflow migration jobs.

---

## Assumptions

1. Google Cloud SDK (`gcloud` CLI) and Python 3 are installed.
2. User account or service account running Terraform has sufficient GCP permissions.

## Terraform permissions

The account running Terraform requires Cloud SQL Admin, Cloud Spanner Admin, Compute Network Admin, and Secret Manager Admin permissions.

## Prerequisites

Before you begin, make sure your computer has the following installed and configured:

1. **Terraform CLI** (Version 1.2.0 or newer)
2. **Google Cloud SDK (`gcloud` CLI)**: Installed, logged in, and set up with your project:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
3. **Python 3** (installed and accessible from your command line)
4. **Google Cloud Project** with billing enabled.

---

## Description: How the Automated Scripts Work

This setup includes several helper scripts in the `scripts/` folder to handle database loading, cleanup, and state reconciliation.

### 1. Database Schema Loader (`scripts/import_schema.sh`)
Once the Cloud SQL database instances are created, Terraform runs this bash script **once per physical instance** (the import step uses `for_each`), so a failure on one instance only re-imports that instance on the next apply instead of all of them. Each run reads your local SQL structure file (like `schema.sql`) and imports it sequentially into that instance's logical databases (Cloud SQL allows only one import at a time per instance); Terraform runs the instances in parallel.
* **Why the retries are needed:** The bucket grants each Cloud SQL instance's service account read access just before the import runs, but IAM changes take a few seconds to propagate across Google Cloud. An import attempted in that window fails with a permission error. To handle this, the script retries each import up to 6 times (waiting 10 seconds between attempts) until the permission propagates and the schema loads successfully.

### 2. Spanner Backup Cleanup (`scripts/delete_spanner_backups.sh`)
When you run `terraform destroy` to delete your setup, Google Cloud Spanner will refuse to delete the database instance if there are any automatic database backups present. This script automatically finds and deletes all backups for the Spanner instance right before Terraform deletes the instance.

### 3. Private Connection Cleanup (`scripts/teardown_vpc_peering.sh`)
If you configure your databases to use private IPs instead of public IPs, Google Cloud creates private networking connections between your network and Cloud SQL. When deleting this infrastructure, Google Cloud occasionally takes time to release these connections. This script cleanly deletes the private network connection using the `gcloud` tool, or safely bypasses it if there are other active resources still using the connection.


---

## How to run: Step-by-Step Guide

### Step 1: Prepare Your Local Database Structure
Create a local SQL file named `schema.sql` in this folder. Define the tables and columns you want to load into your source databases. For example:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);
```

### Step 2: Configure Your Variables
There are two variable sample files provided:
1. **`terraform_simple.tfvars` (Recommended for beginners)**: A simple, minimal configuration containing only the most important variables. It leverages the automated prefix generation.
2. **`terraform.tfvars`**: A comprehensive variable template containing all available settings (such as database user, password, network CIDRs, tags, Spanner processing units).

#### Key Naming Variables:
* **`instance_prefix` (Optional)**: A string prefixed to physical database instances and target Spanner instances. If not provided, a unique random pet name of the form `smt-<word>-<word>` (e.g. `smt-clever-mongoose`) is generated automatically.
* **`migration_prefix` (Optional)**: A string prefixed to other resources like VPC networks, subnets, Secret Manager secrets, and GCS schema buckets. If not provided, a unique random pet name of the form `smt-<word>-<word>` is generated automatically.
* **`spanner_instance_name` / `spanner_database_name` (Optional)**: Overrides the target Spanner instance and database names completely. If left blank, they are dynamically derived from your `instance_prefix` and `migration_prefix` respectively.

Open `terraform_simple.tfvars` or `terraform.tfvars`, replace the placeholders (like `<PROJECT_ID>`) with your actual values, and save the file.

### Step 3: Initialize and Deploy

Run the following commands in your terminal:

```bash
# 1. Download necessary Terraform providers and plugins
terraform init

# 2. Deploy the databases and generate the configuration
# Note: For large scale deployments (e.g., 128 shards), you MUST use the -parallelism flag
# for faster resource creation (default is 10).
terraform apply -parallelism=100 --var-file=terraform_simple.tfvars
```

---

## Resources Created: Outputs & Results

Once the deployment completes successfully, Terraform will print the resource details on your screen and generate two sharding configuration files in this directory:

### 1. Regular Shard Config Format (`shard-config.json`) *(Sample output)*
```json
[
  {
    "logicalShardId": "shard-0",
    "host": "198.51.100.5",
    "port": "3306",
    "user": "migration_user",
    "password": null,
    "dbName": "shard_db_0",
    "namespace": "public",
    "secretManagerUri": "projects/my-gcp-project/secrets/smt_clever_mongoose_db_password/versions/latest",
    "connectionProperties": "jdbcCompliantTruncation=true"
  }
]
```

### 2. Bulk Shard Config Format (`bulk-config.json`) *(Sample output)*
```json
{
  "shardConfigurationBulk": {
    "dataShards": [
      {
        "host": "198.51.100.5",
        "port": 3306,
        "user": "migration_user",
        "password": null,
        "secretManagerUri": "projects/my-gcp-project/secrets/smt_clever_mongoose_db_password/versions/latest",
        "connectionProperties": "jdbcCompliantTruncation=true",
        "namespace": "public",
        "databases": [
          {
            "dbName": "shard_db_0",
            "databaseId": "shard-0"
          },
          {
            "dbName": "shard_db_1",
            "databaseId": "shard-1"
          }
        ]
      }
    ]
  }
}
```

---

## FAQ: Troubleshooting

### Handling Creation Timeouts & Operation Dropouts
When deploying a high number of physical database instances concurrently (e.g., 128 shards), you may occasionally encounter a transient timeout or polling connection dropout error from the Google Cloud API:
```
Error: Error waiting for Create Instance: ...
```
Or when running `terraform apply` again after a timeout:
```
Error: Error, failed to create instance ...: googleapi: Error 409: The Cloud SQL instance already exists., instanceAlreadyExists
```

#### Why this happens:
When Terraform requests the creation of 100+ databases, Google Cloud schedules their creation asynchronously in the background. If the local Terraform process loses connection to the GCP Operation API or hits a client-side wait timeout, Terraform aborts the command and **fails to save those specific instances to your local `terraform.tfstate` file**, even though the creation continues successfully in the background on Google's servers.

#### How to resolve this:
1. **Verify creation in GCP**: Run this CLI command to confirm that the instances are active and running on Google Cloud:
   ```bash
   gcloud sql instances list --project="<YOUR_PROJECT_ID>" --filter="name~smt-sharded"
   ```
2. **Import the affected instances into Terraform State**: For any instances that were successfully created on GCP but are missing from your local state file (causing `409 Already Exists` errors), import them manually back into Terraform. The instances use `for_each`, so the resource address is keyed by the shard index **as a quoted string** (e.g. `["18"]`, not `[18]`):
   ```bash
   terraform import --var-file=terraform_simple.tfvars 'google_sql_database_instance.instances["<INDEX>"]' "projects/<YOUR_PROJECT_ID>/instances/<INSTANCE_NAME>"
   ```
   *Example:*
   ```bash
   terraform import --var-file=terraform_simple.tfvars 'google_sql_database_instance.instances["18"]' "projects/my-gcp-project/instances/smt-sharded-demo-new-physical-shard-18"
   ```
3. **Resume the Deployment**: Once all missing instances are imported, simply rerun the deployment command with controlled parallelism:
   ```bash
   terraform apply -parallelism=30 --var-file=terraform_simple.tfvars
   ```
   Terraform will successfully refresh the state and complete the configuration setup in minutes!

---

### Cleaning Up Resources
To delete all created Google Cloud resources and avoid ongoing charges, run:
```bash
terraform destroy --var-file=terraform_simple.tfvars
```
All Cloud SQL databases, target Spanner databases, Secret Manager secrets, and networking links will be cleanly removed.