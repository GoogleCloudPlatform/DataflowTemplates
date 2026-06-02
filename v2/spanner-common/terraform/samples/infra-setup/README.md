# Source Database & Spanner Target Setup for Migration Testing

This folder contains Terraform configuration files to automatically set up, configure, and clean up database resources on Google Cloud Platform (GCP).

This setup is designed to help you prepare and test database migration pipelines. It automatically creates:
1. One or more **source database instances** using Google Cloud SQL (either MySQL or PostgreSQL).
2. Inside those database instances, it creates multiple **logical databases (shards)**.
3. It imports a database table structure (your SQL schema) from a local file into all created logical databases.
4. A **target Cloud Spanner database instance**.
5. A **configuration file (`shard-config.json`)** that lists the host IP, database name, and credentials for all created database shards. You can pass this file directly as an input parameter to your Dataflow migration jobs.

---

## Prerequisites

Before you begin, make sure your computer has the following installed and configured:

1. **Terraform CLI** (Version 1.3.0 or newer)
2. **Google Cloud SDK (`gcloud` CLI)**: Installed, logged in, and set up with your project:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
3. **Python 3** (installed and accessible from your command line)
4. **Google Cloud Project** with billing enabled.

---

## How the Automated Scripts Work

This setup includes several helper scripts in the `scripts/` folder to handle quota checking, database loading, cleanup, and state reconciliation.

### 1. Quota Validator (`scripts/check_quota.py`)
Before Terraform starts creating any resources, it automatically runs this Python script. The script checks if your Google Cloud Project has enough available quota limits to create the requested number of Cloud SQL instances, Spanner Processing Units, and VPC networks.
* **How it behaves:** 
  * If you do not have enough quota, it stops execution immediately and prints a clear error explaining what resource limit would be exceeded.
  * If the user running Terraform does not have sufficient permissions to read quota limits from the Google Cloud API, it prints a `[QUOTA WARNING]` warning on the screen but lets the execution continue safely without blocking you.

### 2. Database Schema Loader (`scripts/import_schema.sh`)
Once the Cloud SQL database instances are created, Terraform runs this bash script. It reads your local SQL database structure file (like `schema.sql`) and imports it into every logical database shard.
* **Why it is needed:** Setting up IAM permissions on newly created resources sometimes takes a few seconds to propagate across Google Cloud. This script automatically retries the import up to 6 times (waiting 10 seconds between attempts) to ensure the database tables are loaded successfully.

### 3. Spanner Backup Cleanup (`scripts/delete_spanner_backups.sh`)
When you run `terraform destroy` to delete your setup, Google Cloud Spanner will refuse to delete the database instance if there are any automatic database backups present. This script automatically finds and deletes all backups for the Spanner instance right before Terraform deletes the instance.

### 4. Private Connection Cleanup (`scripts/teardown_vpc_peering.sh`)
If you configure your databases to use private IPs instead of public IPs, Google Cloud creates private networking connections between your network and Cloud SQL. When deleting this infrastructure, Google Cloud occasionally takes time to release these connections. This script cleanly deletes the private network connection using the `gcloud` tool, or safely bypasses it if there are other active resources still using the connection.

### 5. Existing Resources Ingestion (`scripts/import_shards.sh` - Auto-Generated)
If you run a plan (`terraform plan`), Terraform dynamically writes this customized import bash script for you in the `scripts/` directory. 
* **Why it is needed:** If you already have existing Cloud SQL instances or Cloud Spanner instances running in your GCP project with the same names, Terraform's default behavior is to throw a `409 Conflict` error and halt. 
* **How to use it:** Simply execute `./scripts/import_shards.sh` in your terminal before running `terraform apply`. It will check Google Cloud for any pre-existing instances and safely register them into Terraform's internal state tracking. This ensures the deployment finishes successfully and uses the existing database IP addresses to build the final `shard-config.json` configuration file.

---

## Step-by-Step Guide to Deploying

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
1. **`terraform_simple.tfvars` (Recommended for beginners)**: A simple, minimal configuration containing only the most important variables.
2. **`terraform.tfvars`**: A comprehensive variable template containing all available settings (such as database user, password, network CIDRs, tags, Spanner processing units).

Open `terraform_simple.tfvars` or `terraform.tfvars`, replace the placeholders (like `<PROJECT_ID>`) with your actual values, and save the file.

### Step 3: Initialize and Deploy

Run the following commands in your terminal:

```bash
# 1. Download necessary Terraform providers and plugins
terraform init

# 2. Optional: If you have pre-existing GCP resources with the same names, run this to register them:
# Note: This script is created or updated after running "terraform plan"
terraform plan --var-file=terraform_simple.tfvars
./scripts/import_shards.sh

# 3. Deploy the databases and generate the configuration
terraform apply --var-file=terraform_simple.tfvars
```

---

## Outputs & Results

Once the deployment completes successfully, Terraform will print the resource details on your screen and generate two sharding configuration files in this directory:

1. **`shard-config.json`**: A flat JSON list of all logical shards.
2. **`bulk-shard.config`**: A grouped JSON bulk sharding configuration mapping database names directly to their hosting database servers, matching the Java migration template reader.

### 1. Regular Shard Config Format (`shard-config.json`)
```json
[
  {
    "logicalShardId": "shard-0",
    "host": "198.51.100.5",
    "port": "3306",
    "user": "migration_user",
    "dbName": "shard_db_0",
    "namespace": "public",
    "secretManagerUri": "smt-migration-db-password-0/versions/latest",
    "connectionProperties": "jdbcCompliantTruncation=true"
  }
]
```

### 2. Bulk Shard Config Format (`bulk-shard.config`)
```json
{
  "shardConfigurationBulk": {
    "dataShards": [
      {
        "host": "198.51.100.5",
        "port": 3306,
        "user": "migration_user",
        "password": null,
        "secretManagerUri": "smt-migration-db-password-0/versions/latest",
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

### Cleaning Up Resources
To delete all created Google Cloud resources and avoid ongoing charges, run:
```bash
terraform destroy --var-file=terraform_simple.tfvars
```
All Cloud SQL databases, target Spanner databases, Secret Manager secrets, and networking links will be cleanly removed.
