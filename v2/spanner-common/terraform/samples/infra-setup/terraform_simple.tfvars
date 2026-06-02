# ==============================================================================
# Simple Terraform configuration for Spanner Common Infrastructure Setup
# Configure the core required and most common variables here.
# ==============================================================================

# Google Cloud Project ID and Region
project_id = "<PROJECT_ID>"
region     = "us-central1"

# Naming prefix to keep resources unique in your project
migration_prefix = "smt-simple-demo"

# Local SQL schema file path to initialize databases
local_schema_file_path = "./schema.sql"

# Cloud SQL Database Setup
database_provider     = "MYSQL"
database_version      = "8_0"
physical_shards_count = 1
logical_shards_count  = 2

# Spanner target configuration
spanner_instance_name = "smt-simple-spanner"
spanner_database_name = "smt-simple-db"
