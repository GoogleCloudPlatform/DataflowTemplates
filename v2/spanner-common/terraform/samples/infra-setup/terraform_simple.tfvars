# ==============================================================================
# Simple Terraform configuration for Spanner Common Infrastructure Setup
# Configure the core required and most common variables here.
# ==============================================================================

# Google Cloud Project ID and Region
project_id = "<PROJECT_ID>"
region     = "<REGION>"

# Naming prefixes to keep resources unique (both are optional)
instance_prefix  = "<INSTANCE_PREFIX>"
migration_prefix = "<MIGRATION_PREFIX>"

# Local SQL schema file path to initialize databases
local_schema_file_path = "./schema.sql"

# Cloud SQL Database Setup
database_provider = "MYSQL"
# database_version      = "8_0"
physical_shards_count = 1
logical_shards_count  = 2

# Target Spanner config. Should match `region` above, otherwise Spanner is
# created in a different region than Cloud SQL. Defaults to regional-${var.region}.
# spanner_config = "regional-<REGION>"