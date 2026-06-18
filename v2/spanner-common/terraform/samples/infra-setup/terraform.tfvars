# ==============================================================================
# Comprehensive Terraform configuration for Spanner Common Infrastructure Setup
# Every available variable is listed below. Required variables must be set.
# Optional variables show their default value; lines that are commented out use
# the module default (and, where noted, trigger automatic name generation).
# ==============================================================================

# ------------------------------------------------------------------------------
# REQUIRED
# ------------------------------------------------------------------------------
project_id             = "<PROJECT_ID>" # GCP project where resources are created
region                 = "<REGION>"     # e.g. "us-central1"
local_schema_file_path = "./schema.sql" # Local SQL schema imported into every shard

# ------------------------------------------------------------------------------
# NAMING (optional). Leave commented to auto-generate a unique "smt-<word>-<word>"
# name. An empty string ("") is treated the same as unset.
# ------------------------------------------------------------------------------
# instance_prefix  = "my-inst"  # Prefix for Cloud SQL + Spanner instance names
# migration_prefix = "my-mig"   # Prefix for VPC, subnet, secret, and bucket names

# ------------------------------------------------------------------------------
# SOURCE DATABASE (Cloud SQL)
# ------------------------------------------------------------------------------
database_provider = "MYSQL" # MYSQL or POSTGRES
# database_version      = "8_0"            # Set specific version (e.g., "14" for Postgres) or leave commented for module default
physical_shards_count = 1                # Number of physical Cloud SQL instances
logical_shards_count  = 2                # Logical databases per physical instance
logical_shard_prefix  = "shard_db"       # DB name prefix -> shard_db_0, shard_db_1, ...
cloudsql_tier         = "db-f1-micro"    # Machine tier for each instance
database_user         = "migration_user" # DB user created on every shard
database_password     = ""               # Empty -> a random password is generated
# database_port       = 3306             # Optional. Defaults: 3306 (MySQL) / 5432 (Postgres)

# ------------------------------------------------------------------------------
# NETWORKING / ACCESS
# ------------------------------------------------------------------------------
enable_public_ip = true # Set false to use private IP only (more secure)

# Provide an existing VPC self-link to skip creating a new network + peering.
# Leave commented to create a dedicated VPC, subnet, and private service connection.
# vpc_network_id = "projects/<PROJECT_ID>/global/networks/<NETWORK_NAME>"

# CIDRs allowed to reach Cloud SQL over public IP (only used when enable_public_ip = true).
authorized_networks = [
  {
    name  = "user-ip"
    value = "<YOUR_PUBLIC_IP_WITH_CIDR>" # e.g. "192.0.2.1/32"
  }
]

# JDBC connection properties embedded in the generated shard config.
connection_properties = "jdbcCompliantTruncation=true"

# ------------------------------------------------------------------------------
# TARGET SPANNER
# ------------------------------------------------------------------------------
# spanner_config           = "regional-<REGION>" # e.g. "regional-us-central1"; defaults to regional-${var.region}
spanner_display_name     = "SMT Spanner Instance"
spanner_processing_units = 100                   # Positive multiple of 100 (100 = 0.1 node)
spanner_database_dialect = "GOOGLE_STANDARD_SQL" # GOOGLE_STANDARD_SQL or POSTGRESQL
# spanner_instance_name  = "my-spanner"         # Optional. Unset -> derived from instance_prefix
# spanner_database_name  = "my-db"              # Optional. Unset -> derived from migration_prefix

# ------------------------------------------------------------------------------
# LABELS
# ------------------------------------------------------------------------------
resource_labels = {
  "env"      = "dev"
  "template" = "sharded-migration"
}