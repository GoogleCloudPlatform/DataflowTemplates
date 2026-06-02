# Google Cloud configuration
project_id = "<PROJECT_ID>"
region     = "<REGION>"

# Naming convention prefixes (both are optional; random suffixes are appended if left blank)
instance_prefix  = "<INSTANCE_PREFIX>"
migration_prefix = "<MIGRATION_PREFIX>"

# Source database setup (MySQL example)
database_provider     = "MYSQL"
database_version      = "8_0"
physical_shards_count = 1
logical_shards_count  = 2
logical_shard_prefix  = "shard_db"
cloudsql_tier         = "db-f1-micro"
database_user         = "<DATABASE_USER>"
database_password     = "<DATABASE_PASSWORD>" # Omit or leave empty to auto-generate

# Networking / access configurations
enable_public_ip    = true
authorized_networks = [
  {
    name  = "user-ip"
    value = "<YOUR_PUBLIC_IP_WITH_CIDR>" # e.g. "192.0.2.1/32"
  }
]

# Path to local SQL schema initialization file
local_schema_file_path = "./schema.sql"

# Spanner configuration
spanner_instance_name    = "<SPANNER_INSTANCE_NAME>"
spanner_display_name     = "<SPANNER_DISPLAY_NAME>"
spanner_config           = "regional-<REGION>"
spanner_processing_units = 100
spanner_database_name    = "<SPANNER_DATABASE_NAME>"

# Tagging labels
resource_labels = {
  "env"      = "dev"
  "template" = "sharded-migration"
}
