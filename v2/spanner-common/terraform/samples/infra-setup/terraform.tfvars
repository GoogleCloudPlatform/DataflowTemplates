# Google Cloud configuration
project_id = "span-cloud-ck-testing-external"
region     = "us-central1"

# Naming convention prefix
migration_prefix = "smt-sharded-demo-new"

# Source database setup (MySQL example)
database_provider     = "MYSQL"
database_version      = "8_0"
physical_shards_count = 1
logical_shards_count  = 8
logical_shard_prefix  = "shard_db"
cloudsql_tier         = "db-f1-micro"
database_user         = "migration_admin"
database_password     = "SuperSecurePassword123!" # Omit or leave empty to auto-generate

# Networking / access configurations
enable_public_ip    = true
authorized_networks = [
  {
    name  = "test-ip"
    value = "104.135.188.25" # For testing only. Restrict in production!
  }
]

# Path to local SQL schema initialization file
local_schema_file_path = "./schema.sql"

# Spanner configuration
spanner_instance_name    = "smt-spanner-instance"
spanner_display_name     = "SMT Spanner Target Instance"
spanner_config           = "regional-us-central1"
spanner_processing_units = 100
spanner_database_name    = "smt-spanner-db"

# Tagging labels
resource_labels = {
  "env"      = "dev"
  "template" = "sharded-migration"
}
