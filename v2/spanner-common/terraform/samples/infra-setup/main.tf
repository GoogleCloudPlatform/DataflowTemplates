# Random suffix for storage bucket name to ensure global uniqueness
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Random prefixes generated using pet names if none are supplied
resource "random_pet" "migration_id" {
  prefix = "smt"
}

resource "random_pet" "instance_id" {
  prefix = "smt"
}

# Random passwords for database users if a specific password was not provided
resource "random_password" "db_password" {
  count   = var.database_password != null && var.database_password != "" ? 0 : 1
  length  = 16
  special = false
}

locals {
  migration_prefix_resolved = var.migration_prefix != null && var.migration_prefix != "" ? var.migration_prefix : random_pet.migration_id.id
  instance_prefix_resolved  = var.instance_prefix != null && var.instance_prefix != "" ? var.instance_prefix : random_pet.instance_id.id

  spanner_instance_name_resolved = var.spanner_instance_name != null && var.spanner_instance_name != "" ? var.spanner_instance_name : "${local.instance_prefix_resolved}-spanner"
  spanner_database_name_resolved = var.spanner_database_name != null && var.spanner_database_name != "" ? var.spanner_database_name : "${local.migration_prefix_resolved}-db"

  database_version_reconstructed = "${upper(var.database_provider)}_${upper(var.database_version)}"
  resolved_vpc_id                = var.vpc_network_id != null ? var.vpc_network_id : google_compute_network.private_network[0].id
}

# Validate GCP Project Quota Before Provisioning Resources
data "external" "quota_validator" {
  program = ["python3", "${path.module}/scripts/check_quota.py"]

  query = {
    project_id                  = var.project_id
    region                      = var.region
    cloudsql_instances_required = tostring(var.physical_shards_count)
    spanner_pu_required         = tostring(var.spanner_processing_units)
    spanner_config              = var.spanner_config
    vpc_network_required        = tostring(var.vpc_network_id == null)
  }
}

# Create a VPC network if one is not supplied as an input variable
resource "google_compute_network" "private_network" {
  count                   = var.vpc_network_id == null ? 1 : 0
  name                    = "${lower(local.migration_prefix_resolved)}-vpc"
  auto_create_subnetworks = false
  project                 = var.project_id
  depends_on = [
    google_project_service.enabled_apis,
    data.external.quota_validator
  ]
}

# Create a subnetwork within the VPC network
resource "google_compute_subnetwork" "private_subnetwork" {
  count         = var.vpc_network_id == null ? 1 : 0
  name          = "${lower(local.migration_prefix_resolved)}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.private_network[0].id
  project       = var.project_id
}

# Allocate an IP range for private service connection
resource "google_compute_global_address" "private_ip_alloc" {
  count         = var.vpc_network_id == null ? 1 : 0
  name          = "${lower(local.migration_prefix_resolved)}-pip-alloc"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.private_network[0].id
  project       = var.project_id
}

# Establish the private service connection mapping using gcloud to support bulletproof teardowns
resource "null_resource" "private_vpc_connection" {
  count = var.vpc_network_id == null ? 1 : 0

  triggers = {
    project_id   = var.project_id
    network_name = google_compute_network.private_network[0].name
    range_name   = google_compute_global_address.private_ip_alloc[0].name
  }

  provisioner "local-exec" {
    environment = {
      NETWORK_NAME = self.triggers.network_name
      RANGE_NAME   = self.triggers.range_name
      PROJECT_ID   = self.triggers.project_id
    }
    command = "${path.module}/scripts/connect_vpc_peering.sh"
  }

  provisioner "local-exec" {
    when    = destroy
    environment = {
      NETWORK_NAME = self.triggers.network_name
      PROJECT_ID   = self.triggers.project_id
    }
    command = "${path.module}/scripts/teardown_vpc_peering.sh"
  }

  depends_on = [
    google_project_service.enabled_apis,
    google_compute_global_address.private_ip_alloc
  ]
}

# Provision Cloud SQL physical database instances
resource "google_sql_database_instance" "instances" {
  count            = var.physical_shards_count
  name             = "${lower(local.instance_prefix_resolved)}-physical-shard-${count.index}"
  database_version = local.database_version_reconstructed
  region           = var.region
  project          = var.project_id

  settings {
    tier = var.cloudsql_tier

    ip_configuration {
      ipv4_enabled                                  = var.enable_public_ip
      private_network                               = local.resolved_vpc_id
      enable_private_path_for_google_cloud_services = true

      dynamic "authorized_networks" {
        for_each = var.authorized_networks
        content {
          name  = authorized_networks.value.name
          value = authorized_networks.value.value
        }
      }
    }

    user_labels = var.resource_labels
  }

  deletion_protection = false

  depends_on = [
    google_project_service.enabled_apis,
    null_resource.private_vpc_connection,
    data.external.quota_validator
  ]
}

# Create the database migration user on all physical database shards
resource "google_sql_user" "users" {
  count    = var.physical_shards_count
  name     = var.database_user
  instance = google_sql_database_instance.instances[count.index].name
  host     = length(regexall(".*POSTGRES.*", upper(var.database_provider))) > 0 ? null : "%"
  password = var.database_password != null && var.database_password != "" ? var.database_password : random_password.db_password[0].result
  project  = var.project_id
}

# Provision Secret Manager secrets to store the shard passwords
resource "google_secret_manager_secret" "db_passwords" {
  count     = 1
  secret_id = "${replace(local.migration_prefix_resolved, "-", "_")}_db_password"

  replication {
    auto {}
  }

  labels     = var.resource_labels
  depends_on = [google_project_service.enabled_apis]
}

# Store database user passwords securely in Secret Manager secret versions
resource "google_secret_manager_secret_version" "db_password_versions" {
  count       = 1
  secret      = google_secret_manager_secret.db_passwords[0].id
  secret_data = var.database_password != null && var.database_password != "" ? var.database_password : random_password.db_password[0].result
}

# Create the logical shard databases distributed across physical instances
resource "google_sql_database" "logical_databases" {
  count    = var.physical_shards_count * var.logical_shards_count
  name     = "${var.logical_shard_prefix}_${count.index}"
  instance = google_sql_database_instance.instances[floor(count.index / var.logical_shards_count)].name
  project  = var.project_id
}

# Create GCS bucket to upload the schema file for Cloud SQL import
resource "google_storage_bucket" "schema_bucket" {
  name                        = "${lower(local.migration_prefix_resolved)}-schema-${random_id.bucket_suffix.hex}"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.resource_labels
  depends_on                  = [google_project_service.enabled_apis]
}

# Upload local schema file to GCS bucket
resource "google_storage_bucket_object" "schema_file" {
  name   = "schema.sql"
  source = var.local_schema_file_path
  bucket = google_storage_bucket.schema_bucket.name
}

# Grant IAM permissions to all Cloud SQL service accounts to read schema from the GCS bucket in a single API call to prevent ETag lock collision delays
resource "google_storage_bucket_iam_binding" "sql_gcs_reader" {
  bucket  = google_storage_bucket.schema_bucket.name
  role    = "roles/storage.objectViewer"
  members = [
    for inst in google_sql_database_instance.instances :
    "serviceAccount:${inst.service_account_email_address}"
  ]
}

# Run sql schema import sequentially or parallelly into logical database shards
resource "null_resource" "schema_import" {
  triggers = {
    schema_md5   = filemd5(var.local_schema_file_path)
    database_ids = join(",", google_sql_database.logical_databases[*].id)
  }

  depends_on = [
    google_storage_bucket_iam_binding.sql_gcs_reader,
    google_sql_user.users,
    google_sql_database.logical_databases,
    google_storage_bucket_object.schema_file
  ]

  provisioner "local-exec" {
    # Pass parameters via shell environments to avoid shell injection issues
    environment = {
      PROJECT_ID     = var.project_id
      BUCKET_NAME    = google_storage_bucket.schema_bucket.name
      OBJECT_NAME    = google_storage_bucket_object.schema_file.name
      PHYSICAL_COUNT = var.physical_shards_count
      LOGICAL_COUNT  = var.logical_shards_count
      INSTANCE_NAMES = join(",", google_sql_database_instance.instances[*].name)
      DATABASE_NAMES = join(",", google_sql_database.logical_databases[*].name)
    }

    command = "${path.module}/scripts/import_schema.sh"
  }
}

# Provision Spanner Target Instance
resource "google_spanner_instance" "spanner_instance" {
  name             = local.spanner_instance_name_resolved
  config           = var.spanner_config
  display_name     = var.spanner_display_name
  processing_units = var.spanner_processing_units
  project          = var.project_id
  labels           = var.resource_labels
  depends_on = [
    google_project_service.enabled_apis,
    data.external.quota_validator
  ]

  # Automated teardown of Spanner backups to prevent destroy failures
  provisioner "local-exec" {
    when    = destroy
    environment = {
      INSTANCE_NAME = self.name
      PROJECT_ID    = self.project
    }
    command = "${path.module}/scripts/delete_spanner_backups.sh"
  }
}

# Provision Spanner Target Database
resource "google_spanner_database" "spanner_database" {
  instance            = google_spanner_instance.spanner_instance.name
  name                = local.spanner_database_name_resolved
  project             = var.project_id
  database_dialect    = var.spanner_database_dialect
  deletion_protection = false
}

# Generate the Shard Config json file matching the Shard.java model properties
locals {
  shards = [
    for idx in range(var.physical_shards_count * var.logical_shards_count) : {
      logicalShardId = "shard-${idx}"
      host = try(
        coalesce(
          one([for ip in google_sql_database_instance.instances[floor(idx / var.logical_shards_count)].ip_address : ip.ip_address if ip.type == "PRIVATE"]),
          google_sql_database_instance.instances[floor(idx / var.logical_shards_count)].ip_address[0].ip_address
        ),
        "127.0.0.1"
      )
      port                 = tostring(var.database_port != null ? var.database_port : (length(regexall(".*POSTGRES.*", upper(var.database_provider))) > 0 ? 5432 : 3306))
      user                 = try(google_sql_user.users[floor(idx / var.logical_shards_count)].name, var.database_user)
      password             = null
      dbName               = "${var.logical_shard_prefix}_${floor(idx / var.logical_shards_count)}_${idx % var.logical_shards_count}"
      namespace            = "public"
      secretManagerUri     = try("${google_secret_manager_secret.db_passwords[0].id}/versions/latest", "projects/${var.project_id}/secrets/placeholder/versions/latest")
      connectionProperties = var.connection_properties
    }
  ]

  bulk_shards = {
    shardConfigurationBulk = {
      dataShards = [
        for p_idx in range(var.physical_shards_count) : {
          host = try(
            coalesce(
              one([for ip in google_sql_database_instance.instances[p_idx].ip_address : ip.ip_address if ip.type == "PRIVATE"]),
              google_sql_database_instance.instances[p_idx].ip_address[0].ip_address
            ),
            "127.0.0.1"
          )
          port                 = var.database_port != null ? var.database_port : (length(regexall(".*POSTGRES.*", upper(var.database_provider))) > 0 ? 5432 : 3306)
          user                 = try(google_sql_user.users[p_idx].name, var.database_user)
          password             = null
          secretManagerUri     = try("${google_secret_manager_secret.db_passwords[0].id}/versions/latest", "projects/${var.project_id}/secrets/placeholder/versions/latest")
          connectionProperties = var.connection_properties
          namespace            = "public"
          databases = [
            for l_idx in range(var.logical_shards_count) : {
              dbName     = "${var.logical_shard_prefix}_${p_idx}_${l_idx}"
              databaseId = "shard-${p_idx * var.logical_shards_count + l_idx}"
            }
          ]
        }
      ]
    }
  }
}

resource "local_file" "shard_config" {
  content  = jsonencode(local.shards)
  filename = "${path.module}/shard-config.json"
}

resource "local_file" "bulk_shard_config" {
  content  = jsonencode(local.bulk_shards)
  filename = "${path.module}/bulk-config.json"
}

