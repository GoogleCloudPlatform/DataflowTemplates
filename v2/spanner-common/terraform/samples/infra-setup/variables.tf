variable "project_id" {
  description = "The GCP Project ID where resources will be created"
  type        = string
}

variable "region" {
  description = "The GCP Region where resources will be created"
  type        = string
}

variable "instance_prefix" {
  description = "A prefix to apply to physical database and Spanner instance names"
  type        = string
  default     = null
}

variable "migration_prefix" {
  description = "A prefix to apply to other migration resources to ensure name uniqueness"
  type        = string
  default     = null
}

variable "database_provider" {
  description = "The Cloud SQL database engine provider. Supported providers: MYSQL, POSTGRES"
  type        = string
  default     = "MYSQL"

  validation {
    condition     = contains(["MYSQL", "POSTGRES"], upper(var.database_provider))
    error_message = "database_provider must be either MYSQL or POSTGRES."
  }
}

variable "database_version" {
  description = "The Cloud SQL database engine version. Supported versions: for MYSQL ('8_0', '5_7'), for POSTGRES ('14', '15', '16'). If null, defaults to '8_0' for MYSQL and '14' for POSTGRES."
  type        = string
  default     = null
}

variable "physical_shards_count" {
  description = "The number of physical Cloud SQL database instances to create"
  type        = number
  default     = 1

  validation {
    condition     = var.physical_shards_count >= 1
    error_message = "physical_shards_count must be at least 1."
  }
}

variable "logical_shards_count" {
  description = "The number of logical databases (shards) to create per physical Cloud SQL instance"
  type        = number
  default     = 1

  validation {
    condition     = var.logical_shards_count >= 1
    error_message = "logical_shards_count must be at least 1."
  }
}

variable "logical_shard_prefix" {
  description = "The database name prefix for each logical database shard"
  type        = string
  default     = "shard_db"
}

variable "cloudsql_tier" {
  description = "The machine type/tier for the Cloud SQL instance"
  type        = string
  default     = "db-f1-micro"
}

variable "database_user" {
  description = "The username of the database user created on all physical shards"
  type        = string
  default     = "migration_user"
}

variable "database_password" {
  description = "The password for the database user. If empty, a random password is created automatically"
  type        = string
  default     = ""
  sensitive   = true
}

variable "database_port" {
  description = "The connection port to define in the shard config output. Defaults to 3306 for MySQL or 5432 for PostgreSQL."
  type        = number
  default     = null
}

variable "enable_public_ip" {
  description = "Whether to enable public IP on the Cloud SQL instances"
  type        = bool
  default     = true
}

variable "vpc_network_id" {
  description = "The full self-link of the VPC network if using private IP configurations for Cloud SQL"
  type        = string
  default     = null
}

variable "authorized_networks" {
  description = "Authorized CIDR networks that can access Cloud SQL via public IP (e.g., [{name = \"all\", value = \"0.0.0.0/0\"}])"
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "local_schema_file_path" {
  description = "The local path to the SQL schema file which will be imported into each logical database shard"
  type        = string
}

variable "connection_properties" {
  description = "Database connection properties for JDBC string to include in the shard config output"
  type        = string
  default     = "jdbcCompliantTruncation=true"
}

variable "spanner_instance_name" {
  description = "The name/ID of the Spanner instance to create. If empty, derived from instance_prefix"
  type        = string
  default     = null
}

variable "spanner_display_name" {
  description = "The display name for the Spanner instance"
  type        = string
  default     = "SMT Spanner Instance"
}

variable "spanner_config" {
  description = "The Spanner instance config name (e.g. regional-us-central1, multi-region-us). If null, defaults to regional-<region>."
  type        = string
  default     = null
}

variable "spanner_processing_units" {
  description = "The number of processing units to allocate for the Spanner instance (100 units = 0.1 node)"
  type        = number
  default     = 1000

  validation {
    condition     = var.spanner_processing_units > 0 && var.spanner_processing_units % 100 == 0
    error_message = "spanner_processing_units must be a positive multiple of 100."
  }
}

variable "spanner_database_name" {
  description = "The name of the Spanner database to create. If empty, derived from migration_prefix"
  type        = string
  default     = null
}

variable "resource_labels" {
  description = "Key/Value labels to tag and organize GCP resources created by this module"
  type        = map(string)
  default = {
    "migration_prefix" = "smt-migration"
  }
}

variable "spanner_database_dialect" {
  description = "The dialect for the target Spanner database. Supported: GOOGLE_STANDARD_SQL, POSTGRESQL"
  type        = string
  default     = "GOOGLE_STANDARD_SQL"

  validation {
    condition     = contains(["GOOGLE_STANDARD_SQL", "POSTGRESQL"], upper(var.spanner_database_dialect))
    error_message = "spanner_database_dialect must be GOOGLE_STANDARD_SQL or POSTGRESQL."
  }
}