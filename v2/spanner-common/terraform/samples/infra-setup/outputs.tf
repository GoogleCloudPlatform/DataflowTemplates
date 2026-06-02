output "spanner_instance_id" {
  description = "The ID of the provisioned Spanner instance"
  value       = google_spanner_instance.spanner_instance.name
}

output "spanner_database_id" {
  description = "The ID of the provisioned Spanner database"
  value       = google_spanner_database.spanner_database.name
}

output "cloudsql_instance_names" {
  description = "The names of the provisioned physical Cloud SQL database instances"
  value       = google_sql_database_instance.instances[*].name
}

output "cloudsql_instance_ips" {
  description = "A map of physical Cloud SQL database instances and their assigned IP addresses"
  value = {
    for inst in google_sql_database_instance.instances : inst.name => inst.ip_address[0].ip_address
  }
}

output "shard_config_file" {
  description = "The filesystem path of the generated shard config JSON file"
  value       = local_file.shard_config.filename
}

output "shard_config_content" {
  description = "The JSON configuration of the generated shard config matching Shard.java"
  value       = jsondecode(local_file.shard_config.content)
}

output "quota_warning" {
  description = "Detailed list of IAM permission denials or limit fallbacks detected during Quota Validation"
  value       = try(data.external.quota_validator.result.warning, "none")
}

output "state_reconciliation_script" {
  description = "The filesystem path of the state reconciliation shell script used to gracefully adopt pre-existing GCP resources"
  value       = local_file.import_shards.filename
}

output "bulk_shard_config_file" {
  description = "The filesystem path of the generated bulk shard config file"
  value       = local_file.bulk_shard_config.filename
}

output "bulk_shard_config_content" {
  description = "The JSON configuration of the generated bulk shard config"
  value       = jsondecode(local_file.bulk_shard_config.content)
}
