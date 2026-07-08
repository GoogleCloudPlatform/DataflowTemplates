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
  value       = [for inst in google_sql_database_instance.instances : inst.name]
}

output "cloudsql_instance_ips" {
  description = "A map of physical Cloud SQL database instances and their assigned IP addresses"
  value = {
    for inst in google_sql_database_instance.instances :
    inst.name => try(
      coalesce(
        one([for ip in inst.ip_address : ip.ip_address if ip.type == "PRIVATE"]),
        inst.ip_address[0].ip_address
      ),
      "unknown"
    )
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


output "bulk_shard_config_file" {
  description = "The filesystem path of the generated bulk shard config file"
  value       = local_file.bulk_shard_config.filename
}

output "bulk_shard_config_content" {
  description = "The JSON configuration of the generated bulk shard config"
  value       = jsondecode(local_file.bulk_shard_config.content)
}