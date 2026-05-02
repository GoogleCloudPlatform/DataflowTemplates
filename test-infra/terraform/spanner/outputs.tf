output "spanner_instance" {
  value = google_spanner_instance.spanner.name
}

output "gcs_bucket" {
  value = google_storage_bucket.bucket.name
}

output "datastream_private_connection_id" {
  value = google_datastream_private_connection.private_conn.id
}

output "postgres_connection_name" {
  value = google_sql_database_instance.postgres.connection_name
}

output "mysql_connection_name" {
  value = google_sql_database_instance.mysql.connection_name
}

output "it_infra_vm_ip" {
  value = google_compute_instance.it_infra_vm.network_interface[0].network_ip
}
