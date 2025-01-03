resource "google_compute_instance" "vm_instance" {
  name                      = var.instance_name
  machine_type              = var.instance_type
  zone                      = var.zone
  project                   = var.project
  hostname                  = var.instance != "windows" ? "" : "${var.instance_name}.${var.domain}"
  tags                      = var.tags
  labels                    = var.labels
  metadata_startup_script   = var.startup_script
  allow_stopping_for_update = var.allow_stopping_for_update

  boot_disk {
    auto_delete = var.auto_delete
    initialize_params {
      size  = var.disk_size_gb
      type  = var.disk_type
      image = var.image != "" ? var.image : "windows-cloud/windows-2019"
    }
    disk_encryption_key_raw = var.disk_encryption_key != null ? base64encode(var.disk_encryption_key) : null
  }

  dynamic "attached_disk" {
    for_each = var.disks != "" ? var.disks : {}
    content {
      source      = google_compute_disk.default[attached_disk.key].id
      device_name = attached_disk.value["device_name"]
    }
  }

  service_account {
    email  = var.service_account == "" ? "" : var.service_account
    scopes = var.scopes
  }

  network_interface {
    network    = "projects/${var.network_project_id}/global/networks/${var.network}"
    subnetwork = "projects/${var.network_project_id}/regions/${var.region}/subnetworks/${var.subnetwork}"
    network_ip = var.create_internal_static_ip ? google_compute_address.internal_static_ip[0].address : null

    dynamic "access_config" {
      for_each = var.create_external_static_ip ? [1] : []
      content {
        nat_ip = google_compute_address.external_static_ip[0].address
      }
    }
  }

  metadata = {
    block-project-ssh-keys = "${var.block-project-ssh-keys}"
    serial-port-enable     = "${var.serial-port-enable}"
    ssh-keys               = var.ssh-keys == "" ? null : file(var.ssh-keys)
    enable-oslogin         = var.enable_oslogin
  }

  scheduling {
    on_host_maintenance = var.on_host_maintenance
    preemptible         = var.preemptible
  }

  deletion_protection = var.deletion_protection

  shielded_instance_config {
    enable_secure_boot          = var.enable_secure_boot
    enable_vtpm                 = var.enable_vtpm
    enable_integrity_monitoring = var.enable_integrity_monitoring
  }
  depends_on = [google_compute_address.internal_static_ip]
}

resource "google_compute_disk" "default" {
  for_each = var.disks
  name     = "${var.instance_name}-disk-${each.key}"
  type     = each.value.type
  project  = var.project
  disk_encryption_key {
    kms_key_self_link = var.disk_encryption_key       
  }
  zone = var.zone
  size = each.value.size
}

# Conditional creation of internal static IP
resource "google_compute_address" "internal_static_ip" {
  count        = var.create_internal_static_ip ? 1 : 0
  name         = "${var.instance_name}-internal-ip"
  address_type = "INTERNAL"
  region       = var.region
  subnetwork   = "projects/${var.network_project_id}/regions/${var.region}/subnetworks/${var.subnetwork}"
  project      = var.project
}

# Conditional creation of external static IP
resource "google_compute_address" "external_static_ip" {
  count   = var.create_external_static_ip ? 1 : 0
  name    = "${var.instance_name}-external-ip"
  region  = var.region
  project = var.project
}