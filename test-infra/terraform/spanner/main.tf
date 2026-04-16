provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Spanner Instance
resource "google_spanner_instance" "spanner" {
  name             = var.spanner_instance_name
  config           = "regional-${var.region}"
  display_name     = var.spanner_instance_name
  processing_units = 50000
}

locals {
  gcs_bucket_name = var.gcs_bucket_name != "" ? var.gcs_bucket_name : "${var.project_id}-it-infra-bucket"
}

# GCS Bucket
resource "google_storage_bucket" "bucket" {
  name                        = local.gcs_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# Network Configuration

data "google_compute_image" "ubuntu" {
  family  = "ubuntu-2204-lts"
  project = "ubuntu-os-cloud"
}

data "google_compute_network" "default" {
  name = var.network
}

# Private Services Access (Required for Cloud SQL Private IP)
resource "google_compute_global_address" "private_ip_alloc" {
  name          = "private-services-ip-allocation"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = data.google_compute_network.default.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = data.google_compute_network.default.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_alloc.name]
}


# Datastream Private Connectivity
resource "google_datastream_private_connection" "private_conn" {
  display_name          = "datastream-connect-2"
  location              = var.region
  private_connection_id = "datastream-connect-2"

  vpc_peering_config {
    vpc    = data.google_compute_network.default.id
    subnet = "10.3.0.0/29" # Should be small /29 range unused in VPC
  }
}


# Cloud SQL PostgreSQL (Private IP)
resource "google_sql_database_instance" "postgres" {
  depends_on          = [google_service_networking_connection.private_vpc_connection]
  deletion_protection = false
  name                = var.postgres_instance_name
  database_version    = "POSTGRES_15"
  region              = var.region

  settings {
    tier    = "db-perf-optimized-N-16"
    edition = "ENTERPRISE_PLUS"
    ip_configuration {
      ipv4_enabled    = false
      private_network = data.google_compute_network.default.id
    }

    database_flags {
      name  = "cloudsql.logical_decoding"
      value = "on"
    }
    database_flags {
      name  = "max_replication_slots"
      value = "1000"
    }
    database_flags {
      name  = "max_wal_senders"
      value = "1000"
    }
  }
}

resource "google_sql_user" "postgres" {
  name     = "postgres"
  instance = google_sql_database_instance.postgres.name
  password = "Hello@123"
}


# 6. Cloud SQL MySQL (Private IP)
resource "google_sql_database_instance" "mysql" {
  depends_on          = [google_service_networking_connection.private_vpc_connection]
  deletion_protection = false
  name                = var.mysql_instance_name
  database_version    = "MYSQL_8_0"
  region              = var.region



  settings {
    tier = "db-n1-standard-1"
    ip_configuration {
      ipv4_enabled    = false
      private_network = data.google_compute_network.default.id
    }
    backup_configuration {
      enabled            = true
      binary_log_enabled = true
    }
  }
}

resource "google_sql_user" "root" {
  name     = "root"
  instance = google_sql_database_instance.mysql.name
  host     = "%"
  password = "Hello@123"
}

resource "google_compute_address" "it_infra_vm_ip" {
  name         = "it-infra-vm-ip"
  subnetwork   = var.subnetwork
  address_type = "INTERNAL"
  region       = var.region
}

# 8. Consolidated VM for Tests and Proxy
resource "google_compute_instance" "it_infra_vm" {
  name                      = var.it_infra_vm_name
  machine_type              = "n2-standard-16"
  allow_stopping_for_update = true
  zone                      = var.zone
  tags                      = ["datastream-proxy"]

  boot_disk {
    initialize_params {
      image = data.google_compute_image.ubuntu.self_link
      size  = 100
    }
  }

  network_interface {
    network    = data.google_compute_network.default.id
    network_ip = google_compute_address.it_infra_vm_ip.address
    access_config {
      # Ephemeral public IP
    }
  }

  service_account {
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    export DEBIAN_FRONTEND=noninteractive
    export REPO_URL="https://github.com/GoogleCloudPlatform/DataflowTemplates"

    # User provided startup script
    user=runner
    id -u $user &> /dev/null | sudo useradd $user

    ulimit -n 65536
    sudo sysctl -w vm.max_map_count=262144

    sudo add-apt-repository ppa:git-core/ppa -y
    sudo apt update
    sudo apt install git -y
    sudo apt install openjdk-17-jdk-headless -y
    sudo apt install jq -y

    pushd /opt/
    MAVEN_VER=3.9.9
    sudo wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$${MAVEN_VER}/apache-maven-$${MAVEN_VER}-bin.tar.gz
    sudo tar -xvzf apache-maven-$${MAVEN_VER}-bin.tar.gz apache-maven-$${MAVEN_VER}
    sudo rm -f /opt/apache-maven-$${MAVEN_VER}-bin.tar.gz
    sudo ln -s /opt/apache-maven-$${MAVEN_VER} /opt/maven
    sudo ln -s /opt/maven/bin/mvn /usr/local/bin/mvn
    popd

    # Install gh
    sudo apt install curl -y \
    && sudo curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && sudo apt update \
    && sudo apt install gh -y

    # Install docker
    sudo apt update
    sudo apt install ca-certificates curl gnupg lsb-release -y
    sudo mkdir -p /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt update
    sudo apt install docker-ce docker-ce-cli containerd.io docker-compose-plugin -y

    sudo groupadd docker
    sudo gpasswd -a $user docker

    sudo mkdir /home/$user
    sudo chown $user /home/$user

    # Install Cloud SQL Proxy
    sudo wget https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.10.1/cloud-sql-proxy.linux.amd64 -O cloud-sql-proxy
    sudo chmod +x cloud-sql-proxy
    sudo mv cloud-sql-proxy /usr/local/bin/

    # Run Cloud SQL Proxy for MySQL
    cloud-sql-proxy --address 0.0.0.0 ${google_sql_database_instance.mysql.connection_name} --private-ip &

    # Run Cloud SQL Proxy for PG
    cloud-sql-proxy --address 0.0.0.0 ${google_sql_database_instance.postgres.connection_name} --private-ip &

  EOT

}

data "google_project" "current" {}

locals {
  compute_sa = "${data.google_project.current.number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_custom_role" "it_infra_role" {
  project     = var.project_id
  role_id     = "it_infra_role"
  title       = "it-infra role"
  description = "Custom role for Spanner Bulk migrations"
  permissions = [
    "compute.firewalls.create",
    "compute.firewalls.delete",
    "compute.firewalls.update",
    "dataflow.jobs.cancel",
    "dataflow.jobs.create",
    "dataflow.jobs.updateContents",
    "iam.roles.get",
    "iam.serviceAccounts.actAs",
    "resourcemanager.projects.setIamPolicy",
    "storage.objects.delete",
    "storage.objects.create",
    "storage.buckets.create",
    "serviceusage.services.use",
    "serviceusage.services.enable",
  ]
}

resource "google_project_iam_member" "custom_role_binding" {
  project = var.project_id
  role    = google_project_iam_custom_role.it_infra_role.id
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "viewer_binding" {
  project = var.project_id
  role    = "roles/viewer"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "storage_admin_binding" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_project_iam_member" "spanner_admin_binding" {
  project = var.project_id
  role    = "roles/spanner.databaseAdmin"
  member  = "serviceAccount:${local.compute_sa}"
}

resource "google_compute_firewall" "allow_datastream_to_proxy" {
  name    = "allow-datastream-to-proxy"
  network = data.google_compute_network.default.name

  allow {
    protocol = "tcp"
    ports    = ["3306", "5432"]
  }

  source_ranges = ["10.3.0.0/29"]
  target_tags   = ["datastream-proxy"]
}

