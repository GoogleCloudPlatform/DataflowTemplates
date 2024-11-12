#############################
## Application - Variables ##
#############################

variable "instance" {
  type        = string
  description = "OS of the instance (windows/linux)."
  validation {
    condition     = var.instance == "windows" || var.instance == "linux"
    error_message = "Value must be 'windows' or 'linux'."
  }
}

# Instance Name
variable "instance_name" {
  type        = string
  description = "This variable defines the name of the instance."
}

#allow stopping VM for update
variable "allow_stopping_for_update" {
  type        = bool
  description = "The boolean variable allow stopping VM when upgrading the Configuration ."
  default     = false
}

# domain name 
variable "domain" {
  type        = string
  description = "This variable defines the domain name used to build resources"
  default     = ""
}


variable "static_private_ip" {
  description = "The private Ip for the instance"
  default     = ""
}

variable "enable_secure_boot" {
  description = "The private Ip for the instance"
  type        = bool  
  default     = true
}

variable "enable_vtpm" {
  description = "The private Ip for the instance"
  type        = bool  
  default     = true
}

variable "enable_integrity_monitoring" {
  description = "The private Ip for the instance"
  type        = bool
  default     = true
}

#########################
## Network - Variables ##
#########################

variable "network" {
  type        = string
  description = "(Optional) The name or self_link of the network to attach this interface to. Either network or subnetwork must be provided. If network isn't provided it will be inferred from the subnetwork."
  default     = ""
}

variable "network_project_id" {
  type        = string
  description = "Name of network project id"
  default     = ""
}

variable "subnetwork" {
  type        = string
  description = "(Optional) The name or self_link of the subnetwork to attach this interface to. Either network or subnetwork must be provided. If network isn't provided it will be inferred from the subnetwork. The subnetwork must exist in the same region this instance will be created in. If the network resource is in legacy mode, do not specify this field. If the network is in auto subnet mode, specifying the subnetwork is optional. If the network is in custom subnet mode, specifying the subnetwork is required."
  default     = ""
}

variable "vm_ipaddress" {
  type    = string
  default = ""
}

variable "create_internal_static_ip" {
  description = "Whether to create an internal static IP"
  type        = bool
  default     = false
}

variable "create_external_static_ip" {
  description = "Whether to create an external static IP"
  type        = bool
  default     = false
}

##############################
## GCP Provider - Variables ##
##############################

# define GCP project name
variable "project" {
  type        = string
  description = "GCP project name"
}

variable "region" {
  type        = string
  description = "region"
}

variable "zone" {
  type        = string
  description = "GCP zone"
}

################################
## GCP Windows VM - Variables ##
################################

variable "instance_type" {
  type        = string
  description = "VM instance type"
  default     = "n2-standard-2"
}

variable "image" {
  type        = string
  description = "SKU for Windows Server (windows-cloud/windows-2012-r2, windows-cloud/windows-2016, windows-cloud/windows-2019, windows-cloud/windows-2022)."
  default     = ""
}

variable "tags" {
  type = list(string)
}

variable "disk_encryption_key" {
  description = "KMS disk encryption key"
  default     = ""
}

variable "disk_size_gb" {}

variable "disk_type" {}

variable "labels" {
  type = map(string)
}

variable "auto_delete" {
  type    = bool
  default = false
}

variable "block-project-ssh-keys" {
  type    = bool
  default = true
}

variable "serial-port-enable" {
  type    = bool
  default = false
}

variable "enable_oslogin" {
  type    = bool
  default = true
}
variable "on_host_maintenance" {
  type    = string
  default = "MIGRATE"
}

variable "preemptible" {
  type    = bool
  default = false
}

variable "deletion_protection" {
  type    = bool
  default = true
}

variable "ssh-keys" {
  description = "Path to the public key to be used for ssh access to the VM.  Only used with non-Windows vms and can be left as-is even if using Windows vms. If specifying a path to a certification on a Windows machine to provision a linux vm use the / in the path versus backslash. e.g. c:/home/id_rsa.pub"
  default     = ""
}

variable "service_account" {
  default = ""
}

variable "disks" {
  type = map(object({
    type        = string
    size        = string
    device_name = string
  }))
  default = {}
}

variable "startup_script" {
  description = "User startup script to run when instances spin up"
  default     = ""
}

variable "scopes" {
  default = [
    "https://www.googleapis.com/auth/cloudruntimeconfig",
    "https://www.googleapis.com/auth/monitoring.write",
    "https://www.googleapis.com/auth/sqlservice.admin",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/compute"
  ]
}