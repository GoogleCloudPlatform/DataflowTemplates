variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type        = object({
    project = string
    region  = string
  })
}

variable "vpc_params" {
  description = "Parameters for VPC configuration"
  type        = object({
    vpc_name              = optional(string, "sample-vpc-network-deep")
    subnetwork_name       = optional(string, "sample-vpc-subnetwork-deep")
    subnetwork_cidr_range = optional(string, "10.128.0.0/20")
  })
}

variable "mysql_params" {
  description = "Parameters for MySQL shards source configuration"
  type        = list(object({
    vm_name              = string
    machine_type         = optional(string, "n2-standard-2")
    zone                 = optional(string, "us-central1-a")
    root_password        = optional(string, "welcome1")
    custom_user          = optional(string, "deep")
    custom_user_password = optional(string, "welcome1")
    ddl                  = optional(string, "create database tftest; use tftest; CREATE TABLE Persons (ID int, Name varchar(255), PRIMARY KEY (ID));INSERT INTO Persons VALUES (1, 'foo'); commit; ")
  }))
}

variable "spanner_params" {
  description = "Parameters for Spanner configuration"
  type        = object({
    config           = optional(string, "regional-us-central1")
    name             = optional(string, "deep-spanner-instance")
    display_name     = optional(string, "deep-spanner-instance")
    processing_units = optional(number, 1000)
    database_name    = optional(string, "tftest")
    ddl              = optional(list(string), [
      "CREATE TABLE Persons (ID INT64, Name STRING(255)) PRIMARY KEY(ID)"
    ])
  })
}


