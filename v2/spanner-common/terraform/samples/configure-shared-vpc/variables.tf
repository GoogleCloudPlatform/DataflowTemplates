variable "common_params" {
  description = "Parameters that are common to multiple resources"
  type = object({
    service_project                 = string
    host_project                    = string
    region                          = string
    service_project_service_account = optional(string)
  })
}


