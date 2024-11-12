variable "project" {
  type        = string
  description = "Project Name"
  default     = ""
}

variable "name" {
  type        = string  
  description = "Name of Firewall Rule"
  default     = ""
}

variable "network" {
  type        = string  
  description = "Name of the network"
  default     = ""
}

variable "description" {
  type        = string  
  description = "Description"
  default     = ""
}

variable "protocol" {
  type        = string  
  description = "protocol"
  default     = ""
}

variable "ports" {
  type = list(string)
  description = "list of ports"
  default     = [] 
}

variable "source_ranges" {
  type = list(string)
  description = "List of Source range IPs"
  default     = [] 
}

variable "target_tags" {
  type = list(string)
  description = "List of Target tags"
  default     = [] 
}
