// Auto-generated file. Do not edit.

variable "from" {
  type = string
  description = "Define where to get data from (Example: a)"
}
variable "to" {
  type = string
  description = "Table to send data to (Example: b)"
  default = ""
}
variable "logical" {
  type = bool
  description = "Define if A goes to B. Defaults to: true."
  default = true
}
variable "JSON" {
  type = bool
  description = "Some JSON property. Defaults to: true."
  default = true
}
variable "inputSubscription" {
  type = string
  description = "Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name)"
}
variable "empty" {
  type = string
  description = "String that defaults to empty (Example: whatever)"
  default = ""
}
