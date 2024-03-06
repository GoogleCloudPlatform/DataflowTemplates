
variable "on_delete" {
  type        = string
  description = "One of \"drain\" or \"cancel\". Specifies behavior of deletion during terraform destroy."
}

variable "project" {
  type        = string
  description = "The Google Cloud Project ID within which this module provisions resources."
}

variable "region" {
  type        = string
  description = "The region in which the created job should run."
}

<#list parameters as variable>
variable "${variable.name}" {
  type = ${variable.type?lower_case}
  description = ${variable.description}
  <#if variable.defaultValue??>default = ${variable.defaultValue}</#if>
}

</#list>
