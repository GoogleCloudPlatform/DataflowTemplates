// Auto-generated file. Do not edit.

<#list parameters as variable>
variable "${variable.name}" {
  type = ${variable.type?lower_case}
  description = "${variable.description}"
  <#if variable.defaultValue??>
  <#if variable.type == "STRING">default = "${variable.defaultValue}"<#else>default = ${variable.defaultValue}</#if>
  </#if>
}
</#list>