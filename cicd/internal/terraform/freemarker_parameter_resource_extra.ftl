  parameters = {
<#list parameters as variable>
    ${variable.name} = <#if variable.type == "STRING">var.${variable.name}<#else>tostring(var.${variable.name})</#if>
</#list>
  }