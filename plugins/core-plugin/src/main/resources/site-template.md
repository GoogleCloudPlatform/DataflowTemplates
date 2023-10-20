<#assign TemplateDocsUtils=statics['com.google.cloud.teleport.plugin.docs.TemplateDocsUtils']>
{% extends "${base_include}/_base.html" %}
{% include "${base_include}/_local_variables.html" %}
{% include "${base_include}/docs/guides/templates/_provided-templates-vars.html" %}
{% block page_title %}${TemplateDocsUtils.replaceVariableInterpolationNames(spec.metadata.name)} template{% endblock %}

{% block body %}

<section id="${spec.metadata.internalName?lower_case?replace("_", "")}">

{% dynamic setvar launch_name %}the <b>${TemplateDocsUtils.replaceVariableInterpolationNames(spec.metadata.name)}</b> template{% dynamic endsetvar %}
{% dynamic setvar gcs_template_name %}${spec.metadata.internalName}{% dynamic endsetvar %}
<#if spec.metadata.preview!false>
{% dynamic setvar launch_stage %}beta{% dynamic endsetvar %}
{% dynamic setvar launch_type %}feature{% dynamic endsetvar %}
{% dynamic setvar info_params %}realtime_warning{% dynamic endsetvar %}
{% dynamic include /docs/includes/___info_launch_stage_disclaimer %}
</#if>

<#list spec.metadata.description?split("\n\n") as paragraph>
<p>${TemplateDocsUtils.wrapText(TemplateDocsUtils.replaceSiteTags(TemplateDocsUtils.replaceVariableInterpolationNames(paragraph!?trim)), 100, "  ", true)}</p>
</#list>

<h2>Pipeline requirements</h2>

<ul>
<#list spec.metadata.requirements as requirement>
  <li>${TemplateDocsUtils.wrapText(TemplateDocsUtils.replaceSiteTags(TemplateDocsUtils.replaceVariableInterpolationNames(requirement)), 100, "    ", true)}</li>
</#list>
</ul>

<h2>Template parameters</h2>
  {% dynamic setvar df_tab_name "param_table" %}
<table>
  <tr>
    <th>Parameter</th>
    <th>Description</th>
  </tr>
<#list spec.metadata.parameters as parameter>
<#if !parameter.optional!false>
  <tr>
    <td><code>${parameter.name}</code></td>
    <td>${TemplateDocsUtils.wrapText(TemplateDocsUtils.replaceSiteTags(TemplateDocsUtils.replaceVariableInterpolationNames(parameter.helpText?trim)), 120, "      ", true)?ensure_ends_with(".")}</td>
  </tr>
</#if>
</#list>
<#list spec.metadata.parameters as parameter>
<#if parameter.optional!false>
  <tr>
    <td><code>${parameter.name}</code></td>
    <td>Optional: ${TemplateDocsUtils.wrapText(TemplateDocsUtils.replaceSiteTags(TemplateDocsUtils.replaceVariableInterpolationNames(parameter.helpText?trim)), 120, "      ", true)?ensure_ends_with(".")}</td>
  </tr>
</#if>
</#list>
</table>

<#if spec.metadata.udfSupport>
<h2>User-defined function</h2>

<<../../../_shared/guides/_template-udf-boilerplate.md>>

<h3>Function specification</h3>

<p>The UDF has the following specification:</p>

<ul>
  <li><strong>Input</strong>: JSON string.</li>
  <li><strong>Output</strong>: The output must be a string or a stringified JSON object.</li>
</ul>

</#if>
<h2>Run the template</h2>
<#if flex>
{% dynamic setvar df_template_type "flex" %}
<#else>
{% dynamic setvar df_template_type "classic" %}
</#if>

{% setvar user_replaced_values_${spec.metadata.internalName?lower_case} %}
<p>Replace the following:</p>
<ul>
  {% dynamic if setvar.df_tab_name == "api" %}
  <li><code><var>PROJECT_ID</var></code>: {{df_project_id_desc}}</li>
  {% dynamic endif %}
  <li><code><var>JOB_NAME</var></code>: {{df_job_name_desc}}</li>
  <li><code><var>VERSION</var></code>: {{df_template_version_desc}}</li>
  <li><code><var>{{df_region_placeholder}}</var></code>: {{df_region_desc}}</li>
<#list spec.metadata.parameters as parameter>
<#if !parameter.optional!false>
  <li><code><var>${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}</var></code>: the ${parameter.label}</li>
</#if>
</#list>
</ul>
{% endsetvar %}
<div class="ds-selector-tabs">
  <section>
    <h3>Console</h3>
    {% include "${base_include}/docs/guides/templates/_provided-templates-using-console.html" %}
  </section>
  <section>
    <h3>{{gcloud_name_short}}</h3>
    {% dynamic setvar df_tab_name "gcloud" %}
    {{df_gcloud_tab_intro}}

<pre class="prettyprint lang-bsh">
<#if flex>
gcloud dataflow flex-template run <var>JOB_NAME</var> \
    --template-file-gcs-location={{df_template_file_gcs_location}} \
    --project=<var>PROJECT_ID</var> \
    --region=<var>{{df_region_placeholder}}</var> \
<#else>
gcloud dataflow jobs run <var>JOB_NAME</var> \
    --gcs-location={{df_template_file_gcs_location}} \
    --project=<var>PROJECT_ID</var> \
    --region=<var>{{df_region_placeholder}}</var> \
</#if>
    --parameters \
<#list spec.metadata.parameters as parameter>
<#if !parameter.optional!false>
       ${parameter.name}=<var>${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}</var>,\
</#if>
</#list>
</pre>

    {{user_replaced_values_${spec.metadata.internalName?lower_case}}}

  </section>
  <section>
    <h3>API</h3>
    {% dynamic setvar df_tab_name "api" %}
    {{df_api_tab_intro}}

<pre class="prettyprint lang-json">
POST {{df_template_launch_uri}}
<#if flex>
{
   "launchParameter": {
     "jobName": "<var>JOB_NAME</var>",
     "parameters": {
  <#list spec.metadata.parameters as parameter>
  <#if !parameter.optional!false>
       "${parameter.name}": "<var>${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}</var>",
  </#if>
  </#list>
     },
     "containerSpecGcsPath": "{{df_template_file_gcs_location_api}}",
     "environment": { "maxWorkers": "10" }
  }
}
<#else>
{
   "jobName": "<var>JOB_NAME</var>",
   "parameters": {
<#list spec.metadata.parameters as parameter>
<#if !parameter.optional!false>
     "${parameter.name}": "<var>${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}</var>",
</#if>
</#list>
   },
   "environment": { "maxWorkers": "10" }
}
</#if>
</pre>

    {{user_replaced_values_${spec.metadata.internalName?lower_case}}}

  </section>
</div>

<section class="expandable">
<h2 class="showalways">Template source code</h2>
{% setvar sample_id %}${spec.metadata.internalName?replace("_", "")}{% endsetvar %}
<div class="ds-selector-tabs" data-ds-scope="code-sample">
  {% include "_shared/widgets/_sample_tab_section.html" with lang="java" project="DataflowTemplates" file="${spec.metadata.sourceFilePath!README.md}" sample_hide_preface=True %}
</div>
</section>

</section>

{% endblock %}