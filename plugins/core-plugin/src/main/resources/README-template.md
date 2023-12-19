<#assign TemplateDocsUtils=statics['com.google.cloud.teleport.plugin.docs.TemplateDocsUtils']>

${spec.metadata.name} template
---
<#list spec.metadata.description?split("\n\n") as paragraph>
${TemplateDocsUtils.wrapText(paragraph!?trim, 80, "", false)?ensure_ends_with(".")}

</#list>

<#if spec.metadata.googleReleased>
:memo: This is a Google-provided template! Please
check [Provided templates documentation](<#if spec.metadata.documentationLink?has_content>${spec.metadata.documentationLink}<#else>https://cloud.google.com/dataflow/docs/guides/templates/provided-templates</#if>)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=${spec.metadata.internalName}).
</#if>

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>* **${parameter.name}** (${parameter.label}): ${parameter.helpText?ensure_ends_with(".")}
</#if></#list>

### Optional Parameters

<#list spec.metadata.parameters as parameter><#if parameter.optional!false>* **${parameter.name}** (${parameter.label}): ${parameter.helpText?ensure_ends_with(".")}
</#if></#list>


<#if spec.metadata.udfSupport>
## User-Defined functions (UDFs)

The ${spec.metadata.name} Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.

</#if>

## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=${spec.metadata.sourceFilePath!README.md})

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin). 

### Building Template

<#if flex>
This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.
<#else>
This template is a Classic Template, meaning that the pipeline code will be
executed only once and the pipeline will be saved to Google Cloud Storage for
further reuse. Please check [Creating classic Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
and [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)
for more information.
</#if>

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="${spec.metadata.internalName}" \
<#if language == 'PYTHON'>
-f python
<#elseif flex>
-f v2/${spec.metadata.module!}
<#else>
-f v1
</#if>
```

<#if flex>
<#else>
The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.
</#if>

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
<#if flex>
Flex Template was staged! gs://<bucket-name>/templates/<#if flex>flex/</#if>${spec.metadata.internalName}
<#else>
Classic Template was staged! gs://<bucket-name>/templates/<#if flex>flex/</#if>${spec.metadata.internalName}
</#if>
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/<#if flex>flex/</#if>${spec.metadata.internalName}"

### Required
<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${TemplateDocsUtils.printDefaultValueVariable(parameter)}
</#if></#list>

### Optional
<#list spec.metadata.parameters as parameter><#if parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${TemplateDocsUtils.printDefaultValueVariable(parameter)}
</#if></#list>

gcloud dataflow <#if flex>flex-template<#else>jobs</#if> run "${spec.metadata.internalName?lower_case?replace("_", "-")}-job" \
  --project "$PROJECT" \
  --region "$REGION" \
<#if flex>
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
<#else>
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
</#if>
<#list spec.metadata.parameters as parameter>
  --parameters "${parameter.name}=$${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}"<#sep> \</#sep>
</#list>
```

For more information about the command, please check:
<#if flex>
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run
<#else>
https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run
</#if>


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${TemplateDocsUtils.printDefaultValueVariable(parameter)}
</#if></#list>

### Optional
<#list spec.metadata.parameters as parameter><#if parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${TemplateDocsUtils.printDefaultValueVariable(parameter)}
</#if></#list>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="${spec.metadata.internalName?lower_case?replace("_", "-")}-job" \
-DtemplateName="${spec.metadata.internalName}" \
-Dparameters="<#list spec.metadata.parameters as parameter>${parameter.name}=$${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}<#sep>,</#sep></#list>" \
<#if language == 'PYTHON'>
-f python
<#elseif flex>
-f v2/${spec.metadata.module!}
<#else>
-f v1
</#if>
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
<#if flex>
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).
<#else>
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).
</#if>

Here is an example of Terraform configuration:


```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

<#if flex>
resource "google_dataflow_flex_template_job" "${spec.metadata.internalName?lower_case}" {
<#else>
resource "google_dataflow_job" "${spec.metadata.internalName?lower_case}" {
</#if>

  provider          = google-beta
<#if flex>
  container_spec_gcs_path = "gs://dataflow-templates-${r"${var.region}"}/latest/flex/${spec.metadata.internalName}"
<#else>
  template_gcs_path = "gs://dataflow-templates-${r"${var.region}"}/latest/${spec.metadata.internalName}"
</#if>
  name              = "${spec.metadata.internalName?lower_case?replace("_", "-")}"
  region            = var.region
<#if !flex>
  temp_gcs_location = "gs://bucket-name-here/temp"
</#if>
  parameters        = {
<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>    ${parameter.name} = "${TemplateDocsUtils.printExampleOrDefaultValueVariable(parameter).replace("\"\"", "")}"
</#if>
</#list>
<#list spec.metadata.parameters as parameter><#if parameter.optional!false>    # ${parameter.name} = "${TemplateDocsUtils.printExampleOrDefaultValueVariable(parameter).replace("\"\"", "")}"
</#if>
</#list>
  }
}
```
