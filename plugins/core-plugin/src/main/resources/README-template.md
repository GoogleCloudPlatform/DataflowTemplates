${spec.metadata.name} Template
---
${spec.metadata.description!?ensure_ends_with(".")}

<#if spec.metadata.googleReleased>
:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources.
</#if>

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>* **${parameter.name}** (${parameter.label}): ${parameter.helpText?ensure_ends_with(".")}
</#if></#list>

### Optional Parameters

<#list spec.metadata.parameters as parameter><#if parameter.optional!false>* **${parameter.name}** (${parameter.label}): ${parameter.helpText?ensure_ends_with(".")}
</#if></#list>

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

This README uses
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
for more information.

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
-pl v2/${spec.metadata.module!} -am
```

The command should print what is the template location on Cloud Storage:

```
Flex Template was staged! gs://{BUCKET}/{PATH}
```


#### Running the Template

**Using the staged template**:

You can use the path above to share or run the template.

To start a job with the template at any time using `gcloud`, you can use:

```shell
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/${spec.metadata.internalName}"
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${(parameter.defaultValue?c)!"<${parameter.name}>"}
</#if></#list>

### Optional
<#list spec.metadata.parameters as parameter><#if parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${(parameter.defaultValue?c)!"<${parameter.name}>"}
</#if></#list>

gcloud dataflow flex-template run "${spec.metadata.internalName?lower_case?replace("_", "-")}-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
<#list spec.metadata.parameters as parameter>
  --parameters "${parameter.name}=$${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}" <#sep>\</#sep>
</#list>
```


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Mandatory
<#list spec.metadata.parameters as parameter><#if !parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${(parameter.defaultValue?c)!"<${parameter.name}>"}
</#if></#list>

### Optional
<#list spec.metadata.parameters as parameter><#if parameter.optional!false>export ${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}=${(parameter.defaultValue?c)!"<${parameter.name}>"}
</#if></#list>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="${spec.metadata.internalName?lower_case?replace("_", "-")}-job" \
-DtemplateName="${spec.metadata.internalName}" \
-Dparameters="<#list spec.metadata.parameters as parameter>${parameter.name}=$${parameter.name?replace('([a-z])([A-Z])', '$1_$2', 'r')?upper_case?replace("-", "_")}<#sep>,</#sep></#list>" \
-pl v2/${spec.metadata.module!} -am
```
