
Managed I/O to Managed I/O template
---
The Managed I/O to Managed I/O template is a flexible pipeline that can read from
any Managed I/O source and write to any Managed I/O sink. This template supports
all available Managed I/O connectors including ICEBERG, ICEBERG_CDC, KAFKA, and
BIGQUERY. The template uses Apache Beam's Managed API to provide a unified
interface for configuring different I/O connectors through simple configuration
maps.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/managed-io-to-managed-io)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Managed_IO_to_Managed_IO).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **sourceConnectorType**: The type of Managed I/O connector to use as source. Supported values: ICEBERG, ICEBERG_CDC, KAFKA, BIGQUERY.
* **sourceConfig**: JSON configuration for the source Managed I/O connector. The configuration format depends on the connector type. For example, for KAFKA: {"bootstrap_servers": "localhost:9092", "topic": "input-topic", "format": "JSON"}.
* **sinkConnectorType**: The type of Managed I/O connector to use as sink. Supported values: ICEBERG, KAFKA, BIGQUERY. Note: ICEBERG_CDC is only available for reading.
* **sinkConfig**: JSON configuration for the sink Managed I/O connector. The configuration format depends on the connector type. For example, for BIGQUERY: {"table": "project:dataset.table"}.

### Optional parameters

* **streaming**: Whether to run the pipeline in streaming mode. This is only supported for certain connector combinations. Default is false (batch mode).



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/managed-io-to-managed-io/src/main/java/com/google/cloud/teleport/v2/templates/ManagedIOToManagedIO.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
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
-DtemplateName="Managed_IO_to_Managed_IO" \
-f v2/managed-io-to-managed-io
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Managed_IO_to_Managed_IO
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Managed_IO_to_Managed_IO"

### Required
export SOURCE_CONNECTOR_TYPE=<sourceConnectorType>
export SOURCE_CONFIG=<sourceConfig>
export SINK_CONNECTOR_TYPE=<sinkConnectorType>
export SINK_CONFIG=<sinkConfig>

### Optional
export STREAMING=<streaming>

gcloud dataflow flex-template run "managed-io-to-managed-io-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceConnectorType=$SOURCE_CONNECTOR_TYPE" \
  --parameters "sourceConfig=$SOURCE_CONFIG" \
  --parameters "sinkConnectorType=$SINK_CONNECTOR_TYPE" \
  --parameters "sinkConfig=$SINK_CONFIG" \
  --parameters "streaming=$STREAMING"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export SOURCE_CONNECTOR_TYPE=<sourceConnectorType>
export SOURCE_CONFIG=<sourceConfig>
export SINK_CONNECTOR_TYPE=<sinkConnectorType>
export SINK_CONFIG=<sinkConfig>

### Optional
export STREAMING=<streaming>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="managed-io-to-managed-io-job" \
-DtemplateName="Managed_IO_to_Managed_IO" \
-Dparameters="sourceConnectorType=$SOURCE_CONNECTOR_TYPE,sourceConfig=$SOURCE_CONFIG,sinkConnectorType=$SINK_CONNECTOR_TYPE,sinkConfig=$SINK_CONFIG,streaming=$STREAMING" \
-f v2/managed-io-to-managed-io
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/managed-io-to-managed-io/terraform/Managed_IO_to_Managed_IO
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

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

resource "google_dataflow_flex_template_job" "managed_io_to_managed_io" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Managed_IO_to_Managed_IO"
  name              = "managed-io-to-managed-io"
  region            = var.region
  parameters        = {
    sourceConnectorType = "<sourceConnectorType>"
    sourceConfig = "<sourceConfig>"
    sinkConnectorType = "<sinkConnectorType>"
    sinkConfig = "<sinkConfig>"
    # streaming = "<streaming>"
  }
}
```
