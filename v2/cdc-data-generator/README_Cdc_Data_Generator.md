
CDC Data Generator template
---
A template to generate synthetic CDC data based on a source schema.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **sinkType**: The type of sink to generate data for. Supported values: SPANNER, MYSQL.
* **sinkOptions**: GCS Path to a file containing JSON Options for the sink. For Spanner: {"projectId": "...", "instanceId": "...", "databaseId": "..."}. For MySQL: {"driverClassName": "...", "connectionUrl": "...", "username": "...", "password": "..."} or {"shardFilePath": "gs://..."} For example, `gs://your-bucket/path/to/sink_options.json`.

### Optional parameters

* **batchSize**: The maximum batch size for writing to the sink. Batches are partitioned by table name, shard, and operation type, and may be written in smaller sizes depending on Dataflow bundle size and execution context. Default is 1.
* **insertQps**: The target Insert QPS for each table. Default is 1000.
* **updateQps**: The target Update QPS for each table. Default is 0.
* **deleteQps**: The target Delete QPS for each table. Default is 0.
* **jdbcPoolSize**: The maximum number of connections to open per logical shard for JDBC (MySQL) sink. Default is 10.
* **updateInterval**: Interval cadence between successive UPDATEs for a given row. Default is 5 seconds.
* **deleteInterval**: Interval cadence trailing standard updates before a trailing DELETE is scheduled. Default is 5 seconds.
* **schemaConfig**: GCS Path to a file containing JSON overrides for data generation. For example, `gs://your-bucket/path/to/schema_config.json`.
* **dlqDirectory**: The GCS directory to write dead-letter queue records to. For example, `gs://your-bucket/dlq`.
* **maxParallelism**: The maximum parallelism (shards) for key-based redistribution. If not specified, default values are resolved based on the sink type.
* **disabledAlgorithms**: Comma separated algorithms to disable. If this value is set to `none`, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. For example, `SSLv3, RC4`.
* **extraFilesToStage**: Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. For example, `gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/templates/CdcDataGenerator.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl v2/cdc-data-generator
```

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
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="Cdc_Data_Generator" \
-pl v2/cdc-data-generator -am
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cdc_Data_Generator
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cdc_Data_Generator"

### Required
export SINK_TYPE=<sinkType>
export SINK_OPTIONS=<sinkOptions>

### Optional
export BATCH_SIZE=1
export INSERT_QPS=1000
export UPDATE_QPS=0
export DELETE_QPS=0
export JDBC_POOL_SIZE=10
export UPDATE_INTERVAL=5
export DELETE_INTERVAL=5
export SCHEMA_CONFIG=<schemaConfig>
export DLQ_DIRECTORY=<dlqDirectory>
export MAX_PARALLELISM=<maxParallelism>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow flex-template run "cdc-data-generator-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sinkType=$SINK_TYPE" \
  --parameters "sinkOptions=$SINK_OPTIONS" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "insertQps=$INSERT_QPS" \
  --parameters "updateQps=$UPDATE_QPS" \
  --parameters "deleteQps=$DELETE_QPS" \
  --parameters "jdbcPoolSize=$JDBC_POOL_SIZE" \
  --parameters "updateInterval=$UPDATE_INTERVAL" \
  --parameters "deleteInterval=$DELETE_INTERVAL" \
  --parameters "schemaConfig=$SCHEMA_CONFIG" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "maxParallelism=$MAX_PARALLELISM" \
  --parameters "disabledAlgorithms=$DISABLED_ALGORITHMS" \
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE"
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
export SINK_TYPE=<sinkType>
export SINK_OPTIONS=<sinkOptions>

### Optional
export BATCH_SIZE=1
export INSERT_QPS=1000
export UPDATE_QPS=0
export DELETE_QPS=0
export JDBC_POOL_SIZE=10
export UPDATE_INTERVAL=5
export DELETE_INTERVAL=5
export SCHEMA_CONFIG=<schemaConfig>
export DLQ_DIRECTORY=<dlqDirectory>
export MAX_PARALLELISM=<maxParallelism>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cdc-data-generator-job" \
-DtemplateName="Cdc_Data_Generator" \
-Dparameters="sinkType=$SINK_TYPE,sinkOptions=$SINK_OPTIONS,batchSize=$BATCH_SIZE,insertQps=$INSERT_QPS,updateQps=$UPDATE_QPS,deleteQps=$DELETE_QPS,jdbcPoolSize=$JDBC_POOL_SIZE,updateInterval=$UPDATE_INTERVAL,deleteInterval=$DELETE_INTERVAL,schemaConfig=$SCHEMA_CONFIG,dlqDirectory=$DLQ_DIRECTORY,maxParallelism=$MAX_PARALLELISM,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-f v2/cdc-data-generator
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
cd v2/cdc-data-generator/terraform/Cdc_Data_Generator
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

resource "google_dataflow_flex_template_job" "cdc_data_generator" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cdc_Data_Generator"
  name              = "cdc-data-generator"
  region            = var.region
  parameters        = {
    sinkType = "<sinkType>"
    sinkOptions = "<sinkOptions>"
    # batchSize = "1"
    # insertQps = "1000"
    # updateQps = "0"
    # deleteQps = "0"
    # jdbcPoolSize = "10"
    # updateInterval = "5"
    # deleteInterval = "5"
    # schemaConfig = "<schemaConfig>"
    # dlqDirectory = "<dlqDirectory>"
    # maxParallelism = "<maxParallelism>"
    # disabledAlgorithms = "<disabledAlgorithms>"
    # extraFilesToStage = "<extraFilesToStage>"
  }
}
```
