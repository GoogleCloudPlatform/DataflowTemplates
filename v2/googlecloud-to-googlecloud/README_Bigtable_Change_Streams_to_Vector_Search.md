
Bigtable Change Streams to Vector Search template
---
Streaming pipeline. Streams Bigtable data change records and writes them into
Vertex AI Vector Search using Dataflow Runner V2.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-change-streams-to-vector-search)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Bigtable_Change_Streams_to_Vector_Search).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **embeddingColumn** : The fully qualified column name where the embeddings are stored. In the format cf:col.
* **embeddingByteSize** : The byte size of each entry in the embeddings array. Use 4 for Float, and 8 for Double. Defaults to: 4.
* **vectorSearchIndex** : The Vector Search Index where changes will be streamed, in the format 'projects/{projectID}/locations/{region}/indexes/{indexID}' (no leading or trailing spaces) (Example: projects/123/locations/us-east1/indexes/456).
* **bigtableChangeStreamAppProfile** : The Bigtable application profile ID. The application profile must use single-cluster routing and allow single-row transactions.
* **bigtableReadInstanceId** : The source Bigtable instance ID.
* **bigtableReadTableId** : The source Bigtable table ID.

### Optional parameters

* **bigtableMetadataTableTableId** : Table ID used for creating the metadata table.
* **crowdingTagColumn** : The fully qualified column name where the crowding tag is stored. In the format cf:col.
* **allowRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as the `allow` restricts, with their alias. In the format cf:col->alias.
* **denyRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as the `deny` restricts, with their alias. In the format cf:col->alias.
* **intNumericRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as integer `numeric_restricts`, with their alias. In the format cf:col->alias.
* **floatNumericRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as float (4 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.
* **doubleNumericRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as double (8 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.
* **upsertMaxBatchSize** : The maximum number of upserts to buffer before upserting the batch to the Vector Search Index. Batches will be sent when there are either upsertBatchSize records ready, or any record has been waiting upsertBatchDelay time has passed. (Example: 10). Defaults to: 10.
* **upsertMaxBufferDuration** : The maximum delay before a batch of upserts is sent to Vector Search.Batches will be sent when there are either upsertBatchSize records ready, or any record has been waiting upsertBatchDelay time has passed. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 10s). Defaults to: 10s.
* **deleteMaxBatchSize** : The maximum number of deletes to buffer before deleting the batch from the Vector Search Index. Batches will be sent when there are either deleteBatchSize records ready, or any record has been waiting deleteBatchDelay time has passed. (Example: 10). Defaults to: 10.
* **deleteMaxBufferDuration** : The maximum delay before a batch of deletes is sent to Vector Search.Batches will be sent when there are either deleteBatchSize records ready, or any record has been waiting deleteBatchDelay time has passed. Allowed formats are: Ns (for seconds, example: 5s), Nm (for minutes, example: 12m), Nh (for hours, example: 2h). (Example: 10s). Defaults to: 10s.
* **dlqDirectory** : The path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job's temp location. The default value is enough under most conditions.
* **bigtableChangeStreamMetadataInstanceId** : The Bigtable change streams metadata instance ID. Defaults to empty.
* **bigtableChangeStreamMetadataTableTableId** : The ID of the Bigtable change streams connector metadata table. If not provided, a Bigtable change streams connector metadata table is automatically created during pipeline execution. Defaults to empty.
* **bigtableChangeStreamCharset** : The Bigtable change streams charset name. Defaults to: UTF-8.
* **bigtableChangeStreamStartTimestamp** : The starting timestamp (https://tools.ietf.org/html/rfc3339), inclusive, to use for reading change streams. For example, `2022-05-05T07:59:59Z`. Defaults to the timestamp of the pipeline start time.
* **bigtableChangeStreamIgnoreColumnFamilies** : A comma-separated list of column family name changes to ignore. Defaults to empty.
* **bigtableChangeStreamIgnoreColumns** : A comma-separated list of column name changes to ignore. Defaults to empty.
* **bigtableChangeStreamName** : A unique name for the client pipeline. Lets you resume processing from the point at which a previously running pipeline stopped. Defaults to an automatically generated name. See the Dataflow job logs for the value used.
* **bigtableChangeStreamResume** : When set to `true`, a new pipeline resumes processing from the point at which a previously running pipeline with the same `bigtableChangeStreamName` value stopped. If the pipeline with the given `bigtableChangeStreamName` value has never run, a new pipeline doesn't start. When set to `false`, a new pipeline starts. If a pipeline with the same `bigtableChangeStreamName` value has already run for the given source, a new pipeline doesn't start. Defaults to `false`.
* **bigtableReadProjectId** : The Bigtable project ID. The default is the project for the Dataflow job.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstovectorsearch/BigtableChangeStreamsToVectorSearch.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="Bigtable_Change_Streams_to_Vector_Search" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Bigtable_Change_Streams_to_Vector_Search
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Bigtable_Change_Streams_to_Vector_Search"

### Required
export EMBEDDING_COLUMN=<embeddingColumn>
export EMBEDDING_BYTE_SIZE=4
export VECTOR_SEARCH_INDEX=<vectorSearchIndex>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export BIGTABLE_METADATA_TABLE_TABLE_ID=<bigtableMetadataTableTableId>
export CROWDING_TAG_COLUMN=<crowdingTagColumn>
export ALLOW_RESTRICTS_MAPPINGS=<allowRestrictsMappings>
export DENY_RESTRICTS_MAPPINGS=<denyRestrictsMappings>
export INT_NUMERIC_RESTRICTS_MAPPINGS=<intNumericRestrictsMappings>
export FLOAT_NUMERIC_RESTRICTS_MAPPINGS=<floatNumericRestrictsMappings>
export DOUBLE_NUMERIC_RESTRICTS_MAPPINGS=<doubleNumericRestrictsMappings>
export UPSERT_MAX_BATCH_SIZE=10
export UPSERT_MAX_BUFFER_DURATION=10s
export DELETE_MAX_BATCH_SIZE=10
export DELETE_MAX_BUFFER_DURATION=10s
export DLQ_DIRECTORY=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""

gcloud dataflow flex-template run "bigtable-change-streams-to-vector-search-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigtableMetadataTableTableId=$BIGTABLE_METADATA_TABLE_TABLE_ID" \
  --parameters "embeddingColumn=$EMBEDDING_COLUMN" \
  --parameters "crowdingTagColumn=$CROWDING_TAG_COLUMN" \
  --parameters "embeddingByteSize=$EMBEDDING_BYTE_SIZE" \
  --parameters "allowRestrictsMappings=$ALLOW_RESTRICTS_MAPPINGS" \
  --parameters "denyRestrictsMappings=$DENY_RESTRICTS_MAPPINGS" \
  --parameters "intNumericRestrictsMappings=$INT_NUMERIC_RESTRICTS_MAPPINGS" \
  --parameters "floatNumericRestrictsMappings=$FLOAT_NUMERIC_RESTRICTS_MAPPINGS" \
  --parameters "doubleNumericRestrictsMappings=$DOUBLE_NUMERIC_RESTRICTS_MAPPINGS" \
  --parameters "upsertMaxBatchSize=$UPSERT_MAX_BATCH_SIZE" \
  --parameters "upsertMaxBufferDuration=$UPSERT_MAX_BUFFER_DURATION" \
  --parameters "deleteMaxBatchSize=$DELETE_MAX_BATCH_SIZE" \
  --parameters "deleteMaxBufferDuration=$DELETE_MAX_BUFFER_DURATION" \
  --parameters "vectorSearchIndex=$VECTOR_SEARCH_INDEX" \
  --parameters "dlqDirectory=$DLQ_DIRECTORY" \
  --parameters "bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID" \
  --parameters "bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID" \
  --parameters "bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE" \
  --parameters "bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET" \
  --parameters "bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP" \
  --parameters "bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES" \
  --parameters "bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS" \
  --parameters "bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME" \
  --parameters "bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME" \
  --parameters "bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID" \
  --parameters "bigtableReadTableId=$BIGTABLE_READ_TABLE_ID" \
  --parameters "bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID"
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
export EMBEDDING_COLUMN=<embeddingColumn>
export EMBEDDING_BYTE_SIZE=4
export VECTOR_SEARCH_INDEX=<vectorSearchIndex>
export BIGTABLE_CHANGE_STREAM_APP_PROFILE=<bigtableChangeStreamAppProfile>
export BIGTABLE_READ_INSTANCE_ID=<bigtableReadInstanceId>
export BIGTABLE_READ_TABLE_ID=<bigtableReadTableId>

### Optional
export BIGTABLE_METADATA_TABLE_TABLE_ID=<bigtableMetadataTableTableId>
export CROWDING_TAG_COLUMN=<crowdingTagColumn>
export ALLOW_RESTRICTS_MAPPINGS=<allowRestrictsMappings>
export DENY_RESTRICTS_MAPPINGS=<denyRestrictsMappings>
export INT_NUMERIC_RESTRICTS_MAPPINGS=<intNumericRestrictsMappings>
export FLOAT_NUMERIC_RESTRICTS_MAPPINGS=<floatNumericRestrictsMappings>
export DOUBLE_NUMERIC_RESTRICTS_MAPPINGS=<doubleNumericRestrictsMappings>
export UPSERT_MAX_BATCH_SIZE=10
export UPSERT_MAX_BUFFER_DURATION=10s
export DELETE_MAX_BATCH_SIZE=10
export DELETE_MAX_BUFFER_DURATION=10s
export DLQ_DIRECTORY=""
export BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID=""
export BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID=""
export BIGTABLE_CHANGE_STREAM_CHARSET=UTF-8
export BIGTABLE_CHANGE_STREAM_START_TIMESTAMP=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES=""
export BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS=""
export BIGTABLE_CHANGE_STREAM_NAME=<bigtableChangeStreamName>
export BIGTABLE_CHANGE_STREAM_RESUME=false
export BIGTABLE_READ_PROJECT_ID=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigtable-change-streams-to-vector-search-job" \
-DtemplateName="Bigtable_Change_Streams_to_Vector_Search" \
-Dparameters="bigtableMetadataTableTableId=$BIGTABLE_METADATA_TABLE_TABLE_ID,embeddingColumn=$EMBEDDING_COLUMN,crowdingTagColumn=$CROWDING_TAG_COLUMN,embeddingByteSize=$EMBEDDING_BYTE_SIZE,allowRestrictsMappings=$ALLOW_RESTRICTS_MAPPINGS,denyRestrictsMappings=$DENY_RESTRICTS_MAPPINGS,intNumericRestrictsMappings=$INT_NUMERIC_RESTRICTS_MAPPINGS,floatNumericRestrictsMappings=$FLOAT_NUMERIC_RESTRICTS_MAPPINGS,doubleNumericRestrictsMappings=$DOUBLE_NUMERIC_RESTRICTS_MAPPINGS,upsertMaxBatchSize=$UPSERT_MAX_BATCH_SIZE,upsertMaxBufferDuration=$UPSERT_MAX_BUFFER_DURATION,deleteMaxBatchSize=$DELETE_MAX_BATCH_SIZE,deleteMaxBufferDuration=$DELETE_MAX_BUFFER_DURATION,vectorSearchIndex=$VECTOR_SEARCH_INDEX,dlqDirectory=$DLQ_DIRECTORY,bigtableChangeStreamMetadataInstanceId=$BIGTABLE_CHANGE_STREAM_METADATA_INSTANCE_ID,bigtableChangeStreamMetadataTableTableId=$BIGTABLE_CHANGE_STREAM_METADATA_TABLE_TABLE_ID,bigtableChangeStreamAppProfile=$BIGTABLE_CHANGE_STREAM_APP_PROFILE,bigtableChangeStreamCharset=$BIGTABLE_CHANGE_STREAM_CHARSET,bigtableChangeStreamStartTimestamp=$BIGTABLE_CHANGE_STREAM_START_TIMESTAMP,bigtableChangeStreamIgnoreColumnFamilies=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMN_FAMILIES,bigtableChangeStreamIgnoreColumns=$BIGTABLE_CHANGE_STREAM_IGNORE_COLUMNS,bigtableChangeStreamName=$BIGTABLE_CHANGE_STREAM_NAME,bigtableChangeStreamResume=$BIGTABLE_CHANGE_STREAM_RESUME,bigtableReadInstanceId=$BIGTABLE_READ_INSTANCE_ID,bigtableReadTableId=$BIGTABLE_READ_TABLE_ID,bigtableReadProjectId=$BIGTABLE_READ_PROJECT_ID" \
-f v2/googlecloud-to-googlecloud
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/googlecloud-to-googlecloud/terraform/Bigtable_Change_Streams_to_Vector_Search
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

resource "google_dataflow_flex_template_job" "bigtable_change_streams_to_vector_search" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Bigtable_Change_Streams_to_Vector_Search"
  name              = "bigtable-change-streams-to-vector-search"
  region            = var.region
  parameters        = {
    embeddingColumn = "<embeddingColumn>"
    embeddingByteSize = "4"
    vectorSearchIndex = "projects/123/locations/us-east1/indexes/456"
    bigtableChangeStreamAppProfile = "<bigtableChangeStreamAppProfile>"
    bigtableReadInstanceId = "<bigtableReadInstanceId>"
    bigtableReadTableId = "<bigtableReadTableId>"
    # bigtableMetadataTableTableId = "<bigtableMetadataTableTableId>"
    # crowdingTagColumn = "<crowdingTagColumn>"
    # allowRestrictsMappings = "<allowRestrictsMappings>"
    # denyRestrictsMappings = "<denyRestrictsMappings>"
    # intNumericRestrictsMappings = "<intNumericRestrictsMappings>"
    # floatNumericRestrictsMappings = "<floatNumericRestrictsMappings>"
    # doubleNumericRestrictsMappings = "<doubleNumericRestrictsMappings>"
    # upsertMaxBatchSize = "10"
    # upsertMaxBufferDuration = "10s"
    # deleteMaxBatchSize = "10"
    # deleteMaxBufferDuration = "10s"
    # dlqDirectory = ""
    # bigtableChangeStreamMetadataInstanceId = ""
    # bigtableChangeStreamMetadataTableTableId = ""
    # bigtableChangeStreamCharset = "UTF-8"
    # bigtableChangeStreamStartTimestamp = ""
    # bigtableChangeStreamIgnoreColumnFamilies = ""
    # bigtableChangeStreamIgnoreColumns = ""
    # bigtableChangeStreamName = "<bigtableChangeStreamName>"
    # bigtableChangeStreamResume = "false"
    # bigtableReadProjectId = ""
  }
}
```
