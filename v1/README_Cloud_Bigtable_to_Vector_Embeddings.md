
Cloud Bigtable to Vector Embeddings template
---
The Bigtable to Vector Embedding template is a pipeline that reads data from a
Bigtable table and writes it to a Cloud Storage bucket in JSON format, for vector
embeddings.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-vector-embeddings)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Bigtable_to_Vector_Embeddings).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **bigtableProjectId** : The ID of the Google Cloud project of the Cloud Bigtable instance that you want to read data from.
* **bigtableInstanceId** : The ID of the Cloud Bigtable instance that contains the table.
* **bigtableTableId** : The ID of the Cloud Bigtable table to read.
* **filenamePrefix** : The prefix of the JSON file name. For example, "table1-". Defaults to: part.
* **idColumn** : The fully qualified column name where the ID is stored. In the format cf:col or _key.
* **embeddingColumn** : The fully qualified column name where the embeddings are stored. In the format cf:col or _key.

### Optional parameters

* **outputDirectory** : The Cloud Storage path where the output JSON files can be stored. (Example: gs://your-bucket/your-path/).
* **crowdingTagColumn** : The fully qualified column name where the crowding tag is stored. In the format cf:col or _key.
* **embeddingByteSize** : The byte size of each entry in the embeddings array. Use 4 for Float, and 8 for Double. Defaults to: 4.
* **allowRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as the `allow` restricts, with their alias. In the format cf:col->alias.
* **denyRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as the `deny` restricts, with their alias. In the format cf:col->alias.
* **intNumericRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as integer `numeric_restricts`, with their alias. In the format cf:col->alias.
* **floatNumericRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as float (4 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.
* **doubleNumericRestrictsMappings** : The comma separated fully qualified column names of the columns that should be used as double (8 bytes) `numeric_restricts`, with their alias. In the format cf:col->alias.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/bigtable/BigtableToVectorEmbeddings.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

### Building Template

This template is a Classic Template, meaning that the pipeline code will be
executed only once and the pipeline will be saved to Google Cloud Storage for
further reuse. Please check [Creating classic Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
and [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)
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
-DtemplateName="Cloud_Bigtable_to_Vector_Embeddings" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_Bigtable_to_Vector_Embeddings
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_Bigtable_to_Vector_Embeddings"

### Required
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export FILENAME_PREFIX=part
export ID_COLUMN=<idColumn>
export EMBEDDING_COLUMN=<embeddingColumn>

### Optional
export OUTPUT_DIRECTORY=<outputDirectory>
export CROWDING_TAG_COLUMN=<crowdingTagColumn>
export EMBEDDING_BYTE_SIZE=4
export ALLOW_RESTRICTS_MAPPINGS=<allowRestrictsMappings>
export DENY_RESTRICTS_MAPPINGS=<denyRestrictsMappings>
export INT_NUMERIC_RESTRICTS_MAPPINGS=<intNumericRestrictsMappings>
export FLOAT_NUMERIC_RESTRICTS_MAPPINGS=<floatNumericRestrictsMappings>
export DOUBLE_NUMERIC_RESTRICTS_MAPPINGS=<doubleNumericRestrictsMappings>

gcloud dataflow jobs run "cloud-bigtable-to-vector-embeddings-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "bigtableProjectId=$BIGTABLE_PROJECT_ID" \
  --parameters "bigtableInstanceId=$BIGTABLE_INSTANCE_ID" \
  --parameters "bigtableTableId=$BIGTABLE_TABLE_ID" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "filenamePrefix=$FILENAME_PREFIX" \
  --parameters "idColumn=$ID_COLUMN" \
  --parameters "embeddingColumn=$EMBEDDING_COLUMN" \
  --parameters "crowdingTagColumn=$CROWDING_TAG_COLUMN" \
  --parameters "embeddingByteSize=$EMBEDDING_BYTE_SIZE" \
  --parameters "allowRestrictsMappings=$ALLOW_RESTRICTS_MAPPINGS" \
  --parameters "denyRestrictsMappings=$DENY_RESTRICTS_MAPPINGS" \
  --parameters "intNumericRestrictsMappings=$INT_NUMERIC_RESTRICTS_MAPPINGS" \
  --parameters "floatNumericRestrictsMappings=$FLOAT_NUMERIC_RESTRICTS_MAPPINGS" \
  --parameters "doubleNumericRestrictsMappings=$DOUBLE_NUMERIC_RESTRICTS_MAPPINGS"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>
export FILENAME_PREFIX=part
export ID_COLUMN=<idColumn>
export EMBEDDING_COLUMN=<embeddingColumn>

### Optional
export OUTPUT_DIRECTORY=<outputDirectory>
export CROWDING_TAG_COLUMN=<crowdingTagColumn>
export EMBEDDING_BYTE_SIZE=4
export ALLOW_RESTRICTS_MAPPINGS=<allowRestrictsMappings>
export DENY_RESTRICTS_MAPPINGS=<denyRestrictsMappings>
export INT_NUMERIC_RESTRICTS_MAPPINGS=<intNumericRestrictsMappings>
export FLOAT_NUMERIC_RESTRICTS_MAPPINGS=<floatNumericRestrictsMappings>
export DOUBLE_NUMERIC_RESTRICTS_MAPPINGS=<doubleNumericRestrictsMappings>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-bigtable-to-vector-embeddings-job" \
-DtemplateName="Cloud_Bigtable_to_Vector_Embeddings" \
-Dparameters="bigtableProjectId=$BIGTABLE_PROJECT_ID,bigtableInstanceId=$BIGTABLE_INSTANCE_ID,bigtableTableId=$BIGTABLE_TABLE_ID,outputDirectory=$OUTPUT_DIRECTORY,filenamePrefix=$FILENAME_PREFIX,idColumn=$ID_COLUMN,embeddingColumn=$EMBEDDING_COLUMN,crowdingTagColumn=$CROWDING_TAG_COLUMN,embeddingByteSize=$EMBEDDING_BYTE_SIZE,allowRestrictsMappings=$ALLOW_RESTRICTS_MAPPINGS,denyRestrictsMappings=$DENY_RESTRICTS_MAPPINGS,intNumericRestrictsMappings=$INT_NUMERIC_RESTRICTS_MAPPINGS,floatNumericRestrictsMappings=$FLOAT_NUMERIC_RESTRICTS_MAPPINGS,doubleNumericRestrictsMappings=$DOUBLE_NUMERIC_RESTRICTS_MAPPINGS" \
-f v1
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v1/terraform/Cloud_Bigtable_to_Vector_Embeddings
terraform init
terraform apply
```

To use
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
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

resource "google_dataflow_job" "cloud_bigtable_to_vector_embeddings" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cloud_Bigtable_to_Vector_Embeddings"
  name              = "cloud-bigtable-to-vector-embeddings"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    bigtableProjectId = "<bigtableProjectId>"
    bigtableInstanceId = "<bigtableInstanceId>"
    bigtableTableId = "<bigtableTableId>"
    filenamePrefix = "part"
    idColumn = "<idColumn>"
    embeddingColumn = "<embeddingColumn>"
    # outputDirectory = "gs://your-bucket/your-path/"
    # crowdingTagColumn = "<crowdingTagColumn>"
    # embeddingByteSize = "4"
    # allowRestrictsMappings = "<allowRestrictsMappings>"
    # denyRestrictsMappings = "<denyRestrictsMappings>"
    # intNumericRestrictsMappings = "<intNumericRestrictsMappings>"
    # floatNumericRestrictsMappings = "<floatNumericRestrictsMappings>"
    # doubleNumericRestrictsMappings = "<doubleNumericRestrictsMappings>"
  }
}
```
