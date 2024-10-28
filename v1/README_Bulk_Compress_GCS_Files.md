
Bulk Compress Files on Cloud Storage template
---
The Bulk Compress Cloud Storage Files template is a batch pipeline that
compresses files on Cloud Storage to a specified location. This template can be
useful when you need to compress large batches of files as part of a periodic
archival process. The supported compression modes are: BZIP2, DEFLATE, GZIP.
Files output to the destination location will follow a naming schema of original
filename appended with the compression mode extension. The extensions appended
will be one of: .bzip2, .deflate, .gz.

Any errors which occur during the compression process will be output to the
failure file in CSV format of filename, error message. If no failures occur while
running the pipeline, the error file will still be created but will contain no
error records.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bulk-compress-cloud-storage)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Bulk_Compress_GCS_Files).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern** : The Cloud Storage location of the files you'd like to process. (Example: gs://your-bucket/your-files/*.txt).
* **outputDirectory** : The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters. (Example: gs://your-bucket/your-path).
* **outputFailureFile** : The error log output file to use for write failures that occur during compression. The contents will be one line for each file which failed compression. Note that this parameter will allow the pipeline to continue processing in the event of a failure. (Example: gs://your-bucket/compressed/failed.csv).
* **compression** : The compression algorithm used to compress the matched files. Valid algorithms: BZIP2, DEFLATE, GZIP.

### Optional parameters

* **outputFilenameSuffix** : Output filename suffix of the files to write. Defaults to .bzip2, .deflate or .gz depending on the compression algorithm.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/BulkCompressor.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="Bulk_Compress_GCS_Files" \
-f v1
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Bulk_Compress_GCS_Files
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Bulk_Compress_GCS_Files"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FAILURE_FILE=<outputFailureFile>
export COMPRESSION=<compression>

### Optional
export OUTPUT_FILENAME_SUFFIX=<outputFilenameSuffix>

gcloud dataflow jobs run "bulk-compress-gcs-files-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputFailureFile=$OUTPUT_FAILURE_FILE" \
  --parameters "compression=$COMPRESSION" \
  --parameters "outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX"
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
export INPUT_FILE_PATTERN=<inputFilePattern>
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FAILURE_FILE=<outputFailureFile>
export COMPRESSION=<compression>

### Optional
export OUTPUT_FILENAME_SUFFIX=<outputFilenameSuffix>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bulk-compress-gcs-files-job" \
-DtemplateName="Bulk_Compress_GCS_Files" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,outputDirectory=$OUTPUT_DIRECTORY,outputFailureFile=$OUTPUT_FAILURE_FILE,compression=$COMPRESSION,outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX" \
-f v1
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
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v1/terraform/Bulk_Compress_GCS_Files
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

resource "google_dataflow_job" "bulk_compress_gcs_files" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Bulk_Compress_GCS_Files"
  name              = "bulk-compress-gcs-files"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    inputFilePattern = "gs://your-bucket/your-files/*.txt"
    outputDirectory = "gs://your-bucket/your-path"
    outputFailureFile = "gs://your-bucket/compressed/failed.csv"
    compression = "<compression>"
    # outputFilenameSuffix = "<outputFilenameSuffix>"
  }
}
```
