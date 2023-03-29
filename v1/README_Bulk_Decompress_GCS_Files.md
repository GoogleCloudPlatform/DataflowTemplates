Bulk Decompress Files on Cloud Storage Template
---
A pipeline which decompresses files on Cloud Storage to a specified location. Supported formats: Bzip2, deflate, and gzip.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bulk-decompress-cloud-storage)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **inputFilePattern** (Input Cloud Storage File(s)): The Cloud Storage location of the files you'd like to process. (Example: gs://your-bucket/your-files/*.gz).
* **outputDirectory** (Output file directory in Cloud Storage): The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters. (Example: gs://your-bucket/decompressed/).
* **outputFailureFile** (The output file for failures during the decompression process): The output file to write failures to during the decompression process. If there are no failures, the file will still be created but will be empty. The contents will be one line for each file which failed decompression in CSV format (Filename, Error). Note that this parameter will allow the pipeline to continue processing in the event of a failure. (Example: gs://your-bucket/decompressed/failed.csv).

### Optional Parameters


## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

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
-DtemplateName="Bulk_Decompress_GCS_Files" \
-pl v1 \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Bulk_Decompress_GCS_Files
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Bulk_Decompress_GCS_Files"

### Mandatory
export INPUT_FILE_PATTERN=<inputFilePattern>
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FAILURE_FILE=<outputFailureFile>

### Optional

gcloud dataflow jobs run "bulk-decompress-gcs-files-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputFailureFile=$OUTPUT_FAILURE_FILE"
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

### Mandatory
export INPUT_FILE_PATTERN=<inputFilePattern>
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FAILURE_FILE=<outputFailureFile>

### Optional

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bulk-decompress-gcs-files-job" \
-DtemplateName="Bulk_Decompress_GCS_Files" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,outputDirectory=$OUTPUT_DIRECTORY,outputFailureFile=$OUTPUT_FAILURE_FILE" \
-pl v1 \
-am
```
