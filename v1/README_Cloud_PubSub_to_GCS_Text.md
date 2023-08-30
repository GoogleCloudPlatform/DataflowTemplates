
Pub/Sub to Text Files on Cloud Storage template
---
The Pub/Sub to Cloud Storage Text template is a streaming pipeline that reads
records from Pub/Sub topic and saves them as a series of Cloud Storage files in
text format. The template can be used as a quick way to save data in Pub/Sub for
future use. By default, the template generates a new file every 5 minutes.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-topic-to-text)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_GCS_Text).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **outputDirectory** (Output file directory in Cloud Storage): The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters.
* **outputFilenamePrefix** (Output filename prefix of the files to write): The prefix to place on each windowed file. Defaults to: output.

### Optional Parameters

* **inputTopic** (Pub/Sub input topic): Pub/Sub topic to read the input from, in the format of 'projects/your-project-id/topics/your-topic-name'.
* **userTempLocation** (User provided temp location): The user provided directory to output temporary files to. Must end with a slash.
* **outputFilenameSuffix** (Output filename suffix of the files to write): The suffix to place on each windowed file. Typically a file extension such as .txt or .csv. Defaults to empty.
* **outputShardTemplate** (Shard template): Defines the unique/dynamic portion of each windowed file. Recommended: use the default (W-P-SS-of-NN). At runtime, 'W' is replaced with the window date range and 'P' is replaced with the pane info. Repeating sequences of the letters 'S' or 'N' are replaced with the shard number and number of shards respectively. The pipeline assumes a single file output and will produce the text of '00-of-01' by default.
* **yearPattern** (Custom Year Pattern to use for the output directory): Pattern for formatting the year. Must be one or more of 'y' or 'Y'. Case makes no difference in the year. The pattern can be optionally wrapped by characters that aren't either alphanumeric or the directory ('/') character. Defaults to 'YYYY'.
* **monthPattern** (Custom Month Pattern to use for the output directory): Pattern for formatting the month. Must be one or more of the 'M' character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory ('/') character. Defaults to 'MM'.
* **dayPattern** (Custom Day Pattern to use for the output directory): Pattern for formatting the day. Must be one or more of 'd' for day of month or 'D' for day of year. Case makes no difference in the year. The pattern can be optionally wrapped by characters that aren't either alphanumeric or the directory ('/') character. Defaults to 'dd'.
* **hourPattern** (Custom Hour Pattern to use for the output directory): Pattern for formatting the hour. Must be one or more of the 'H' character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory ('/') character. Defaults to 'HH'.
* **minutePattern** (Custom Minute Pattern to use for the output directory): Pattern for formatting the minute. Must be one or more of the 'm' character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory ('/') character. Defaults to 'mm'.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/PubsubToText.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

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
-DtemplateName="Cloud_PubSub_to_GCS_Text" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cloud_PubSub_to_GCS_Text
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cloud_PubSub_to_GCS_Text"

### Required
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FILENAME_PREFIX=output

### Optional
export INPUT_TOPIC=<inputTopic>
export USER_TEMP_LOCATION=<userTempLocation>
export OUTPUT_FILENAME_SUFFIX=""
export OUTPUT_SHARD_TEMPLATE=W-P-SS-of-NN
export YEAR_PATTERN=<yearPattern>
export MONTH_PATTERN=<monthPattern>
export DAY_PATTERN=<dayPattern>
export HOUR_PATTERN=<hourPattern>
export MINUTE_PATTERN=<minutePattern>

gcloud dataflow jobs run "cloud-pubsub-to-gcs-text-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "userTempLocation=$USER_TEMP_LOCATION" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX" \
  --parameters "outputShardTemplate=$OUTPUT_SHARD_TEMPLATE" \
  --parameters "yearPattern=$YEAR_PATTERN" \
  --parameters "monthPattern=$MONTH_PATTERN" \
  --parameters "dayPattern=$DAY_PATTERN" \
  --parameters "hourPattern=$HOUR_PATTERN" \
  --parameters "minutePattern=$MINUTE_PATTERN"
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
export OUTPUT_DIRECTORY=<outputDirectory>
export OUTPUT_FILENAME_PREFIX=output

### Optional
export INPUT_TOPIC=<inputTopic>
export USER_TEMP_LOCATION=<userTempLocation>
export OUTPUT_FILENAME_SUFFIX=""
export OUTPUT_SHARD_TEMPLATE=W-P-SS-of-NN
export YEAR_PATTERN=<yearPattern>
export MONTH_PATTERN=<monthPattern>
export DAY_PATTERN=<dayPattern>
export HOUR_PATTERN=<hourPattern>
export MINUTE_PATTERN=<minutePattern>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-gcs-text-job" \
-DtemplateName="Cloud_PubSub_to_GCS_Text" \
-Dparameters="inputTopic=$INPUT_TOPIC,outputDirectory=$OUTPUT_DIRECTORY,userTempLocation=$USER_TEMP_LOCATION,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX,outputShardTemplate=$OUTPUT_SHARD_TEMPLATE,yearPattern=$YEAR_PATTERN,monthPattern=$MONTH_PATTERN,dayPattern=$DAY_PATTERN,hourPattern=$HOUR_PATTERN,minutePattern=$MINUTE_PATTERN" \
-pl v1 \
-am
```
