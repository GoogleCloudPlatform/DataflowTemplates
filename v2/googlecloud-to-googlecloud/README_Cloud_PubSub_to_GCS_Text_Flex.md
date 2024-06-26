
Pub/Sub Subscription or Topic to Text Files on Cloud Storage template
---
The Pub/Sub Topic or Subscription to Cloud Storage Text template is a streaming
pipeline that reads records from Pub/Sub and saves them as a series of Cloud
Storage files in text format. The template can be used as a quick way to save
data in Pub/Sub for future use. By default, the template generates a new file
every 5 minutes.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-topic-subscription-to-text)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_GCS_Text_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **outputDirectory** : The path and filename prefix to write write output files to. This value must end in a slash. (Example: gs://your-bucket/your-path).

### Optional parameters

* **inputTopic** : The Pub/Sub topic to read the input from. The topic name should be in the format `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`. If this parameter is provided don't use `inputSubscription`. (Example: projects/your-project-id/topics/your-topic-name).
* **inputSubscription** : The Pub/Sub subscription to read the input from. The subscription name uses the format `projects/<PROJECT_ID>/subscription/<SUBSCRIPTION_NAME>`. If this parameter is provided, don't use `inputTopic`. (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **userTempLocation** : The user provided directory to output temporary files to. Must end with a slash.
* **outputFilenamePrefix** : The prefix to place on each windowed file. (Example: output-). Defaults to: output.
* **outputFilenameSuffix** : The suffix to place on each windowed file, typically a file extension such as `.txt` or `.csv`. (Example: .txt). Defaults to empty.
* **outputShardTemplate** : The shard template defines the dynamic portion of each windowed file. By default, the pipeline uses a single shard for output to the file system within each window. This means that all data outputs into a single file per window. The `outputShardTemplate` defaults to `W-P-SS-of-NN` where `W` is the window date range, `P` is the pane info, `S` is the shard number, and `N` is the number of shards. In case of a single file, the `SS-of-NN` portion of the `outputShardTemplate` is `00-of-01`.
* **numShards** : The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Defaults to: 0.
* **windowDuration** : The window duration is the interval in which data is written to the output directory. Configure the duration based on the pipeline's throughput. For example, a higher throughput might require smaller window sizes so that the data fits into memory. Defaults to 5m (5 minutes), with a minimum of 1s (1 second). Allowed formats are: [int]s (for seconds, example: 5s), [int]m (for minutes, example: 12m), [int]h (for hours, example: 2h). (Example: 5m).
* **yearPattern** : Pattern for formatting the year. Must be one or more of 'y' or 'Y'. Case makes no difference in the year. The pattern can be optionally wrapped by characters that aren't either alphanumeric or the directory ('/') character. Defaults to 'YYYY'.
* **monthPattern** : Pattern for formatting the month. Must be one or more of the 'M' character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory ('/') character. Defaults to 'MM'.
* **dayPattern** : Pattern for formatting the day. Must be one or more of 'd' for day of month or 'D' for day of year. Case makes no difference in the year. The pattern can be optionally wrapped by characters that aren't either alphanumeric or the directory ('/') character. Defaults to 'dd'.
* **hourPattern** : Pattern for formatting the hour. Must be one or more of the 'H' character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory ('/') character. Defaults to 'HH'.
* **minutePattern** : Pattern for formatting the minute. Must be one or more of the 'm' character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory ('/') character. Defaults to 'mm'.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/pubsubtotext/PubsubToText.java)

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
-DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_PubSub_to_GCS_Text_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_PubSub_to_GCS_Text_Flex"

### Required
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export INPUT_TOPIC=<inputTopic>
export INPUT_SUBSCRIPTION=<inputSubscription>
export USER_TEMP_LOCATION=<userTempLocation>
export OUTPUT_FILENAME_PREFIX=output
export OUTPUT_FILENAME_SUFFIX=""
export OUTPUT_SHARD_TEMPLATE=W-P-SS-of-NN
export NUM_SHARDS=0
export WINDOW_DURATION=5m
export YEAR_PATTERN=YYYY
export MONTH_PATTERN=MM
export DAY_PATTERN=dd
export HOUR_PATTERN=HH
export MINUTE_PATTERN=mm

gcloud dataflow flex-template run "cloud-pubsub-to-gcs-text-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "userTempLocation=$USER_TEMP_LOCATION" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX" \
  --parameters "outputShardTemplate=$OUTPUT_SHARD_TEMPLATE" \
  --parameters "numShards=$NUM_SHARDS" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "yearPattern=$YEAR_PATTERN" \
  --parameters "monthPattern=$MONTH_PATTERN" \
  --parameters "dayPattern=$DAY_PATTERN" \
  --parameters "hourPattern=$HOUR_PATTERN" \
  --parameters "minutePattern=$MINUTE_PATTERN"
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
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export INPUT_TOPIC=<inputTopic>
export INPUT_SUBSCRIPTION=<inputSubscription>
export USER_TEMP_LOCATION=<userTempLocation>
export OUTPUT_FILENAME_PREFIX=output
export OUTPUT_FILENAME_SUFFIX=""
export OUTPUT_SHARD_TEMPLATE=W-P-SS-of-NN
export NUM_SHARDS=0
export WINDOW_DURATION=5m
export YEAR_PATTERN=YYYY
export MONTH_PATTERN=MM
export DAY_PATTERN=dd
export HOUR_PATTERN=HH
export MINUTE_PATTERN=mm

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-gcs-text-flex-job" \
-DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
-Dparameters="inputTopic=$INPUT_TOPIC,inputSubscription=$INPUT_SUBSCRIPTION,outputDirectory=$OUTPUT_DIRECTORY,userTempLocation=$USER_TEMP_LOCATION,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX,outputShardTemplate=$OUTPUT_SHARD_TEMPLATE,numShards=$NUM_SHARDS,windowDuration=$WINDOW_DURATION,yearPattern=$YEAR_PATTERN,monthPattern=$MONTH_PATTERN,dayPattern=$DAY_PATTERN,hourPattern=$HOUR_PATTERN,minutePattern=$MINUTE_PATTERN" \
-f v2/googlecloud-to-googlecloud
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
cd v2/googlecloud-to-googlecloud/terraform/Cloud_PubSub_to_GCS_Text_Flex
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

resource "google_dataflow_flex_template_job" "cloud_pubsub_to_gcs_text_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_PubSub_to_GCS_Text_Flex"
  name              = "cloud-pubsub-to-gcs-text-flex"
  region            = var.region
  parameters        = {
    outputDirectory = "gs://your-bucket/your-path"
    # inputTopic = "projects/your-project-id/topics/your-topic-name"
    # inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    # userTempLocation = "<userTempLocation>"
    # outputFilenamePrefix = "output-"
    # outputFilenameSuffix = ".txt"
    # outputShardTemplate = "W-P-SS-of-NN"
    # numShards = "0"
    # windowDuration = "5m"
    # yearPattern = "YYYY"
    # monthPattern = "MM"
    # dayPattern = "dd"
    # hourPattern = "HH"
    # minutePattern = "mm"
  }
}
```
