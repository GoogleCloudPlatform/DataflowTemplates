
BigQuery Anomaly Detection template
---
[Experimental] Real-time anomaly detection on BigQuery change data (CDC). Reads
streaming APPENDS/CHANGES data from a BigQuery table, computes a configurable
windowed metric, runs anomaly detection (ZScore, IQR, or RobustZScore), and emits
anomalies to Pub/Sub and/or a REST webhook. Alerts to Pub/Sub and the REST
webhook are rate-limited by default: per anomaly key, after the first alert fires
further anomalies are suppressed until a 10-minute gap between consecutive
anomalies elapses. Tune or disable via alert_cooldown_seconds (set to 0 to
disable). The BigQuery sink table is unaffected and records every anomaly.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **table**: BigQuery table to monitor. Format: project:dataset.table.
* **metric_spec**: JSON string defining the metric computation. Example: {"aggregation":{"window":{"type":"fixed","size_seconds":3600},"measures":[{"field":"amount","agg":"SUM","alias":"total"}]}}.
* **detector_spec**: JSON string defining the anomaly detector. Statistical: {"type":"ZScore"}, {"type":"IQR"}, {"type":"RobustZScore"}. Threshold: {"type":"Threshold","expression":"value >= 100"}. RelativeChange: {"type":"RelativeChange","direction":"decrease","threshold_pct":20,"lookback_windows":1}.

### Optional parameters

* **topic**: Pub/Sub topic for anomaly results. Full path: projects/<project>/topics/<topic>. Optional: at least one of topic or webhook_spec must be set.
* **poll_interval_sec**: Seconds between BigQuery CDC polls. Default: 60.
* **change_function**: BigQuery change function: APPENDS or CHANGES. Default: APPENDS.
* **buffer_sec**: Safety buffer behind now() in seconds. Default: 15.
* **start_offset_sec**: Start reading from this many seconds ago. Default: 60.
* **duration_sec**: How long to run in seconds. 0 means run forever. Default: 0.
* **temp_dataset**: BigQuery dataset for temp tables. If unset, auto-created.
* **log_all_results**: Log all anomaly detection results (normal, outlier, warmup) at WARNING level. Default: false.
* **sink_table**: BigQuery table to write all anomaly detection results to. Format: project:dataset.table. If unset, results are not written to BigQuery.
* **fanout_strategy**: Parallelism strategy for metric aggregation: sharded, hotkey_fanout, precombine, or none. Default: sharded.
* **fanout**: Number of shards for sharded or hotkey_fanout strategies. Ignored for none and precombine. Default: 400.
* **message_format**: Python format string for Pub/Sub anomaly messages. Available fields: {value}, {score}, {label}, {threshold}, {model_id}, {info}, {key}, {window_start}, {window_end}, plus any keys from message_metadata. If unset, a default JSON payload is used.
* **message_metadata**: JSON object of static key-value pairs available as additional fields in message_format. Example: {"job_id": "pipeline-123", "env": "prod"}. Anomaly fields take precedence on key collision.
* **webhook_spec**: JSON object configuring a REST webhook for anomaly results. Required keys: endpoint (http/https URL), body (JSON object/array). Optional keys: method (POST/PUT/PATCH, default POST), headers (object), scopes (list of OAuth scopes; default cloud-platform), timeout_seconds (default 600, i.e. 10 min), parallelism (max concurrent in-flight POSTs per worker, default 5), callback_frequency_seconds (how often the AsyncWrapper sweeps finished futures, default 30). String leaves in body and headers are Python-format-substituted against anomaly fields, message_metadata keys, and the {anomaly_message} field (which equals message_format output, or a default natural-language summary). At least one of topic or webhook_spec must be set.
* **alert_cooldown_seconds**: Session-window gap for debouncing alerts to external systems (Pub/Sub, webhook). Per anomaly key, the first anomaly fires immediately; subsequent anomalies are suppressed (logged as "still active") until a gap of at least this many seconds passes between consecutive anomalies. Continuous anomalies extend the active-alert window. The BigQuery sink table is unaffected and records every anomaly. Set to 0 to disable rate limiting. Default: 600 (10 minutes).



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=python/src/main/java/com/google/cloud/teleport/templates/python/BigQueryAnomalyDetection.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl python
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
-DtemplateName="BigQuery_Anomaly_Detection" \
-f python
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/BigQuery_Anomaly_Detection
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/BigQuery_Anomaly_Detection"

### Required
export TABLE=<table>
export METRIC_SPEC=<metric_spec>
export DETECTOR_SPEC=<detector_spec>

### Optional
export TOPIC=<topic>
export POLL_INTERVAL_SEC=<poll_interval_sec>
export CHANGE_FUNCTION=<change_function>
export BUFFER_SEC=<buffer_sec>
export START_OFFSET_SEC=<start_offset_sec>
export DURATION_SEC=<duration_sec>
export TEMP_DATASET=<temp_dataset>
export LOG_ALL_RESULTS=<log_all_results>
export SINK_TABLE=<sink_table>
export FANOUT_STRATEGY=<fanout_strategy>
export FANOUT=<fanout>
export MESSAGE_FORMAT=<message_format>
export MESSAGE_METADATA=<message_metadata>
export WEBHOOK_SPEC=<webhook_spec>
export ALERT_COOLDOWN_SECONDS=<alert_cooldown_seconds>

gcloud dataflow flex-template run "bigquery-anomaly-detection-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "table=$TABLE" \
  --parameters "metric_spec=$METRIC_SPEC" \
  --parameters "detector_spec=$DETECTOR_SPEC" \
  --parameters "topic=$TOPIC" \
  --parameters "poll_interval_sec=$POLL_INTERVAL_SEC" \
  --parameters "change_function=$CHANGE_FUNCTION" \
  --parameters "buffer_sec=$BUFFER_SEC" \
  --parameters "start_offset_sec=$START_OFFSET_SEC" \
  --parameters "duration_sec=$DURATION_SEC" \
  --parameters "temp_dataset=$TEMP_DATASET" \
  --parameters "log_all_results=$LOG_ALL_RESULTS" \
  --parameters "sink_table=$SINK_TABLE" \
  --parameters "fanout_strategy=$FANOUT_STRATEGY" \
  --parameters "fanout=$FANOUT" \
  --parameters "message_format=$MESSAGE_FORMAT" \
  --parameters "message_metadata=$MESSAGE_METADATA" \
  --parameters "webhook_spec=$WEBHOOK_SPEC" \
  --parameters "alert_cooldown_seconds=$ALERT_COOLDOWN_SECONDS"
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
export TABLE=<table>
export METRIC_SPEC=<metric_spec>
export DETECTOR_SPEC=<detector_spec>

### Optional
export TOPIC=<topic>
export POLL_INTERVAL_SEC=<poll_interval_sec>
export CHANGE_FUNCTION=<change_function>
export BUFFER_SEC=<buffer_sec>
export START_OFFSET_SEC=<start_offset_sec>
export DURATION_SEC=<duration_sec>
export TEMP_DATASET=<temp_dataset>
export LOG_ALL_RESULTS=<log_all_results>
export SINK_TABLE=<sink_table>
export FANOUT_STRATEGY=<fanout_strategy>
export FANOUT=<fanout>
export MESSAGE_FORMAT=<message_format>
export MESSAGE_METADATA=<message_metadata>
export WEBHOOK_SPEC=<webhook_spec>
export ALERT_COOLDOWN_SECONDS=<alert_cooldown_seconds>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigquery-anomaly-detection-job" \
-DtemplateName="BigQuery_Anomaly_Detection" \
-Dparameters="table=$TABLE,metric_spec=$METRIC_SPEC,detector_spec=$DETECTOR_SPEC,topic=$TOPIC,poll_interval_sec=$POLL_INTERVAL_SEC,change_function=$CHANGE_FUNCTION,buffer_sec=$BUFFER_SEC,start_offset_sec=$START_OFFSET_SEC,duration_sec=$DURATION_SEC,temp_dataset=$TEMP_DATASET,log_all_results=$LOG_ALL_RESULTS,sink_table=$SINK_TABLE,fanout_strategy=$FANOUT_STRATEGY,fanout=$FANOUT,message_format=$MESSAGE_FORMAT,message_metadata=$MESSAGE_METADATA,webhook_spec=$WEBHOOK_SPEC,alert_cooldown_seconds=$ALERT_COOLDOWN_SECONDS" \
-f python
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
cd v2/python/terraform/BigQuery_Anomaly_Detection
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

resource "google_dataflow_flex_template_job" "bigquery_anomaly_detection" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/BigQuery_Anomaly_Detection"
  name              = "bigquery-anomaly-detection"
  region            = var.region
  parameters        = {
    table = "<table>"
    metric_spec = "<metric_spec>"
    detector_spec = "<detector_spec>"
    # topic = "<topic>"
    # poll_interval_sec = "<poll_interval_sec>"
    # change_function = "<change_function>"
    # buffer_sec = "<buffer_sec>"
    # start_offset_sec = "<start_offset_sec>"
    # duration_sec = "<duration_sec>"
    # temp_dataset = "<temp_dataset>"
    # log_all_results = "<log_all_results>"
    # sink_table = "<sink_table>"
    # fanout_strategy = "<fanout_strategy>"
    # fanout = "<fanout>"
    # message_format = "<message_format>"
    # message_metadata = "<message_metadata>"
    # webhook_spec = "<webhook_spec>"
    # alert_cooldown_seconds = "<alert_cooldown_seconds>"
  }
}
```
