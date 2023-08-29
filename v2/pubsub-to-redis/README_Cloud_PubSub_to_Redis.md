
Pub/Sub to Redis template
---
A streaming pipeline which inserts data from a Pub/Sub and writes them to Redis.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_Redis).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **redisHost** (Redis DB Host): Redis database host. (Example: your.cloud.db.redislabs.com). Defaults to: 127.0.0.1.
* **redisPort** (Redis DB Port): Redis database port. (Example: 12345). Defaults to: 6379.
* **redisPassword** (Redis DB Password): Redis database password. Defaults to empty.

### Optional Parameters

* **sslEnabled** (Redis ssl enabled): Redis database ssl parameter. Defaults to: false.
* **redisSinkType** (Redis sink to write): Supported Redis sinks are STRING_SINK, HASH_SINK, STREAMS_SINK and LOGGING_SINK (Example: STRING_SINK). Defaults to: STRING_SINK.
* **connectionTimeout** (Redis connection timeout in ms): Redis connection timeout in ms. (Example: 2000). Defaults to: 2000.
* **ttl** (Hash key expiration time in sec (ttl)): Key expiration time in sec (ttl, default for HASH_SINK is -1 i.e. no expiration).
* **javascriptTextTransformGcsPath** (Cloud Storage path to Javascript UDF source): The Cloud Storage path pattern for the JavaScript code containing your user-defined functions. (Example: gs://your-bucket/your-function.js).
* **javascriptTextTransformFunctionName** (UDF Javascript Function Name): The name of the function to call from your JavaScript file. Use only letters, digits, and underscores. (Example: 'transform' or 'transform_udf1').
* **javascriptTextTransformFunctionReload** (Enable JavaScript UDF auto-reload feature): If set to true, enables the JavaScript UDF auto-reload feature, which guarantees that updated code is used without the need to restart jobs.
* **javascriptTextTransformReloadIntervalMinutes** (JavaScript UDF auto-reload interval (minutes)): Define the interval that workers may check for JavaScript UDF changes to reload the files. Defaults to: 60.


## User-Defined functions (UDFs)

The Pub/Sub to Redis Template supports User-Defined functions (UDFs).
UDFs allow you to customize functionality by providing a JavaScript function
without having to maintain or build the entire template code.

Check [Create user-defined functions for Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/create-template-udf)
and [Using UDFs](https://github.com/GoogleCloudPlatform/DataflowTemplates#using-udfs)
for more information about how to create and test those functions.


## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/pubsub-to-redis/src/main/java/com/google/cloud/teleport/v2/templates/PubSubToRedis.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
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

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Cloud_PubSub_to_Redis" \
-pl v2/pubsub-to-redis \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_PubSub_to_Redis
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_PubSub_to_Redis"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_PASSWORD=""

### Optional
export SSL_ENABLED=false
export REDIS_SINK_TYPE=STRING_SINK
export CONNECTION_TIMEOUT=2000
export TTL=-1
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=60

gcloud dataflow flex-template run "cloud-pubsub-to-redis-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "redisHost=$REDIS_HOST" \
  --parameters "redisPort=$REDIS_PORT" \
  --parameters "redisPassword=$REDIS_PASSWORD" \
  --parameters "sslEnabled=$SSL_ENABLED" \
  --parameters "redisSinkType=$REDIS_SINK_TYPE" \
  --parameters "connectionTimeout=$CONNECTION_TIMEOUT" \
  --parameters "ttl=$TTL" \
  --parameters "javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME" \
  --parameters "javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES"
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
export INPUT_SUBSCRIPTION=<inputSubscription>
export REDIS_HOST=127.0.0.1
export REDIS_PORT=6379
export REDIS_PASSWORD=""

### Optional
export SSL_ENABLED=false
export REDIS_SINK_TYPE=STRING_SINK
export CONNECTION_TIMEOUT=2000
export TTL=-1
export JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH=<javascriptTextTransformGcsPath>
export JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME=<javascriptTextTransformFunctionName>
export JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES=60

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-redis-job" \
-DtemplateName="Cloud_PubSub_to_Redis" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,redisHost=$REDIS_HOST,redisPort=$REDIS_PORT,redisPassword=$REDIS_PASSWORD,sslEnabled=$SSL_ENABLED,redisSinkType=$REDIS_SINK_TYPE,connectionTimeout=$CONNECTION_TIMEOUT,ttl=$TTL,javascriptTextTransformGcsPath=$JAVASCRIPT_TEXT_TRANSFORM_GCS_PATH,javascriptTextTransformFunctionName=$JAVASCRIPT_TEXT_TRANSFORM_FUNCTION_NAME,javascriptTextTransformReloadIntervalMinutes=$JAVASCRIPT_TEXT_TRANSFORM_RELOAD_INTERVAL_MINUTES" \
-pl v2/pubsub-to-redis \
-am
```
