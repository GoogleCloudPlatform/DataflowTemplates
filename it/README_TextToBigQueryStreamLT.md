# How to run TextToBigQueryStreamLT

[TextToBigQueryStreamLT](../v1/src/test/java/com/google/cloud/teleport/templates/TextToBigQueryStreamLT.java)
is a load test for the [TextToBigQueryStream](../v1/src/main/java/com/google/cloud/teleport/templates/TextToBigQueryStreaming.java) template.

## Prerequisites
* Java 11
* Maven
* gcloud CLI, and execution of the following commands:
  * gcloud auth login
  * gcloud auth application-default login
  * GCP project
  * GCP bucket
  * BigQuery service should be enabled in GCP

## Building template (Optional)

Template is already staged and its `specPath` will be used by default in the test.
But you can stage the template by yourself using these two steps:

```
mvn clean install -pl plugins/templates-maven-plugin -am
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
```

```
mvn clean package -PtemplatesStage  \
    -DskipTests \
    -DprojectId="$PROJECT" \
    -DbucketName="$BUCKET_NAME" \
    -DstagePrefix="templates" \
    -DtemplateName="Stream_GCS_Text_to_BigQuery" \
    -pl v1 \
    -am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write temp files to during serialization.
The path used will be `gs://<temp-bucket-name>/temp/`.

After running these steps you will get the message:
``Flex Template was staged! gs://<bucket-name>/templates/flex/Stream_GCS_Text_to_BigQuery``

Save your `specPath` to use later.

## Run test using Maven

You can run all tests using this Maven command:

```
mvn clean verify \
    -PtemplatesIntegrationTests \
    -Dproject="{project}" \
    -DartifactBucket="{bucketName}" \
    -Dregion=us-central1 \
    -Dtest=TextToBigQueryStreamLT
    -pl v1 -am
```

## Run test from Idea

You will need to create Idea Configuration:
* Set Java 11
* Set parameters:
  * -ea
  * -Dregion="us-central1"
  * -Dproject="apache-beam-testing"
  * -DartifactBucket="dataflow-templates-test-bucket"
  * -DjobName="stream-gcs-text-to-big-query-job"
  * -DtemplateName="Stream_GCS_Text_to_BigQuery"
  * -DspecPath=”your-path” (OPTIONAL)
* Set classpath: `classic-templates`
* Set test class `com.google.cloud.teleport.templates.TextToBigQueryStreamLT`
