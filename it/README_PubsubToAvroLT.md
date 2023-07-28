# How to run PubsubToAvroLT

[PubsubToAvroLT](../v2/googlecloud-to-googlecloud/src/test/java/com/google/cloud/teleport/v2/templates/PubsubToAvroLT.java)
is a load test for the [PubsubToAvro](../v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/PubsubToAvro.java) template.

## Prerequisites
* Java 11
* Maven
* gcloud CLI, and execution of the following commands:
  * gcloud auth login
  * gcloud auth application-default login
  * GCP project
  * GCP bucket

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
    -DtemplateName="Cloud_PubSub_to_Avro_Flex" \
    -pl v2/googlecloud-to-googlecloud \
    -am
```

After running these steps you will get the message:
``
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_PubSub_to_Avro_Flex
``

Save your `specPath` to use later.

## Run test using Maven

You can run all tests using this Maven command:

```
mvn clean verify \
    -PtemplatesIntegrationTests \
    -Dproject="{project}" \
    -DartifactBucket="{bucketName}" \
    -Dregion=us-central1 \
    -Dtest=PubsubToAvroLT
    -pl v2/googlecloud-to-googlecloud -am
```

## Run test from Idea

You will need to create Idea Configuration:
* Set Java 11
* Set parameters:
  * -ea
  * -Dregion="us-central1"
  * -Dproject="apache-beam-testing"
  * -DartifactBucket="dataflow-templates-test-bucket"
  * -DjobName="cloud-pubsub-to-gcs-avro-job"
  * -DtemplateName="PubSub_to_GCS_Avro"
  * -DspecPath=”your-path” (OPTIONAL)
* Set classpath: `googlecloud-to-googlecloud`
* Set test class `com.google.cloud.teleport.v2.templates.PubsubToAvroLT`
