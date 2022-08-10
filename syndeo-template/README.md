# Syndeo Template

The code in this directory defines a 'meta-template' (i.e. a template that can be parameterized with sources, sinks,
and intermediate transforms).

The prototype comes with a few utilties to bootstrap a simple workflow, though these are meant only for testing of the
template capabilities.

**This is a prototype and may change significantly**. It is not supported for direct use.

## How does it work?

The template works by relying on Beam transforms that implement the `SchemaTransform` interface and exist in the
template's classpath.

The template receives a *pipeline spec*, which is a specification defining the pipeline's source, sink, and intermediate
transforms. Then, the template will take the pipeline spec, and

## Common workflow tasks

Generate SchemaIO configs: This command will traverse the classpath for the template, find all `SchemaTransform`
subclasses, and generate a file with the protocol buffer configuration for all of these subclasses.

```shell
mvn compile exec:java -Dexec.mainClass="com.google.cloud.syndeo.ConfigGen"
```

Write out the pipeline spec for testing: This command will generate a configuration proto for a pipeline that reads
from a Pubsub topic, and writes avro files into a GCS bucket.

```shell
mvn compile exec:java -Dexec.mainClass="com.google.cloud.syndeo.WritePipelineSpecForTesting"
```

Generate the template:
```shell
# Generate the JAR file under target/syndeo-template-1.0-SNAPSHOT.jar
mvn package -DskipTests
```

Run the template:
```shell
# Running locally
mvn exec:java -Dexec.mainClass="com.google.cloud.syndeo.SyndeoTemplate" \
  -Dexec.args="--runner=DataflowRunner --project=$GCP_PROJECT --region=$REGION \
               --gcpTempLocation=$GCS_BUCKET --pipelineSpec=$PATH_TO_SPEC --tempLocation=$GCS_BUCKET/temp"

# Running a pushed template (see Push template below)
Run Template on Dataflow from Uploaded Template:
gcloud dataflow flex-template run "syndeojob-`date +%Y%m%d-%H%M%S`" \
  --template-file-gcs-location "gs://$GCS_BUCKET/template.json" --region $REGION \
  --temp-location "gs://$GCS_BUCKET/temp" --parameters=pipelineSpec="gs://$GCS_BUCKET/pubsub_to_avro_config.txt"
```

Push template to GCP
```shell
# Push Template to GCP:
gcloud dataflow flex-template build gs://$GCS_BUCKET/template.json \
    --sdk-language "JAVA"  --flex-template-base-image JAVA11 \
    --image-gcr-path='gcr.io/$GCP_PROJECT/template:latest' \
    --jar "target/syndeo-template-1.0-SNAPSHOT.jar" \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.syndeo.SyndeoTemplate"
```
