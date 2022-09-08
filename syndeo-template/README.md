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

### Format code

To apply spotless formatting rules to the Syndeo template code, run the following command:

```shell
mvn -B spotless:apply compile -f  unified-templates.xml -pl syndeo-template/pom.xml
```

### Run tests (unit and integration tests)

To **run unit tests** for the Syndeo template, run the following command. Note that this command knows to skip
integration tests and only runs unit tests:

```shell
mvn clean package test -f unified-templates.xml -pl syndeo-template/pom.xml
```

To set up your Google Cloud project for the integration tests, the following steps assume you have installed and setup [gcloud](https://cloud.google.com/sdk/gcloud).

1. Set your default project.

```shell
gcloud config set project PROJECT
```

#### BigQuery to BigTable tests

The file `BigTableWriteIT` **holds integration tests** for the basic BigTable Syndeo integration. These integration tests
rely on the existence of a BigTable instance and table, as well as a BigQuery dataset, which holds the BQ data that
is part of the pipeline's read.

This integration test requires a BigQuery dataset and a BigTable instance, as well as GCS buckets to handle artifacts.

1. Create the BigQuery dataset.

```
bq mk syndeo_dataset
```

2. Create the BigTable instance.

```
gcloud bigtable instances create teleport --display-name=teleport --cluster-config=id=teleport,zone=us-central1-a
```

3. Create the artifact bucket

```
ARTIFACT_BUCKET=[CHANGE ME]
gsutil mb gs://$ARTIFACT_BUCKET
```

4. Create the temporary location bucket

```
TEMP_LOCATION_BUCKET=[CHANGE ME]
gsutil mb gs://$TEMP_LOCATION_BUCKET
```


```shell
mvn clean package test -f unified-templates.xml -pl syndeo-template/pom.xml  \
    -Dtest="BigTableWriteIT#testBigQueryToBigTableSmallNonTemplateJob"  \
    -Dproject="$(gcloud config get-value project)"  -DartifactBucket="gs://$ARTIFACT_BUCKET"  \
    -Dregion="us-central1" -DtempLocation=gs://$TEMP_LOCATION_BUCKET
```

### Push template to GCP

The following command will take the already-built artifacts and push them to GCS and GCR, where they can be utilized to run a template.

```shell
# Push Template to GCP:
gcloud dataflow flex-template build gs://$GCS_BUCKET_NAME/syndeo-template.json  \
    --metadata-file syndeo-template/metadata.json   --sdk-language "JAVA"  \
    --flex-template-base-image JAVA11     --image-gcr-path=gcr.io/$GCP_PROJECT/syndeo-template:latest \
    --jar "syndeo-template/target/syndeo-template-1.0-SNAPSHOT.jar"  \
    --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.syndeo.SyndeoTemplate"
```


### Workflow tasks to run the template manually

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

Run the template:
```shell
# Running a pushed template (see Push template below)
Run Template on Dataflow from Uploaded Template:
gcloud dataflow flex-template run "syndeojob-`date +%Y%m%d-%H%M%S`" \
  --template-file-gcs-location "gs://$GCS_BUCKET/template.json" --region $REGION \
  --temp-location "gs://$GCS_BUCKET/temp" --parameters=pipelineSpec="gs://$GCS_BUCKET/pubsub_to_avro_config.txt"
```
