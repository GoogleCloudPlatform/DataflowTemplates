# Google Cloud Dataflow Template Pipelines

These Dataflow templates are an effort to solve simple, but large, in-Cloud data
tasks, including data import/export/backup/restore and bulk API operations,
without a development environment. The technology under the hood which makes
these operations possible is the
[Google Cloud Dataflow](https://cloud.google.com/dataflow/) service combined
with a set of [Apache Beam](https://beam.apache.org/) SDK templated pipelines.

Google is providing this collection of pre-implemented Dataflow templates as a
reference and to provide easy customization for developers wanting to extend
their functionality.

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git)

## Template Pipelines

* [BigQuery to Datastore](src/main/java/com/google/cloud/teleport/templates/BigQueryToDatastore.java)
* [Bigtable to GCS Avro](src/main/java/com/google/cloud/teleport/bigtable/BigtableToAvro.java)
* [Bulk Compressor](src/main/java/com/google/cloud/teleport/templates/BulkCompressor.java)
* [Bulk Decompressor](src/main/java/com/google/cloud/teleport/templates/BulkDecompressor.java)
* [Datastore Bulk Delete](src/main/java/com/google/cloud/teleport/templates/DatastoreToDatastoreDelete.java) *
* [Datastore to BigQuery](src/main/java/com/google/cloud/teleport/templates/DatastoreToBigQuery.java)
* [Datastore to GCS Text](src/main/java/com/google/cloud/teleport/templates/DatastoreToText.java) *
* [Datastore to Pub/Sub](src/main/java/com/google/cloud/teleport/templates/DatastoreToPubsub.java) *
* [Datastore Unique Schema Count](src/main/java/com/google/cloud/teleport/templates/DatastoreSchemasCountToText.java)
* [GCS Avro to Bigtable](src/main/java/com/google/cloud/teleport/bigtable/AvroToBigtable.java)
* [GCS Avro to Spanner](src/main/java/com/google/cloud/teleport/spanner/ImportPipeline.java)
* [GCS Text to Spanner](src/main/java/com/google/cloud/teleport/spanner/TextImportPipeline.java)
* [GCS Text to BigQuery](src/main/java/com/google/cloud/teleport/templates/TextIOToBigQuery.java) *
* [GCS Text to Datastore](src/main/java/com/google/cloud/teleport/templates/TextToDatastore.java)
* [GCS Text to Pub/Sub (Batch)](src/main/java/com/google/cloud/teleport/templates/TextToPubsub.java)
* [GCS Text to Pub/Sub (Streaming)](src/main/java/com/google/cloud/teleport/templates/TextToPubsubStream.java)
* [Jdbc to BigQuery](src/main/java/com/google/cloud/teleport/templates/JdbcToBigQuery.java)
* [Pub/Sub to BigQuery](src/main/java/com/google/cloud/teleport/templates/PubSubToBigQuery.java) *
* [Pub/Sub to Datastore](src/main/java/com/google/cloud/teleport/templates/PubsubToDatastore.java) *
* [Pub/Sub to GCS Avro](src/main/java/com/google/cloud/teleport/templates/PubsubToAvro.java)
* [Pub/Sub to GCS Text](src/main/java/com/google/cloud/teleport/templates/PubsubToText.java)
* [Pub/Sub to Pub/Sub](src/main/java/com/google/cloud/teleport/templates/PubsubToPubsub.java)
* [Pub/Sub to Splunk](src/main/java/com/google/cloud/teleport/templates/PubSubToSplunk.java)
* [Spanner to GCS Avro](src/main/java/com/google/cloud/teleport/spanner/ExportPipeline.java)
* [Spanner to GCS Text](src/main/java/com/google/cloud/teleport/templates/SpannerToText.java)
* [Word Count](src/main/java/com/google/cloud/teleport/templates/WordCount.java)


\* Supports user-defined functions (UDFs).

For documentation on each template's usage and parameters, please see
the official [docs](https://cloud.google.com/dataflow/docs/templates/provided-templates).

## Getting Started

### Requirements

* Java 8
* Maven 3

### Building the Project

Build the entire project using the maven compile command.

```sh
mvn clean compile
```

### Creating a Template File

Dataflow templates can be [created](https://cloud.google.com/dataflow/docs/templates/creating-templates#creating-and-staging-templates)
using a maven command which builds the project and stages the template
file on Google Cloud Storage. Any parameters passed at template build
time will not be able to be overwritten at execution time.

```sh
mvn compile exec:java \
-Dexec.mainClass=com.google.cloud.teleport.templates.<template-class> \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=<project-id> \
--stagingLocation=gs://<bucket-name>/staging \
--tempLocation=gs://<bucket-name>/temp \
--templateLocation=gs://<bucket-name>/templates/<template-name>.json \
--runner=DataflowRunner"
```


### Executing a Template File

Once the template is staged on Google Cloud Storage, it can then be
executed using the
[gcloud CLI](https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run)
tool. The runtime parameters required by the template can be passed in the
parameters field via comma-separated list of `paramName=Value`.

```sh
gcloud dataflow jobs run <job-name> \
--gcs-location=<template-location> \
--zone=<zone> \
--parameters <parameters>
```


## Using UDFs

User-defined functions (UDFs) allow you to customize a template's
functionality by providing a short JavaScript function without having to
maintain the entire codebase. This is useful in situations which you'd
like to rename fields, filter values, or even transform data formats
before output to the destination. All UDFs are executed by providing the
payload of the element as a string to the JavaScript function. You can
then use JavaScript's in-built JSON parser or other system functions to
transform the data prior to the pipeline's output. The return statement
of a UDF specifies the payload to pass forward in the pipeline. This
should always return a string value. If no value is returned or the
function returns undefined, the incoming record will be filtered from
the output.

### UDF Function Specification
| Template              | UDF Input Type | Input Description                               | UDF Output Type | Output Description                                                            |
|-----------------------|----------------|-------------------------------------------------|-----------------|-------------------------------------------------------------------------------|
| Datastore Bulk Delete | String         | A JSON string of the entity                     | String          | A JSON string of the entity to delete; filter entities by returning undefined |
| Datastore to Pub/Sub  | String         | A JSON string of the entity                     | String          | The payload to publish to Pub/Sub                                             |
| Datastore to GCS Text | String         | A JSON string of the entity                     | String          | A single-line within the output file                                          |
| GCS Text to BigQuery  | String         | A single-line within the input file             | String          | A JSON string which matches the destination table's schema                    |
| Pub/Sub to BigQuery   | String         | A string representation of the incoming payload | String          | A JSON string which matches the destination table's schema                    |
| Pub/Sub to Datastore  | String         | A string representation of the incoming payload | String          | A JSON string of the entity to write to Datastore                             |


### UDF Examples

#### Adding fields
```js
/**
 * A transform which adds a field to the incoming data.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  obj.dataFeed = "Real-time Transactions";
  obj.dataSource = "POS";
  return JSON.stringify(obj);
}
```

#### Filtering records
```js
/**
 * A transform function which only accepts 42 as the answer to life.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  // only output objects which have an answer to life of 42.
  if (obj.hasOwnProperty('answerToLife') && obj.answerToLife === 42) {
    return JSON.stringify(obj);
  }
}
```
