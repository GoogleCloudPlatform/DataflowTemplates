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

## Note on Default Branch

As of November 18, 2021, our default branch is now named "main". This does not
affect forks. If you would like your fork and its local clone to reflect these
changes you can
follow [GitHub's branch renaming guide](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/renaming-a-branch)
.

## Building

Maven commands should be run on the parent POM. An example would be:

```
mvn clean package -pl v2/pubsub-binary-to-bigquery -am
```

## Template Pipelines

* [BigQuery to Bigtable](v2/bigquery-to-bigtable/src/main/java/com/google/cloud/teleport/v2/templates/BigQueryToBigtable.java)
* [BigQuery to Datastore](v1/src/main/java/com/google/cloud/teleport/templates/BigQueryToDatastore.java)
* [BigQuery to TFRecords](v1/src/main/java/com/google/cloud/teleport/templates/BigQueryToTFRecord.java)
* [Bigtable to GCS Avro](v1/src/main/java/com/google/cloud/teleport/bigtable/BigtableToAvro.java)
* [Bulk Compressor](v1/src/main/java/com/google/cloud/teleport/templates/BulkCompressor.java)
* [Bulk Decompressor](v1/src/main/java/com/google/cloud/teleport/templates/BulkDecompressor.java)
* [Datastore Bulk Delete](v1/src/main/java/com/google/cloud/teleport/templates/DatastoreToDatastoreDelete.java) *
* [Datastore to BigQuery](v1/src/main/java/com/google/cloud/teleport/templates/DatastoreToBigQuery.java)
* [Datastore to GCS Text](v1/src/main/java/com/google/cloud/teleport/templates/DatastoreToText.java) *
* [Datastore to Pub/Sub](v1/src/main/java/com/google/cloud/teleport/templates/DatastoreToPubsub.java) *
* [Datastore Unique Schema Count](v1/src/main/java/com/google/cloud/teleport/templates/DatastoreSchemasCountToText.java)
* [DLP Text to BigQuery (Streaming)](v1/src/main/java/com/google/cloud/teleport/templates/DLPTextToBigQueryStreaming.java)
* [File Format Conversion](v2/file-format-conversion/src/main/java/com/google/cloud/teleport/v2/templates/FileFormatConversion.java)
* [GCS Avro to Bigtable](v1/src/main/java/com/google/cloud/teleport/bigtable/AvroToBigtable.java)
* [GCS Avro to Spanner](v1/src/main/java/com/google/cloud/teleport/spanner/ImportPipeline.java)
* [GCS Text to Spanner](v1/src/main/java/com/google/cloud/teleport/spanner/TextImportPipeline.java)
* [GCS Text to BigQuery](v1/src/main/java/com/google/cloud/teleport/templates/TextIOToBigQuery.java) *
* [GCS Text to Datastore](v1/src/main/java/com/google/cloud/teleport/templates/TextToDatastore.java)
* [GCS Text to Pub/Sub (Batch)](v1/src/main/java/com/google/cloud/teleport/templates/TextToPubsub.java)
* [GCS Text to Pub/Sub (Streaming)](v1/src/main/java/com/google/cloud/teleport/templates/TextToPubsubStream.java)
* [Jdbc to BigQuery](v1/src/main/java/com/google/cloud/teleport/templates/JdbcToBigQuery.java)
* [Pub/Sub to BigQuery](v1/src/main/java/com/google/cloud/teleport/templates/PubSubToBigQuery.java) *
* [Pub/Sub to Datastore](v1/src/main/java/com/google/cloud/teleport/templates/PubsubToDatastore.java) *
* [Pub/Sub to GCS Avro](v1/src/main/java/com/google/cloud/teleport/templates/PubsubToAvro.java)
* [Pub/Sub to GCS Text](v1/src/main/java/com/google/cloud/teleport/templates/PubsubToText.java)
* [Pub/Sub to Pub/Sub](v1/src/main/java/com/google/cloud/teleport/templates/PubsubToPubsub.java)
* [Pub/Sub to Splunk](v1/src/main/java/com/google/cloud/teleport/templates/PubSubToSplunk.java) *
* [Spanner to GCS Avro](v1/src/main/java/com/google/cloud/teleport/spanner/ExportPipeline.java)
* [Spanner to GCS Text](v1/src/main/java/com/google/cloud/teleport/templates/SpannerToText.java)
* [Word Count](v1/src/main/java/com/google/cloud/teleport/templates/WordCount.java)

\* Supports user-defined functions (UDFs).

For documentation on each template's usage and parameters, please see the
official [docs](https://cloud.google.com/dataflow/docs/templates/provided-templates)
.

## Getting Started

### Requirements

* Java 11
* Maven 3

### Building the Project

Build the entire project using the maven compile command.

```sh
mvn clean compile
```

### Building/Testing from IntelliJ

IntelliJ, by default, will often skip necessary Maven goals, leading to build
failures. You can fix these in the Maven view by going to
**Module_Name > Plugins > Plugin_Name** where Module_Name and Plugin_Name are
the names of the respective module and plugin with the rule. From there,
right-click the rule and select "Execute Before Build".

The list of known rules that require this are:

* common > Plugins > protobuf > protobuf:compile
* common > Plugins > protobuf > protobuf:test-compile

### Formatting Code

From either the root directory or v2/ directory, run:

```sh
mvn spotless:apply
```

This will format the code and add a license header. To verify that the code is
formatted correctly, run:

```sh
mvn spotless:check
```

The directory to run the commands from is based on whether the changes are under
v2/ or not.

### Creating a Template File

Dataflow templates can
be [created](https://cloud.google.com/dataflow/docs/templates/creating-templates#creating-and-staging-templates)
using a Maven command which builds the project and stages the template file on
Google Cloud Storage. Any parameters passed at template build time will not be
able to be overwritten at execution time.

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

Once the template is staged on Google Cloud Storage, it can then be executed
using the
[gcloud CLI](https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run)
tool. The runtime parameters required by the template can be passed in the
parameters field via comma-separated list of `paramName=Value`.

```sh
gcloud dataflow jobs run <job-name> \
--gcs-location=<template-location> \
--zone=<zone> \
--parameters <parameters>
```

## Metadata Annotations

A template requires more information than just a name and description. For
example, in order to be used from the Dataflow UI, parameters need a longer help
text to guide users, as well as proper types and validations to make sure
parameters are being passed correctly.

We introduced annotations to have the source code as a single source of truth,
along with a set of utilities / plugins to generate template-accompanying
artifacts (such as command specs, parameter specs).

### @Template Annotation

Every template must be annotated with `@Template`. Existing templates can be
used for reference, but the structure is as follows:

```java

@Template(
    name = "BigQuery_to_Elasticsearch",
    category = TemplateCategory.BATCH,
    displayName = "BigQuery to Elasticsearch",
    description = "A pipeline which sends BigQuery records into an Elasticsearch instance as JSON documents.",
    optionsClass = BigQueryToElasticsearchOptions.class,
    flexContainerName = "bigquery-to-elasticsearch")
public class BigQueryToElasticsearch {
```

### @TemplateParameter Annotation

A set of `@TemplateParameter.{Type}` annotations were created to allow the
definition of options for a template, and the proper rendering in the UI, and
validations by the template launch service. Examples can be found in the
repository, but the general structure is as follows:

```java
@TemplateParameter.Text(
    order = 2,
    optional = false,
    regexes = {"[,a-zA-Z0-9._-]+"},
    description = "Kafka topic(s) to read the input from",
    helpText = "Kafka topic(s) to read the input from.",
    example = "topic1,topic2")
@Validation.Required
String getInputTopics();
```

```java
@TemplateParameter.GcsReadFile(
    order = 1,
    description = "Cloud Storage Input File(s)",
    helpText = "Path of the file pattern glob to read from.",
    example = "gs://your-bucket/path/*.csv")
String getInputFilePattern();
```

```java
@TemplateParameter.Boolean(
    order = 11,
    optional = true,
    description = "Whether to use column alias to map the rows.",
    helpText = "If enabled (set to true) the pipeline will consider column alias (\"AS\") instead of the column name to map the rows to BigQuery.")
@Default.Boolean(false)
Boolean getUseColumnAlias();
```

```java
@TemplateParameter.Enum(
    order = 21,
    enumOptions = {"INDEX", "CREATE"},
    optional = true,
    description = "Build insert method",
    helpText = "Whether to use INDEX (index, allows upsert) or CREATE (create, errors on duplicate _id) with Elasticsearch bulk requests.")
@Default.Enum("CREATE")
BulkInsertMethodOptions getBulkInsertMethod();
```

Note: `order` is relevant for templates that can be used from the UI, and
specify the relative order of parameters.

### @TemplateIntegrationTest Annotation

This annotation should be used by classes that are used for integration tests of
other templates. This is used to wire a specific `IT` class with a template, and
allows environment preparation / proper template staging before tests are
executed on Dataflow.

Template tests have to follow this general format (please note
the `@TemplateIntegrationTest` annotation and the `TemplateTestBase`
super-class):

```java

@TemplateIntegrationTest(PubsubToText.class)
@RunWith(JUnit4.class)
public final class PubsubToTextIT extends TemplateTestBase {
```

Please refer to `Templates Plugin (Beta)` to use and validate such annotations.

## Templates Plugin (Beta)

Templates plugin was created to make the workflow of creating, testing and
releasing Templates much simpler.

Before using the plugin, please make sure that
the [gcloud CLI](https://cloud.google.com/sdk/docs/install) is installed and
up-to-date, and that the client is properly authenticated using:

```shell
gcloud init
gcloud auth application-default login
```

After authenticated, install the plugin into your local repository:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Deploying/Staging Templates

To deploy a Template, it is necessary to upload the images to Artifact
Registry (for Flex templates) and copy the template to Cloud Storage.

Although there are different steps that depend on the kind of template being
developed. The plugin allows a template to be staged using the following single
command:

```shell
mvn clean package -PtemplatesStage  \
  -DskipTests \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DstagePrefix="images/$(date +%Y_%m_%d)_v1" \
  -DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
  -pl v2/googlecloud-to-googlecloud -am
```

**Note**: if `-DtemplateName` is not specified, all templates for the module
will be staged.

### Running a Template

A template can also be executed on Dataflow, directly from the command line. The
command-line is similar to staging a template, but it is required to
specify `-Dparameters` with the parameters that will be used when launching the
template. For example:

```shell
mvn clean package -PtemplatesRun \
  -DskipTests \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -Dregion="us-central1" \
  -DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
  -Dparameters="inputTopic=projects/{projectId}/topics/{topicName},windowDuration=15s,outputDirectory=gs://{outputDirectory}/out,outputFilenamePrefix=output-,outputFilenameSuffix=.txt" \
  -pl v2/googlecloud-to-googlecloud -am

```

**Notes**

- When running a template, `-DtemplateName` is mandatory, as `-Dparameters=` are
  different across templates.
- `-PtemplatesRun` is self-contained, i.e., it is not required to run **
  Deploying/Staging Templates** before. In case you want to run a previously
  staged template, the existing path can be provided
  as `-DspecPath=gs://.../path`
- `-DjobName="{name}"` may be informed if a specific name is desirable (
  optional).


### Running Integration Tests

To run integration tests, the developer plugin can be also used to stage template on-demand (in case the parameter `-DspecPath=` is not specified).

For example, to run all the integration tests in a specific module (in the example below, `v2/googlecloud-to-googlecloud`):

```shell
mvn clean verify \
  -PtemplatesIntegrationTests \
  -Dproject="{project}" \
  -DartifactBucket="{bucketName}" \
  -Dregion=us-central1 \
  -pl v2/googlecloud-to-googlecloud -am
```

The parameter `-Dtest=` can be given to test a single class (e.g., `-Dtest=PubsubToTextIT`) or single test case (e.g., `-Dtest=PubsubToTextIT#testTopicToGcs`).

The same happens when the test is executed from an IDE, just make sure to add the parameters `-Dproject=`, `-DartifactBucket=` and `-Dregion=` as program or VM arguments.

### Validating annotations

To simply validate the templates in the current context without building the
images or running, the profile `-PtemplatesValidate` can be activated on Maven
commands. For example, use the following commands:

Validate in all modules:

```shell
mvn clean compile -PtemplatesValidate
```

Validate in a specific module (for example, `v2/googlecloud-to-googlecloud)`:

```shell
mvn clean compile \
  -PtemplatesValidate \
  -pl v2/googlecloud-to-googlecloud -am
```

### Generating Specs

Specs are useful to visualize the resulting metadata after writing a template,
as those are the artifacts that are read by Dataflow when starting a job from a
template.

The spec files can be generated by using `-PtemplatesSpec`. For example:

```shell
mvn clean package \
  -PtemplatesSpec \
  -DskipTests \
  -pl v2/googlecloud-to-googlecloud -am
```

Specs are generated in the folder `target`, with the
name `{template}-spec-generated-metadata.json`. Please verify if all parameters
are listed correctly, following the desired order that is intended on the
templates UI.

### Releasing Templates

There's a specific command to release all Templates, which is a shortcut to
create specs and stage templates, with additional validations.

```shell
mvn clean package -PtemplatesRelease  \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DlibrariesBucketName="{bucketName}-libraries" \
  -DstagePrefix="$(date +%Y_%m_%d)-00_RC00"
```

## Using UDFs

User-defined functions (UDFs) allow you to customize a template's functionality
by providing a short JavaScript function without having to maintain the entire
codebase. This is useful in situations which you'd like to rename fields, filter
values, or even transform data formats before output to the destination. All
UDFs are executed by providing the payload of the element as a string to the
JavaScript function. You can then use JavaScript's in-built JSON parser or other
system functions to transform the data prior to the pipeline's output. The
return statement of a UDF specifies the payload to pass forward in the pipeline.
This should always return a string value. If no value is returned or the
function returns undefined, the incoming record will be filtered from the
output.

### UDF Function Specification

| Template              | UDF Input Type | Input Description                               | UDF Output Type | Output Description                                                            |
|-----------------------|----------------|-------------------------------------------------|-----------------|-------------------------------------------------------------------------------|
| Datastore Bulk Delete | String         | A JSON string of the entity                     | String          | A JSON string of the entity to delete; filter entities by returning undefined |
| Datastore to Pub/Sub  | String         | A JSON string of the entity                     | String          | The payload to publish to Pub/Sub                                             |
| Datastore to GCS Text | String         | A JSON string of the entity                     | String          | A single-line within the output file                                          |
| GCS Text to BigQuery  | String         | A single-line within the input file             | String          | A JSON string which matches the destination table's schema                    |
| Pub/Sub to BigQuery   | String         | A string representation of the incoming payload | String          | A JSON string which matches the destination table's schema                    |
| Pub/Sub to Datastore  | String         | A string representation of the incoming payload | String          | A JSON string of the entity to write to Datastore                             |
| Pub/Sub to Splunk  | String         | A string representation of the incoming payload | String          | The event data to be sent to Splunk HEC events endpoint. Must be a string or a stringified JSON object |

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
