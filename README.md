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
follow [GitHub's branch renaming guide](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/renaming-a-branch).

## Building

Maven commands should be run on the parent POM. An example would be:

```
mvn clean package -pl v2/pubsub-binary-to-bigquery -am
```

## Template Pipelines

- Get Started
    - [Word Count](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Word_Count&type=code)
- Process Data Continuously (stream)
    - [Azure Eventhub to Pubsub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Azure_Eventhub_to_PubSub&type=code)
    - [Bigtable Change Streams to HBase Replicator](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Bigtable_Change_Streams_to_HBase&type=code)
    - [Cloud Bigtable change streams to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Bigtable_Change_Streams_to_BigQuery&type=code)
    - [Cloud Bigtable change streams to Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Bigtable_Change_Streams_to_Google_Cloud_Storage&type=code)
    - [Cloud Spanner change streams to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Spanner_Change_Streams_to_BigQuery&type=code)
    - [Cloud Spanner change streams to Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Spanner_Change_Streams_to_Google_Cloud_Storage&type=code)
    - [Cloud Spanner change streams to Pub/Sub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Spanner_Change_Streams_to_PubSub&type=code)
    - [Cloud Storage Text to BigQuery (Stream)](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Stream_GCS_Text_to_BigQuery_Flex&type=code)
    - [Data Masking/Tokenization from Cloud Storage to BigQuery (using Cloud DLP)](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Stream_DLP_GCS_Text_to_BigQuery_Flex&type=code)
    - [Datastream to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Datastream_to_BigQuery&type=code)
    - [Datastream to Cloud Spanner](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Datastream_to_Spanner&type=code)
    - [Datastream to SQL](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Datastream_to_SQL&type=code)
    - [JMS to Pubsub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Jms_to_PubSub&type=code)
    - [Kafka to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Kafka_to_BigQuery&type=code)
    - [Kafka to Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Kafka_to_GCS&type=code)
    - [Kinesis To Pubsub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Kinesis_To_Pubsub&type=code)
    - [MongoDB to BigQuery (CDC)](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20MongoDB_to_BigQuery_CDC&type=code)
    - [Mqtt to Pubsub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Mqtt_to_PubSub&type=code)
    - [Ordered change stream buffer to Source DB](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Ordered_Changestream_Buffer_to_Sourcedb&type=code)
    - [Pub/Sub Avro to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_Avro_to_BigQuery&type=code)
    - [Pub/Sub CDC to Bigquery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_CDC_to_BigQuery&type=code)
    - [Pub/Sub Proto to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_Proto_to_BigQuery&type=code)
    - [Pub/Sub Subscription or Topic to Text Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_GCS_Text_Flex&type=code)
    - [Pub/Sub Subscription to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_to_BigQuery_Flex&type=code)
    - [Pub/Sub Topic to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_to_BigQuery&type=code)
    - [Pub/Sub to Avro Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_Avro_Flex&type=code)
    - [Pub/Sub to Datadog](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_Datadog&type=code)
    - [Pub/Sub to Elasticsearch](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_to_Elasticsearch&type=code)
    - [Pub/Sub to JDBC](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Pubsub_to_Jdbc&type=code)
    - [Pub/Sub to Kafka](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PubSub_to_Kafka&type=code)
    - [Pub/Sub to MongoDB](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_MongoDB&type=code)
    - [Pub/Sub to Pub/Sub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_Cloud_PubSub&type=code)
    - [Pub/Sub to Redis](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_Redis&type=code)
    - [Pub/Sub to Splunk](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_Splunk&type=code)
    - [Pub/Sub to Text Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_PubSub_to_GCS_Text&type=code)
    - [Pubsub to JMS](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Pubsub_to_Jms&type=code)
    - [Spanner Change Streams to Sink](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Spanner_Change_Streams_to_Sink&type=code)
    - [Synchronizing CDC data to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cdc_To_BigQuery_Template&type=code)
    - [Text Files on Cloud Storage to Pub/Sub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Stream_GCS_Text_to_Cloud_PubSub&type=code)
- Process Data in Bulk (batch)
    - [AstraDB to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20AstraDB_To_BigQuery&type=code)
    - [Avro Files on Cloud Storage to Cloud Bigtable](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Avro_to_Cloud_Bigtable&type=code)
    - [Avro Files on Cloud Storage to Cloud Spanner](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Avro_to_Cloud_Spanner&type=code)
    - [BigQuery export to Parquet (via Storage API)](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20BigQuery_to_Parquet&type=code)
    - [BigQuery to Bigtable](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20BigQuery_to_Bigtable&type=code)
    - [BigQuery to Datastore](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_BigQuery_to_Cloud_Datastore&type=code)
    - [BigQuery to Elasticsearch](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20BigQuery_to_Elasticsearch&type=code)
    - [BigQuery to MongoDB](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20BigQuery_to_MongoDB&type=code)
    - [BigQuery to TensorFlow Records](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_BigQuery_to_GCS_TensorFlow_Records&type=code)
    - [Cassandra to Cloud Bigtable](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cassandra_To_Cloud_Bigtable&type=code)
    - [Cloud Bigtable to Avro Files in Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Bigtable_to_GCS_Avro&type=code)
    - [Cloud Bigtable to Parquet Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Bigtable_to_GCS_Parquet&type=code)
    - [Cloud Bigtable to SequenceFile Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Bigtable_to_GCS_SequenceFile&type=code)
    - [Cloud Spanner to Avro Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Cloud_Spanner_to_GCS_Avro&type=code)
    - [Cloud Spanner to Text Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Spanner_to_GCS_Text&type=code)
    - [Cloud Storage To Splunk](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_To_Splunk&type=code)
    - [Cloud Storage to Elasticsearch](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_to_Elasticsearch&type=code)
    - [Dataplex JDBC Ingestion](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Dataplex_JDBC_Ingestion&type=code)
    - [Dataplex: Convert Cloud Storage File Format](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Dataplex_File_Format_Conversion&type=code)
    - [Dataplex: Tier Data from BigQuery to Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Dataplex_BigQuery_to_GCS&type=code)
    - [Firestore (Datastore mode) to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Firestore_to_BigQuery_Flex&type=code)
    - [Firestore (Datastore mode) to Text Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Firestore_to_GCS_Text&type=code)
    - [Google Cloud to Neo4j](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Google_Cloud_to_Neo4j&type=code)
    - [JDBC to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Jdbc_to_BigQuery&type=code)
    - [JDBC to BigQuery with BigQuery Storage API support](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Jdbc_to_BigQuery_Flex&type=code)
    - [JDBC to Pub/Sub](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Jdbc_to_PubSub&type=code)
    - [MongoDB to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20MongoDB_to_BigQuery&type=code)
    - [MySQL to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20MySQL_to_BigQuery&type=code)
    - [Parquet Files on Cloud Storage to Cloud Bigtable](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Parquet_to_Cloud_Bigtable&type=code)
    - [PostgreSQL to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20PostgreSQL_to_BigQuery&type=code)
    - [SQLServer to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20SQLServer_to_BigQuery&type=code)
    - [SequenceFile Files on Cloud Storage to Cloud Bigtable](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_SequenceFile_to_Cloud_Bigtable&type=code)
    - [Text Files on Cloud Storage to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Text_to_BigQuery&type=code)
    - [Text Files on Cloud Storage to BigQuery with BigQuery Storage API support](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Text_to_BigQuery_Flex&type=code)
    - [Text Files on Cloud Storage to Cloud Spanner](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Text_to_Cloud_Spanner&type=code)
    - [Text Files on Cloud Storage to Firestore (Datastore mode)](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Text_to_Firestore&type=code)
- Utilities
    - [Bulk Compress Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Bulk_Compress_GCS_Files&type=code)
    - [Bulk Decompress Files on Cloud Storage](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Bulk_Decompress_GCS_Files&type=code)
    - [Bulk Delete Entities in Firestore (Datastore mode)](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Firestore_to_Firestore_Delete&type=code)
    - [Convert file formats between Avro, Parquet & CSV](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20File_Format_Conversion&type=code)
    - [Streaming Data Generator](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Streaming_Data_Generator&type=code)
- Legacy Templates
    - [Bulk Delete Entities in Datastore [Deprecated]](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Datastore_to_Datastore_Delete&type=code)
    - [Datastore to Text Files on Cloud Storage [Deprecated]](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Datastore_to_GCS_Text&type=code)
    - [Text Files on Cloud Storage to Datastore [Deprecated]](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20GCS_Text_to_Datastore&type=code)

For documentation on each template's usage and parameters, please see the
official [docs](https://cloud.google.com/dataflow/docs/templates/provided-templates).

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

### Executing a Template File

Once the template is staged on Google Cloud Storage, it can then be executed
using the
gcloud CLI tool. Please check [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates#using-gcloud)
or [Using Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates#run-a-flex-template-pipeline)
for more information.

## Developing/Contributing Templates

### Templates Plugin

Templates plugin was created to make the workflow of creating, testing and
releasing Templates easier.

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

### Staging (Deploying) Templates

To stage a Template, it is necessary to upload the images to Artifact
Registry (for Flex templates) and copy the template to Cloud Storage.

Although there are different steps that depend on the kind of template being
developed. The plugin allows a template to be staged using the following single
command:

```shell
mvn clean package -PtemplatesStage  \
  -DskipTests \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DstagePrefix="images/$(date +%Y_%m_%d)_01" \
  -DtemplateName="Cloud_PubSub_to_GCS_Text_Flex" \
  -pl v2/googlecloud-to-googlecloud -am
```

Notes:
- Change `-pl v2/googlecloud-to-googlecloud` and `-DtemplateName` to point to the specific Maven module where your template is located. Even though `-pl` is not required, it allows the command to run considerably faster.
- In case `-DtemplateName` is not specified, all templates for the module will be staged.

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

Notes:
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

## Metadata Annotations

A template requires more information than just a name and description. For
example, in order to be used from the Dataflow UI, parameters need a longer help
text to guide users, as well as proper types and validations to make sure
parameters are being passed correctly.

We introduced annotations to have the source code as a single source of truth,
along with a set of utilities / plugins to generate template-accompanying
artifacts (such as command specs, parameter specs).

#### @Template Annotation

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

#### @TemplateParameter Annotation

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

#### @TemplateIntegrationTest Annotation

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

Please refer to `Templates Plugin` to use and validate such annotations.

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

| Template              | UDF Input Type | Input Description                               | UDF Output Type | Output Description                                                                                     |
|-----------------------|----------------|-------------------------------------------------|-----------------|--------------------------------------------------------------------------------------------------------|
| Datastore Bulk Delete | String         | A JSON string of the entity                     | String          | A JSON string of the entity to delete; filter entities by returning undefined                          |
| Datastore to Pub/Sub  | String         | A JSON string of the entity                     | String          | The payload to publish to Pub/Sub                                                                      |
| Datastore to GCS Text | String         | A JSON string of the entity                     | String          | A single-line within the output file                                                                   |
| GCS Text to BigQuery  | String         | A single-line within the input file             | String          | A JSON string which matches the destination table's schema                                             |
| Pub/Sub to BigQuery   | String         | A string representation of the incoming payload | String          | A JSON string which matches the destination table's schema                                             |
| Pub/Sub to Datastore  | String         | A string representation of the incoming payload | String          | A JSON string of the entity to write to Datastore                                                      |
| Pub/Sub to Splunk     | String         | A string representation of the incoming payload | String          | The event data to be sent to Splunk HEC events endpoint. Must be a string or a stringified JSON object |

### UDF Examples

For a comprehensive list of samples, please check our [udf-samples](v2/common/src/main/resources/udf-samples) folder.

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


## Generated Documentation

This repository contains generated documentation, which contains a list of parameters
and instructions on how to customize and/or build every template.

To generate the documentation for all templates, the following command can be used:

```shell
mvn clean prepare-package \
  -DskipTests \
  -PtemplatesSpec
```

## Release Process

Templates are released in a weekly basis (best-effort) as part of the efforts to
keep [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates) updated with latest fixes and improvements.

In case desired, you can stage and use your own changes using the `Staging (Deploying) Templates` steps.

To execute the release of multiple templates, we provide a single Maven command to release Templates, which is a shortcut to
stage all templates while running additional validations.

```shell
mvn clean package -PtemplatesRelease \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DlibrariesBucketName="{bucketName}-libraries" \
  -DstagePrefix="$(date +%Y_%m_%d)-00_RC00"
```

## More Information

* [Dataflow templates](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates)
* [Google-provided templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
* [Apache Beam](https://beam.apache.org)
  - [Overview](https://beam.apache.org/use/beam-overview/)
  - Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)
