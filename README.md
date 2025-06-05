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

## Status

[![Bigtable PR](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/bigtable-pr.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Bigtable+PR%22+branch%3Amain+event%3Aschedule)
[![Datastream PR](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/datastream-pr.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Datastream+PR%22+branch%3Amain+event%3Aschedule)
[![Java PR](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/java-pr.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Java+PR%22+branch%3Amain+event%3Aschedule)
[![Kafka PR](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/kafka-pr.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Kafka+PR%22+branch%3Amain+event%3Aschedule)
[![Load Tests](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/load-tests.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Load+Tests%22+branch%3Amain+event%3Aschedule)
[![Run IT Tests with Beam Snapshots](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/run-it-tests-beam-snapshots.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Run+IT+Tests+with+Beam+Snapshots%22+branch%3Amain+event%3Aschedule)
[![Spanner Load Tests](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/spanner-load-tests.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Spanner+Load+Tests%22+branch%3Amain+event%3Aschedule)
[![Spanner PR](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions/workflows/spanner-pr.yml/badge.svg?event=schedule&&?branch=main)](https://github.com/GoogleCloudPlatform/DataflowTemplates/actions?query=workflow%3A%22Spanner+PR%22+branch%3Amain+event%3Aschedule)

## Note on Default Branch

As of November 18, 2021, our default branch is now named `main`. This does not
affect forks. If you would like your fork and its local clone to reflect these
changes you can
follow [GitHub's branch renaming guide](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-branches-in-your-repository/renaming-a-branch).

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
    - [MongoDB (CDC) to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20MongoDB_to_BigQuery_CDC&type=code)
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
    - [Any SourceDB to Cloud Spanner](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Sourcedb_To_Spanner&type=code)
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
    - [Google Ads to BigQuery](https://github.com/search?q=repo%3AGoogleCloudPlatform%2FDataflowTemplates%20Google_Ads_to_BigQuery&type=code)
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

## UDF Examples

For a comprehensive list of samples, please check our [udf-samples](v2/common/src/main/resources/udf-samples) folder.

### Adding fields

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

### Filtering records

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

## Contributing

To contribute to the repository, see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Release Process

Templates are released in a weekly basis (best-effort) as part of the efforts to
keep [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
updated with latest fixes and improvements.

To learn more about this process, or how you can stage your own changes, see [Release Process](./release-process.md).

## More Information

* [Dataflow](https://cloud.google.com/dataflow/docs/overview) - general Dataflow documentation.
* [Dataflow Templates](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates) - basic template concepts.
* [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates) - official documentation for templates provided by Google (the source code is in this repository).
* Dataflow Cookbook: [Blog](https://cloud.google.com/blog/products/data-analytics/introducing-dataflow-cookbook), [GitHub Repository](https://github.com/GoogleCloudPlatform/dataflow-cookbook) - pipeline examples and practical solutions to common data processing challenges.
* [Dataflow Metrics Collector](https://github.com/GoogleCloudPlatform/dataflow-metrics-exporter) -  CLI tool to collect dataflow resource & execution metrics and export to either BigQuery or Google Cloud Storage. Useful for comparison and visualization of the metrics while benchmarking the dataflow pipelines using various data formats, resource configurations etc
* [Apache Beam](https://beam.apache.org)
  - [Overview](https://beam.apache.org/use/beam-overview/)
  - Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)
  - [Tour of Beam](https://tour.beam.apache.org/) - an interactive tour with learning topics covering core Beam concepts from simple ones to more advanced ones.
  - [Beam Playground](https://beam.apache.org/get-started/try-beam-playground/) -  an interactive environment to try out Beam transforms and examples without having to install Apache Beam.
  - [Beam College](https://beamcollege.dev/) - hands-on training and practical tips, including video recordings of Apache Beam and Dataflow Templates lessons.
  - [Getting Started with Apache Beam - Quest](https://www.cloudskillsboost.google/course_templates/724) - A 5 lab series that provides a Google Cloud certified badge upon completion.
