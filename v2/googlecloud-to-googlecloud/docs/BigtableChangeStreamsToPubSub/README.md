# Cloud Bigtable Change Streams To Pub/Sub Dataflow Template
NOTE: This template is currently unreleased. If you wish to use it now, you
will need to follow the steps outlined below to add it to and run it from
your own Google Cloud project. Make sure to be in the /v2 directory.

The [BigtableChangeStreamsToPubSub](src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstopubsub/BigtableChangeStreamsToPubSub.java)
pipeline reads messages from Cloud Bigtable Change Streams and publish them through Pub/Sub.

Change log schema in different formats such as JSON, AVRO and PROTOCOL_BUFFERS are available in .avsc and .proto files resources directory.
And the change log schema is defined as follows:

| Field name      | Protobuf Type | Avro Type      | JSON Type | Nullable? | Description                                                                                                                                  |
|-----------------|------|----------------|-----------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------|
| rowKey          | STRING | STRING         | STRING    | N         | Bigtable row key                                                                                                                             |
| modType         | STRING | STRING         | STRING    | N         | Modification type: {SET_CELL, DELETE_CELLS, DELETE_FAMILY}. DeleteFromRow mutation is converted into a series of DELETE_FROM_FAMILY entries. |
| isGC            | BOOL | BOOLEAN        | BOOL      | Y         | TRUE indicates that mutation was made by garbage collection in CBT                                                                           |
| tiebreaker*     | INT32 | INT            | INTEGER   | N         | CBT tie-breaker value. Used for conflict resolution if two mutations are committed at the sametime.                                          | 
| commitTimestamp | TIMESTAMP | TIMESTAMP-MILLIS | STRING    | N         | Time when CBT wrote this mutation to a tablet                                                                                                |
| columnFamily    | STRING | STRING         | STRING    | N         | CBT column family name                                                                                                                       |
| column          | STRING | STRING         | STRING    | Y         | CBT column qualifier                                                                                                                         |
| timestamp       | INT64 | LONG | STRING    | Y         | CBT cell’s timestamp in microseconds. Type is determined by _writeNumericTimestamps_ pipeline option                                         |
| timestampFrom   | NT64 | LONG        | STRING    | Y         | Time range start in microseconds (inclusive) for a DeleteFromColumn mutation. Type is determined by _writeNumericTimestamps_ pipeline option |
| timestampTo     | INT64 | LONG        | STRING    | Y         | Time range end in microseconds (exclusive) for a DeleteFromColumn mutation. Type is determined by _writeNumericTimestamps_ pipeline option   |                             
| value           | BYTES | BYTES          | STRING    | Y         | Bigtable cell value. Not specified for delete operations                                                                                     |
&ast; Populating these columns can be skipped using pipeline options configuration.
## Getting started

### Requirements
* Java 11
* Maven
* Cloud Bigtable Instance exists
* Cloud Bigtable table exists
* Cloud Bigtable metadata Instance exists
* Cloud Bigtable Change Streams feature is enabled for the Cloud Bigtable table
* Pub/Sub Topic exists

### Building and Running the template

Run the following command to generate the most up-to-date documentation for the
template:

```build
 mvn clean package -DskipTests -PtemplatesSpec -rf :googlecloud-to-googlecloud
```

The documentation provides list of options and commands how to build and launch
the template using ``Maven`` and ``gcloud``.