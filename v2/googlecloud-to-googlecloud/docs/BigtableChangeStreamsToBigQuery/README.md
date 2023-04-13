# Cloud Bigtable Change Streams To BigQuery Dataflow Template
NOTE: This template is currently unreleased. If you wish to use it now, you
will need to follow the steps outlined below to add it to and run it from
your own Google Cloud project. Make sure to be in the /v2 directory.

The [BigtableChangeStreamsToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstobigquery/BigtableChangeStreamsToBigQuery.java)
pipeline reads messages from Cloud Bigtable Change Streams and stores them in a
BigQuery table having a change log schema.

Change log schema is defined as follows:

| Column name     | BigQuery Type | Required? | Description                                                                                                                                           |
| --------------- | ------------- | --------- |-------------------------------------------------------------------------------------------------------------------------------------------------------|
| row_key | STRING | Y | Bigtable row key                                                                                                                                      |
| mod_type | STRING | Y | Modification type: {SET_CELL, DELETE_CELLS, DELETE_FAMILY}. DeleteFromRow mutation is converted into a series of DELETE_FROM_FAMILY entries.          |
| is_gc* | BOOL | N | TRUE indicates that mutation was made by garbage collection in CBT                                                                                    |
| source_instance* | STRING | N | CBT instance where mutation was made in case a customer wants to run another pipeline writing changes from another CBT instance to the same BQ table. |
| source_cluster* | STRING | N | CBT cluster where mutation was made in case a customer wants to run another pipeline writing changes from another CBT cluster to the same BQ table.   |
| source_table* | STRING | N | CBT table name in case a customer wants to run another pipeline writing changes from another CBT table to the same BQ table.                          |
| tiebreaker* | INT | N | CBT tie-breaker value. Used for conflict resolution if two mutations are committed at the sametime.                                                   | 
| commit_timestamp | TIMESTAMP | Y | Time when CBT wrote this mutation to a tablet                                                                                                         |
| column_family | STRING | Y | CBT column family name                                                                                                                                |
| column | STRING | N | CBT column qualifier                                                                                                                                  |
| timestamp | TIMESTAMP/INT | N | CBT cell’s timestamp. Type is determined by _writeNumericTimestamps_ pipeline option                                                                  |
| timestamp_from | TIMESTAMP/INT | N | Time range start (inclusive) for a DeleteFromColumn mutation. Type is determined by _writeNumericTimestamps_ pipeline option                          |
| timestamp_to | TIMESTAMP/INT | N | Time range end (exclusive) for a DeleteFromColumn mutation. Type is determined by _writeNumericTimestamps_ pipeline option                            |                             
| value | STRING/BYTES | N | Bigtable cell value. Not specified for delete operations                                                                                              |
| big_query_commit_timestamp* | TIMESTAMP | N | BQ auto-generated field describing the time when the record was written to BigQuery                                                                   |
&ast; Populating these columns can be skipped using pipeline options configuration. 

## Getting started

### Requirements
* Java 11
* Maven
* Cloud Bigtable Instance exists
* Cloud Bigtable table exists
* Cloud Bigtable metadata Instance exists
* Cloud Bigtable Change Streams feature is enabled for the Cloud Bigtable table
* BigQuery dataset exists

### Building and Running the template

Run the following command to generate the most up-to-date documentation for the
template: 

```build
 mvn clean package -DskipTests -PtemplatesSpec -rf :googlecloud-to-googlecloud
```

The documentation provides list of options and commands how to build and launch
the template using ``Maven`` and ``gcloud``.