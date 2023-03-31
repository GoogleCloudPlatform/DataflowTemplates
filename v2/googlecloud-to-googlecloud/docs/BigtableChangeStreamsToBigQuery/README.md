# Cloud Bigtable Change Streams To BigQuery Dataflow Template
NOTE: This template is currently unreleased. If you wish to use it now, you
will need to follow the steps outlined below to add it to and run it from
your own Google Cloud project. Make sure to be in the /v2 directory.

The [BigtableChangeStreamsToBigQuery](src/main/java/com/google/cloud/teleport/v2/templates/bigtablechangestreamstobigquery/BigtableChangeStreamsToBigQuery.java)
pipeline reads messages from Cloud Bigtable Change Streams and stores them in a
BigQuery table having a changelog schema.

Changelog schema is defined as follows:

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
* Populating these columns can be skipped using pipeline options configuration. 

## Getting started

### Requirements
* Java 11
* Maven
* Cloud Bigtable Instance exists
* Cloud Bigtable table exists
* Cloud Bigtable metadata Instance exists
* Cloud Bigtable Change Streams feature is enabled for the Cloud Bigtable table
* BigQuery dataset exists

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
run on Dataflow.

##### Building Container Image

* Set environment variables that will be used in the build process.
* Run commands below from the repository root (DataflowTemplates).

```sh
export PROJECT=project
export IMAGE_NAME=googlecloud-to-googlecloud
export BUCKET_NAME=gs://bucket
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/images/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export TEMPLATE_MODULE=v2/googlecloud-to-googlecloud
export COMMAND_MODULE=bigtable-changestreams-to-bigquery
export APP_ROOT=/template/${COMMAND_MODULE}
export COMMAND_SPEC=${APP_ROOT}/resources/${COMMAND_MODULE}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${COMMAND_MODULE}-image-spec.json
```
* Build and push image to Google Container Repository from the v2 directory

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC} \
                  -am -pl ${TEMPLATE_MODULE}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.

```sh
echo '{
    "image": "'${TARGET_GCR_IMAGE}'",
    "metadata":{"name":"Bigtable change streams to BigQuery",
    "description":"Streaming pipeline. Streams change stream records and writes them into a BigQuery table. Note the created pipeline will run on Dataflow Runner V2",
    "parameters":[
        {
            "label": "Bigtable Instance ID",
            "help_text": "The Cloud Bigtable instance to read change streams from.",
            "name": "bigtableInstanceId",
            "is_optional": false,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable table ID",
            "help_text": "The Cloud Bigtable table to read change streams from.",
            "name": "bigtableTableId",
            "is_optional": false,
            "param_type": "TEXT"
        },
        {
            "label": "BigQuery dataset",
            "help_text": "The BigQuery dataset for change streams output.",
            "name": "bigQueryDataset",
            "is_optional": false,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable application profile ID",
            "help_text": "The application profile is used to distinguish workload in Cloud Bigtable. The application profile needs to be single-cluster routing and have single-row transactions enabled",
            "name": "bigtableAppProfileId",
            "is_optional": false,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable Project ID",
            "help_text": "Project to read change streams from. The default for this parameter is the project where the Dataflow pipeline is running.",
            "name": "bigtableProjectId",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable metadata instance ID",
            "help_text": "The Cloud Bigtable instance to use for the change streams connector metadata table.",
            "name": "bigtableMetadataInstanceId",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable metadata table ID",
            "help_text": "The Cloud Bigtable change streams connector metadata table ID to use. If not provided, a Cloud Bigtable change streams connector metadata table will automatically be created during the pipeline flow. This parameter must be provided when updating an existing pipeline and should not be provided otherwise.",
            "name": "bigtableMetadataTableTableId",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable column families to ignore",
            "help_text": "A comma-separated list of column family names changes to which are not to be captured.",
            "name": "ignoreColumnFamilies",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Cloud Bigtable columns to ignore",
            "help_text": "A comma-separated list of column names changes to which are not to be captured",
            "name": "outputFileFormat",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "The timestamp to read change streams from",
            "help_text": "The starting DateTime, inclusive, to use for reading change streams (https://tools.ietf.org/html/rfc3339). For example, 2022-05-05T07:59:59Z. Defaults to the timestamp when the pipeline starts.",
            "name": "startTimestamp",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Write values as BigQuery BYTES",
            "help_text": "When set true rowkeys are written to BYTES column, otherwise to STRING column. Defaults to false.",
            "name": "writeRowkeyAsBytes",
            "is_optional": true,
            "param_type": "TEXT"
        },
        
        {
            "label": "Write values as BigQuery BYTES",
            "help_text": "When set true values are written to BYTES column, otherwise to STRING column. Defaults to false.",
            "name": "writeValuesAsBytes",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Write Bigtable timestamp as BigQuery INT",
            "help_text": "When set true values are written to INT column, otherwise to TIMESTAMP column. Columns affected: `timestamp`, `timestamp_from`, `timestamp_to`. Defaults to false.",
            "name": "writeNumericTimestamps",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "BigQuery charset name when reading values and column qualifiers",
            "help_text": "BigQuery charset name when reading values and column qualifiers. Default is UTF-8",
            "name": "bigtableCharset",
            "is_optional": true,
            "param_type": "TEXT"            
        },
        {
            "label": "BigQuery project ID",
            "help_text": "The BigQuery Project. Default is the project for the Dataflow job.",
            "name": "bigtableProjectId",
            "is_optional": true,
            "param_type": "TEXT"            
        },
        {
            "label": "BigQuery table name",
            "help_text": "The BigQuery table name that contains the change log. Default: {bigtableTableId}_changelog",
            "name": "bigQueryChangelogTableName",
            "is_optional": true,
            "param_type": "TEXT"            
        },
        {
            "label": "Changelog table will be partitioned at specified granularity",
            "help_text": "When set, table partitioning will be in effect. Accepted values: `HOUR`, `DAY`, `MONTH`, `YEAR`. Default is no partitioning.",
            "name": "bigQueryChangelogTablePartitionGranularity",
            "is_optional": true,
            "param_type": "TEXT"            
        },
        {
            "label": "Sets partition expiration time in milliseconds",
            "help_text": "When set true partitions older than specified number of milliseconds will be deleted. Default is no expiration.",
            "name": "bigQueryChangelogTablePartitionExpirationMs",
            "is_optional": true,
            "param_type": "TEXT"            
        },
        {
            "label": "Optional changelog table columns to be disabled",
            "help_text": "A comma-separated list of the changelog columns which will not be created and populated if specified. Supported values should be from the following list: `is_gc`, `source_instance`, `source_cluster`, `source_table`, `tiebreaker`, `big_query_commit_timestamp`. Defaults to all columns to be populated",
            "name": "bigQueryChangelogTableFieldsToIgnore",
            "is_optional": true,
            "param_type": "TEXT"
        },
        {
            "label": "Dead letter queue directory",
            "help_text": "The file path to store any unprocessed records with the reason they failed to be processed. Default is a directory under the Dataflow job temp location. The default value is enough under most conditions.",
            "name": "dlqDirectory",
            "is_optional": true,
            "param_type": "TEXT"            
        },
        {
            "label": "Dead letter queue retry minutes",
            "help_text": "The number of minutes between dead letter queue retries. Defaults to 10.",
            "name": "dlqRetryMinutes",
            "is_optional": true,
            "param_type": "TEXT"            
        }
        ]},
    "sdk_info": {
            "language": "JAVA"
    }
}'> image-spec.json
gsutil cp image-spec.json ${TEMPLATE_IMAGE_SPEC}
```


### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

The template requires the following parameters:
* bigtableInstanceId: CBT Instance ID
* bigtableTableId: CBT table ID
* bigQueryDataset: Destination BQ dataset
* bigtableAppProfile: Application profile to use when reading from CBT. A single-cluster routing must be used.

The template has the following optional parameters:
* bigtableProjectId: Default: The project where the Dataflow pipeline is running.
* bigQueryTableChangelogName: Default: bigtableTableId + “_changelog”. Destination BQ table
* metadataInstanceId: Default: the same as bigtableInstanceId. Change streams metadata table.
* metadataTableTableId: Default: “__change_stream_md_table”. Change streams metadata table.
* ignoreColumnFamilies: Default: empty. A comma-separated list of column families for which changes are to be skipped.
* ignoreColumns: Default: empty. A comma-separated list of columnFamily:columnName values for which changes are to be skipped.
* startTimestamp: Default: current wall time. Starting point of Change stream.
* writeNumericTimestamps: Default: false. If true, timestamp, timestamp_from, timestamp_to changelog table columns will use INT type.
* writeRowkeyAsBytes: Default: false. If true, the rowkey will be written as BYTES BQ type.
* writeValuesAsBytes: Default: false. If true, the value will be written as BYTES BQ type.
* bigtableCharset: Default: “UTF-8”. Charset to use when reading CBT column qualifiers and row keys (or values if writeValuesAsBytes is false)
* bigQueryProjectId: Default to be the Dataflow project id.
* bigQueryChangelogTablePartitionGranularity: Default: empty. Valid options are “daily”, “monthly”, “yearly”.
* bigQueryChangelogTablePartitionExpirationMs: Default: never expires. Set partition expiration if specified.
* bigQueryChangelogTableFieldsToIgnore: Default: empty. The fields will not be inserted if specified, this is useful if customers don’t want the metadata columns. Default to empty. Supported values should be in the following list: [is_gc, source_instance, source_cluster, source_table, tiebreaker]
* dlqDirectory: Default: empty. When empty, a temporary Dataflow GCS bucket will be used.
* dlqRetryMinutes: Default: 10 minutes.

Template can be executed using the following gcloud command:

```sh
export JOB_NAME="bigtable-changestreams-to-bigquery-`date +%Y%m%d-%H%M%S-%N`"
export BIGTABLE_INSTANCE_ID=bigtable-instance
export BIGTABLE_TABLE_ID=table-id
export BIGTABLE_APP_PROFILE_ID=app-profile-id
export BIGQUERY_DATASET=bigquery-dataset
export BIGQUERY_CHANGELOG_TABLE_NAME=output-table-name
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters ^~^bigtableInstanceId=${BIGTABLE_INSTANCE_ID}~bigtableTableId=${BIGTABLE_TABLE_ID}~bigtableAppProfileId=${BIGTABLE_APP_PROFILE_ID}~bigQueryDataset=${BIGQUERY_DATASET}~bigQueryChangelogTableName=${BIGQUERY_CHANGELOG_TABLE_NAME}

```

OPTIONAL Dataflow Params:

--numWorkers=2
--maxNumWorkers=10
--workerMachineType=n1-highcpu-4
