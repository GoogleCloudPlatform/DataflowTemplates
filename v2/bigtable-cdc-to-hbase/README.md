# Bigtable CDC to Hbase Dataflow Template

The [BigtableToHBase](src/main/java/com/google/cloud/teleport/v2/templates/BigtableToHbasePipeline.java) pipeline reads from a Bigtable change stream, applies a mutation converter to convert change stream mutations to HBase mutations, and writes the mutations to a specified Hbase instance.

## Getting Started

### Requirements
* Bigtable table with change streams enabled
* Hbase instance

## Building and Running This Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

All commands are assumed to be run in the root directory of this repository.

### Running Template
* Set environment variables.

```shell
# necessary parameters
# Bigtable configs
export PROJECT=<gcp-project-id>
export INSTANCE=<bigtable-instance-id>
export TABLE=<bigtable-table-id>
export APP_PROFILE=<bigtable-table-app-profile with change streams enabled>
# Dataflow configs
export GCS_BUCKET_NAME=<gcs-bucket-name without gs://, to stage template at>
export REGION=<gcp-region to run dataflow job at>
# Hbase configs
export ZOOKEEPER_QUORUM=<zookeeper-quorum, e.g. my-zookeeper-server:2181>
export HBASE_ROOT_DIR=<hbase-root-dir, e.g. hdfs://my-server/hbase>

# optional parameters, modify as needed
export HBASE_DISTRIBUTED=true
export TWO_WAY_REPLICATION=true
export CBT_QUALIFIER=SOURCE_CBT
export HBASE_QUALIFIER=SOURCE_HBASE
```

* Stage and run template in Dataflow:
*
```shell
mvn clean package -PtemplatesRun \
  -Dcheckstyle.skip \
  -DskipTests \
  -DprojectId=$PROJECT \
  -DbucketName=$GCS_BUCKET_NAME \
  -Dregion=$REGION \
  -DtemplateName="bigtable-cdc-to-hbase" \
  -Dparameters="bigtableProjectId=$PROJECT,instanceId=$INSTANCE,tableId=$TABLE,appProfileId=$APP_PROFILE,hbaseZookeeperQuorum=$ZOOKEEPER_QUORUM,hbaseRootDir=$HBASE_ROOT_DIR,hbaseClusterDistributed=$HBASE_DISTRIBUTED,twoWayReplicationEnabled=$TWO_WAY_REPLICATION,cbtQualifier=$CBT_QUALIFIER,hbaseQualifier=$HBASE_QUALIFIER" \
  -pl v2/bigtable-cdc-to-hbase
```
### Testing Template

To run unit tests:

```shell
mvn clean compile test -pl v2/bigtable-cdc-to-hbase
```

To run integration tests (WIP):

```shell
mvn clean verify -PtemplatesIntegrationTests \
  -DprojectId=$PROJECT \
  -DbucketName=$GCS_BUCKET_NAME \
  -Dregion=$REGION \
  -DtemplateName="bigtable-cdc-to-hbase" \
  -Dparameters="bigtableProjectId=$PROJECT,instanceId=$INSTANCE,tableId=$TABLE,appProfileId=$APP_PROFILE,hbaseZookeeperQuorum=$ZOOKEEPER_QUORUM,hbaseRootDir=$HBASE_ROOT_DIR,hbaseClusterDistributed=$HBASE_DISTRIBUTED,twoWayReplicationEnabled=$TWO_WAY_REPLICATION,cbtQualifier=$CBT_QUALIFIER,hbaseQualifier=$HBASE_QUALIFIER" \
  -pl v2/bigtable-cdc-to-hbase \
  -Dproject=$PROJECT \
  -DstageBucket=$GCS_BUCKET_NAME \
  -DdirectRunnerTest
```
