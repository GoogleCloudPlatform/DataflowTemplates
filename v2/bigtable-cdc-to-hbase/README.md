# Bigtable CDC to Hbase Dataflow Template

The [BigtableToHBase](src/main/java/com/google/cloud/teleport/v2/templates/BigtableCdcToHbase.java) pipeline reads from a Bigtable change stream, applies a mutation converter to convert change stream mutations to HBase mutations, and writes the mutations to a specified Hbase instance.

## Getting Started

### Requirements
* Bigtable table with change streams enabled
* Hbase instance that is accessible from Dataflow

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
export ZOOKEEPER_QUORUM_HOST=<zookeeper-quorum-host, e.g. my-zookeeper-server>
export ZOOKEEPER_QUORUM_PORT=<zookeeper-quorum-port, e.g. 2181>
export HBASE_ROOT_DIR=<hbase-root-dir, e.g. hdfs://my-server/hbase>

# Optional bidirectional replication settings. Enabled by default.
# export BIDIRECTIONAL_REPLICATION=true
# Be wary setting your own qualifiers. They need to match between Hbase and
# Bigtable replicators to correctly prevent replication loops.
# The replicator will use these column qualifier keywords to filter out and
# tag source mutations for bidirectional replication.
# export CBT_QUALIFIER=SOURCE_CBT
# export HBASE_QUALIFIER=SOURCE_HBASE
# Dry run mode is for testing, does not write to Hbase. Disabled by default.
# export DRY_RUN_ENABLED=false
```

* Stage and run template in Dataflow:

```shell
mvn clean package -am -PtemplatesRun \
  -Dcheckstyle.skip \
  -DskipTests \
  -DprojectId=$PROJECT \
  -DbucketName=$GCS_BUCKET_NAME \
  -Dregion=$REGION \
  -DtemplateName="bigtable-cdc-to-hbase" \
  -Dparameters="bigtableReadProjectId=$PROJECT,bigtableReadInstanceId=$INSTANCE,bigtableReadTableId=$TABLE,bigtableChangeStreamAppProfile=$APP_PROFILE,hbaseZookeeperQuorumHost=$ZOOKEEPER_QUORUM_HOST, hbaseZookeeperQuorumPort=$ZOOKEEPER_QUORUM_PORT,hbaseRootDir=$HBASE_ROOT_DIR,bidirectionalReplicationEnabled=$BIDIRECTIONAL_REPLICATION,cbtQualifier=$CBT_QUALIFIER,hbaseQualifier=$HBASE_QUALIFIER" \
  -pl v2/bigtable-cdc-to-hbase
```
### Testing Template

To run unit tests:

```shell
mvn clean compile test -am -pl v2/bigtable-cdc-to-hbase
```

