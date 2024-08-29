# SourceDB to Spanner Dataflow Template

The [SourceDBToSpanner](src/main/java/com/google/cloud/teleport/v2/templates/SourceDbToSpanner.java) pipeline
ingests data by reading from a database via JDBC and writes the data to Cloud Spanner database.

Currently, this template works for a basic set of use cases. A not comprehensive
list of scenarios which are not yet supported.
to be implemented going forward
* Tables with non integer keys
* Support for additional sources
* Support for tables with foreign keys and interleaving
* Migrating sharded databases

## Getting Started

### Requirements
* Java 11
* Maven 3

### Building Template
This is a Flex Template meaning that the pipeline code will be containerized and the container will be
used to launch the Dataflow pipeline.

#### Building Container Image
* Set environment variables.

```sh
export PROJECT=<my-project>
export IMAGE_NAME=sourcedb-to-spanner
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

gcloud config set project ${PROJECT}
```

* Build and push image to Google Container Repository

```sh
mvn clean package \
-Dimage=${TARGET_GCR_IMAGE} \
-Dbase-container-image=${BASE_CONTAINER_IMAGE} \
-Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
-Dapp-root=${APP_ROOT} \
-Dcommand-spec=${DATAFLOW_JAVA_COMMAND_SPEC} \
-am -pl ${IMAGE_NAME}
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Executing Template

#### Required Parameters
* **sourceConfigURL** (Configuration to connect to the source database): Can be the JDBC URL or the location of the sharding config. (Example: jdbc:mysql://10.10.10.10:3306/testdb or gs://test1/shard.conf). Refer to src/main/scripts/create_simple_shard_config.bash for steps to generate a shard configuration.
* **username** (username of the source database): The username which can be used to connect to the source database.
* **password** (username of the source database): The username which can be used to connect to the source database.
* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.
* **outputDirectory** (GCS path of the output directory): The GCS path of the directory where all errors and skipped events are dumped to be used during migrations

#### Optional Parameters
* **jdbcDriverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **jdbcDriverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **tables** (Colon seperated list of tables to migrate): Tables that will be migrated to Spanner. Leave this empty if all tables are to be migrated. (Example: table1:table2).
* **numPartitions** (Number of partitions to create per table): A table is split into partitions and loaded independently. Use higher number of partitions for larger tables. (Example: 1000).
* **spannerHost** (Cloud Spanner Endpoint): Use this endpoint to connect to Spanner. (Example: https://batch-spanner.googleapis.com)
* **maxConnections** (Number of connections to create per source database): The max number of connections that can be used at any given time at source. (Example: 100)
* **sessionFilePath** (GCS path of the session file): The GCS path of the schema mapping file to be used during migrations

Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"

gcloud dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters sourceConfigURL="jdbc:mysql://<source_ip>:3306/<mysql_db_name>",username=<mysql user>,password=<mysql pass>,instanceId="<spanner instanceid>",databaseId="<spanner_database_id>",projectId="$PROJECT",outputDirectory=gs://<gcs-dir> \
        --additional-experiments=disable_runner_v2
```
#### Replaying DLQ entries.
Any errors to transform a source row or failures to write to spanner get written to `dlq/severe/` path within the `outputDirectory`. It's recommended to retry the DLQ entries before applying any change capture (if any).
To retry the DLQs, please run the [Cloud_Datastream_to_Spanner](../datastream-to-spanner/README_Cloud_Datastream_to_Spanner.md) job in `retryDLQ` mode. After the DLQs are successfully applied, the files will be deleted from the dlq directory.
##### Sample Command to retry DLQs.
The following sample command could help to start a DLQ retry job.
```bash
gcloud  dataflow flex-template run <jobname> \
--region=<the region where the dataflow job must run> \
--template-file-gcs-location=gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner \
--additional-experiments=use_runner_v2 \
--parameters inputFilePattern=<GCS location of the input file pattern>,streamName="ignore", \
--datastreamSourceType=<source_type for example mysql/oracle. This needs to be set in the absence of an actual datastream.>\
instanceId=<Spanner Instance Id>,databaseId=<Spanner Database Id>,sessionFilePath=<GCS path to session file>, \
deadLetterQueueDirectory=<outputDirectory/dlq>,runMode="retryDLQ"
```
##### Checking if all DLQ entries are applied.
To check if all DLQ entries have been applied to spanner, you could count the DLQ files in GCS and wait for it to go to 0.
```bash
gcloud storage ls <outputDirectory>/dlq/severe/**.json | wc -l
```
