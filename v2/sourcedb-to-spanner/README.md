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
* **sourceDbURL** (JDBC URL of the source database): The URL which can be used to connect to the source database. (Example: jdbc:mysql://10.10.10.10:3306/testdb)
* **username** (username of the source database): The username which can be used to connect to the source database.
* **password** (username of the source database): The username which can be used to connect to the source database.
* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.
* **DLQDirectory** (GCS path of the dead letter queue direcotry): The GCS path of the schema mapping file to be used during migrations

#### Optional Parameters
* **jdbcDriverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **jdbcDriverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **tables** (Comma seperated list of tables to migrate): Tables that will be migrated to Spanner. Leave this empty if all tabels are to be migrated. (Example: table1,table2).
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
        --parameters sourceDbURL="jdbc:mysql://<source_ip>:3306/<mysql_db_name>",username=<mysql user>,password=<mysql pass>,instanceId="<spanner instanceid>",databaseId="<spanner_database_id>",projectId="$PROJECT",DLQDirectory=gs://<gcs-dir> \
        --additional-experiments=disable_runner_v2
```
