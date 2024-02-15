# JDBC to Spanner Dataflow Template

The [JdbcToSpanner](src/main/java/com/google/cloud/teleport/v2/templates/JdbcToSpanner.java) pipeline
ingests data by reading from a database via JDBC, optionally applies a Javascript or Python UDF if supplied
and writes the data to Cloud Spanner database.

Currently, this template works for a basic set of use cases. (A not comprehensive
list of) Scenarios which are not yet supported
to be implemented going forward
* Generic schema mapping
* Tables with non integer keys
* Support for additional sources
* Support for all datatypes supported in MySQL (And other sources when added)

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
export IMAGE_NAME=jdbc-to-spanner
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java11-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/${IMAGE_NAME}
export DATAFLOW_JAVA_COMMAND_SPEC=${APP_ROOT}/resources/${IMAGE_NAME}-command-spec.json
export TEMPLATE_IMAGE_SPEC=${BUCKET_NAME}/images/${IMAGE_NAME}-image-spec.json

export TOPIC=projects/${PROJECT}/topics/<topic-name>
export SUBSCRIPTION=projects/${PROJECT}/subscriptions/<subscription-name>
export DEADLETTER_TABLE=${PROJECT}:${DATASET_TEMPLATE}.dead_letter

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

The template requires the following parameters:
* **driverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **driverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **connectionURL** (JDBC connection URL string.): The JDBC connection URL string. For example, `jdbc:mysql://some-host:3306/sampledb`. Can be passed in as a string that's Base64-encoded and then encrypted with a Cloud KMS key. Note the difference between an Oracle non-RAC database connection string (`jdbc:oracle:thin:@some-host:<port>:<sid>`) and an Oracle RAC database connection string (`jdbc:oracle:thin:@//some-host[:<port>]/<service_name>`). (Example: jdbc:mysql://some-host:3306/sampledb).
* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.


Template can be executed using the following API call:
```sh
export JOB_NAME="${IMAGE_NAME}-`date +%Y%m%d-%H%M%S-%N`"
gcloud beta dataflow flex-template run ${JOB_NAME} \
        --project=${PROJECT} --region=us-central1 \
        --template-file-gcs-location=${TEMPLATE_IMAGE_SPEC} \
        --parameters instanceId=${INSTANCE_ID},databaseId=${DATABASE_ID},inputFilePattern=${GCS_LOCATION},outputDeadletterTable=${DEADLETTER_TABLE}
```
