# Dataflow Flex Template to tokenize data using Protegrity DSG

This directory contains a Dataflow Flex Template that creates a pipeline to read data from one of
the supported sources, tokenize data with external API calls to Protegrity Data Security Gateway
(DSG), and write data into one of the supported sinks.

Supported data formats:

- JSON
- CSV

Supported input sources:

- [Google Cloud Storage](https://cloud.google.com/storage)
- [Google Pub/Sub](https://cloud.google.com/pubsub)

Supported destination sinks:

- Google Cloud Storage
- [Google Cloud BigQuery](https://cloud.google.com/bigquery)
- [Cloud BigTable](https://cloud.google.com/bigtable)

Supported data schema format:

- JSON with an array of fields described in BigQuery format

In the main scenario, the template will create an Apache Beam pipeline that will read data in CSV or
JSON format from a specified input source, send the data to an external processing server, receive
processed data, and write it into a specified output sink.

## Requirements

- Java 8
- 1 of supported sources to read data from
- 1 of supported destination sinks to write data into
- A configured Protegrity DSG

## Getting Started

This section describes what is needed to get the template up and running.

- Set up the environment
- Build Protegrity Data Tokenization Dataflow Flex Template
- Create a Dataflow job to tokenize data using the template

### Setting Up Project Environment

#### Pipeline variables:

```
PROJECT=<my-project>
BUCKET_NAME=<my-bucket>
REGION=<my-region>
```

#### Template Metadata Storage Bucket Creation

The Dataflow Flex template has to store its metadata in a bucket in
[Google Cloud Storage](https://cloud.google.com/storage), so it can be executed from the Google
Cloud Platform. Create the bucket in Google Cloud Storage if it doesn't exist yet:

```
gsutil mb gs://${BUCKET_NAME}
```

#### Containerization variables:

```
IMAGE_NAME=<my-image-name>
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
BASE_CONTAINER_IMAGE=JAVA8
TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/protegrity-data-tokenization.json"
```

## Build Protegrity Data Tokenization Dataflow Flex Template

Dataflow Flex Templates package the pipeline as a Docker image and stage these images on your
project's [Container Registry](https://cloud.google.com/container-registry).

### Assembling the Uber-JAR

The Dataflow Flex Templates require your Java project to be built into an Uber JAR file.

Navigate to the v2 folder:

```
cd /path/to/DataflowTemplates/v2
```

Build the Uber JAR:

```
mvn package -am -pl protegrity-data-tokenization
```

ℹ️ An **Uber JAR** - also known as **fat JAR** - is a single JAR file that contains both target
package *and* all its dependencies.

The result of the `package` task execution is a `protegrity-data-tokenization-1.0-SNAPSHOT.jar`
file that is generated under the `target` folder protegrity-data-tokenization directory.

### Creating the Dataflow Flex Template

To execute the template you need to create the template spec file containing all the necessary
information to run the job. This template already has the following
[metadata file](src/main/resources/protegrity_data_tokenization_metadata.json) in resources.

Navigate to the template folder:

```
cd /path/to/DataflowTemplates/v2/protegrity-data-tokenization
```

Build the Dataflow Flex Template:

```
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
       --image-gcr-path "${TARGET_GCR_IMAGE}" \
       --sdk-language "JAVA" \
       --flex-template-base-image ${BASE_CONTAINER_IMAGE} \
       --metadata-file "src/main/resources/protegrity_data_tokenization_metadata.json" \
       --jar "target/protegrity-data-tokenization-1.0-SNAPSHOT.jar" \
       --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.google.cloud.teleport.v2.templates.ProtegrityDataTokenization"
```

### Executing Template

To deploy the pipeline, you should refer to the template file and pass the
[parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params#setting-other-cloud-dataflow-pipeline-options)
required by the pipeline.

The template requires the following parameters:

- Data schema
    - **dataSchemaGcsPath**: Path to data schema (JSON format) in GCS compatible with BigQuery
- 1 specified input source out of these:
    - Google Cloud Storage
        - **inputGcsFilePattern**: GCS filepattern for files in bucket to read data from
        - **inputGcsFileFormat**: File format of input files. Supported formats: JSON, CSV
        - In case if input data is in CSV format:
            - **csvContainsHeaders**: `true` if file(s) in bucket to read data from contain headers,
              and `false` otherwise
            - **csvDelimiter**: Delimiting character in CSV. Default: use delimiter provided in
              csvFormat
            - **csvFormat**: Csv format according to Apache Commons CSV format. Default is:
              [Apache Commons CSV default](https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.html#DEFAULT)
              . Must match format names exactly found
              at: https://static.javadoc.io/org.apache.commons/commons-csv/1.7/org/apache/commons/csv/CSVFormat.Predefined.html
    - Google Pub/Sub
        - **pubsubTopic**: The Cloud Pub/Sub topic to read from, in the format of '
          projects/yourproject/topics/yourtopic'
- 1 specified output sink out of these:
    - Google Cloud Storage
        - **outputGcsDirectory**: GCS directory in bucket to write data to
        - **outputGcsFileFormat**: File format of output files. Supported formats: JSON, CSV
        - **windowDuration**: The window duration in which data will be written. Should be specified
          only for 'Pub/Sub -> GCS' case. Defaults to 30s.

          Allowed formats are:
            - Ns (for seconds, example: 5s),
            - Nm (for minutes, example: 12m),
            - Nh (for hours, example: 2h).
        - Google Cloud BigQuery
            - **bigQueryTableName**: Cloud BigQuery table name to write into
    - Cloud BigTable
        - **bigTableProjectId**: Id of the project where the Cloud BigTable instance to write into
          is located
        - **bigTableInstanceId**: Id of the Cloud BigTable instance to write into
        - **bigTableTableId**: Id of the Cloud BigTable table to write into
        - **bigTableKeyColumnName**: Column name to use as a key in Cloud BigTable
        - **bigTableColumnFamilyName**: Column family name to use in Cloud BigTable
- DSG parameters
    - **dsgUri**: URI for the API calls to DSG
    - **batchSize**: Size of the batch to send to DSG per request
    - **payloadConfigGcsPath**: GCS path to the payload configuration file with an array of fields
      to extract for tokenization

The template allows for the user to supply the following optional parameter:

- **nonTokenizedDeadLetterGcsPath**: GCS folder where failed to tokenize data will be stored

You can do this in 3 different ways:

1. Using [Dataflow Google Cloud Console](https://console.cloud.google.com/dataflow/jobs)

2. Using `gcloud` CLI tool
    ```bash
    gcloud dataflow flex-template run "protegrity-data-tokenization-`date +%Y%m%d-%H%M%S`" \
        --template-file-gcs-location "${TEMPLATE_PATH}" \
        --parameters <parameter>="<value>" \
        --parameters <parameter>="<value>" \
        ...
        --parameters <parameter>="<value>" \
        --region "${REGION}"
    ```
3. With a REST API request
    ```
    API_ROOT_URL="https://dataflow.googleapis.com"
    TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/locations/${REGION}/flexTemplates:launch"
    JOB_NAME="protegrity-data-tokenization-`date +%Y%m%d-%H%M%S-%N`"
    
    time curl -X POST -H "Content-Type: application/json" \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        -d '
         {
             "launch_parameter": {
                 "jobName": "'$JOB_NAME'",
                 "containerSpecGcsPath": "'$TEMPLATE_PATH'",
                 "parameters": {
                     "<parameter>": "<value>",
                     "<parameter>": "<value>",
                     ...
                     "<parameter>": "<value>"
                 }
             }
         }
        '
        "${TEMPLATES_LAUNCH_API}"
    ```
