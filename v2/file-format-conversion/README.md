# File Format Conversion Dataflow Template

The [FileFormatConversion](src/main/java/com/google/cloud/teleport/v2/templates/FileFormatConversion.java) pipeline reads a file from Cloud Storage, converts it to a desired format and stores it back in Cloud Storage. The supported file transformations are:
* Csv to Avro
* Csv to Parquet
* Avro to Parquet
* Parquet to Avro

## Getting started 

### Requirements
* Java 8
* Maven
* Input file in Cloud Storage exists
* Cloud Storage output bucket exists

  
### Building Template
This is a flex template meaning that the pipeline code will be containerized and the container will be used to launch the pipeline.

#### Building Container Image
* Set environment variables that will be used in the build process.
```sh
export PROJECT=my-project
export IMAGE_NAME=my-image-name
export BUCKET_NAME=gs://<bucket-name>
export TARGET_GCR_IMAGE=gcr.io/${PROJECT}/${IMAGE_NAME}
export BASE_CONTAINER_IMAGE=gcr.io/dataflow-templates-base/java8-template-launcher-base
export BASE_CONTAINER_IMAGE_VERSION=latest
export APP_ROOT=/template/file-format-conversion
export COMMAND_SPEC=${APP_ROOT}/resources/file-format-conversion-command-spec.json
```
* Build and push image to Google Container Repository
```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC}
```

#### Creating Image Spec

Create file in Cloud Storage with path to container image in Google Container Repository.
```json
{
  "docker_template_spec": {
    "docker_image": "gcr.io/project/my-image-name"
  }
}
```

### Testing Template

The template unit tests can be run using:
```sh
mvn test
```

### Individual pipeline parameters

#### Csv to Avro/Parquet

The pipeline requires the following parameters:
* inputFileFormat: csv
* outputFileFormat: avro/parquet
* inputFileSpec: GCS bucket to read input file from (e.g. gs://mybucket/path/file)
* outputBucket: GCS bucket to write output file(s) to (e.g. gs://mybucket/path/). Must end with a slash.
* containsHeaders: Set to true if Csv file contains headers.
* schema: Path to [Avro schema](https://avro.apache.org/docs/1.8.1/spec.html#schemas) (e.g. gs://mybucket/path/).  
  NOTE: The order of fields in the Avro schema must match the order of the columns exactly in the Csv file.

The pipeline has the following optional parameters:
* csvFormat: Csv format according to [Apache Commons CSV format](https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.html). Default is: Default  
  NOTE: Must match format names exactly found [here](http://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html#Default).
* delimiter: Delimiter for the CSV file. Default: comma (,).  
* outputFilePrefix: The prefix of the files to write to. Default: output.
* numShards: The maximum number of output shards produced when writing. Default: decided by runner.

#### Avro to Parquet

The pipeline requires the following parameters:
* inputFileFormat: avro
* outputFileFormat: parquet
* inputFileSpec: GCS bucket to read input file from (e.g. gs://mybucket/path/file)
* outputBucket: GCS bucket to write output file(s) to (e.g. gs://mybucket/path/). Must end with a slash.
* schema: Path to [Avro schema](https://avro.apache.org/docs/1.8.1/spec.html#schemas) (e.g. gs://mybucket/path/).

The pipeline has the following optional parameters:
* outputFilenamePrefix: The prefix of the files to write to. Default: output.
* numShards: The maximum number of output shards produced when writing. Default: decided by runner.
  
#### Parquet to Avro

The pipeline requires the following parameters:
* inputFileFormat: parquet
* outputFileFormat: avro
* inputFileSpec: GCS bucket to read input file from (e.g. gs://mybucket/path/file)
* outputBucket: GCS bucket to write output file(s) to (e.g. gs://mybucket/path/). Must end with a slash.
* schema: Path to [Avro schema](https://avro.apache.org/docs/1.8.1/spec.html#schemas) (e.g. gs://mybucket/path/).

The pipeline has the following optional parameters:
* outputFilenamePrefix: The prefix of the files to write to. Default: output.
* numShards: The maximum number of output shards produced when writing. Default: decided by runner.
  
### Executing template
  
Template can be executed using the following API call:
```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
JOB_NAME="csv-to-avro-`date +%Y%m%d-%H%M%S-%N`"
time curl -X POST -H "Content-Type: application/json"     \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "${TEMPLATES_LAUNCH_API}"`
     `"?validateOnly=false"`
     `"&dynamicTemplate.gcsPath=${BUCKET_NAME}/path/to/image-spec"`
     `"&dynamicTemplate.stagingLocation=${BUCKET_NAME}/staging" \
     -d '
      {
       "jobName":"'$JOB_NAME'",
       "parameters": {
           "inputFileFormat":"csv",
           "outputFileFormat":"avro",
           "inputFileSpec":"'$BUCKET_NAME/path/to/input-file.csv'",
           "outputBucket":"'$BUCKET_NAME/path/to/output-location'",
           "containsHeaders":"false",
           "schema":"'$BUCKET_NAME/path/to/avro-schema'",
           "outputFilePrefix":"output-avro-file",
           "numShards":"3",
           "csvFormat":"Default",
           "delimiter":","
        }
       }
      '
```
