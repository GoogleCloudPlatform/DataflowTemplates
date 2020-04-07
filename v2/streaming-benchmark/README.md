# Dataflow Streaming Benchmark

During Dataflow pipelines development, common requirement is to run a benchmark at a specific QPS using
fake or generated data. This pipeline takes in a QPS parameter, a path to a schema file, and 
generates fake JSON messages matching the schema to a Pub/Sub topic at the QPS rate.

## Pipeline

[StreamingBenchmark](src/main/java/com/google/cloud/teleport/v2/templates/StreamingBenchmark.java) -
A streaming pipeline which generates messages at a specified rate to a Pub/Sub topic. The messages 
are generated according to a schema template and instructs the pipeline to populate the 
messages with fake data compliant to constraints.

> Note the number of workers executing the pipeline must be large enough to support the supplied 
> QPS. Use a general rule of 2,500 QPS per core in the worker pool when configuring your pipeline.


![Pipeline DAG](img/pipeline-dag.png "Pipeline DAG")

## Getting Started

### Requirements

* Java 8
* Maven 3
* PubSub Topic

### Creating the Schema File
The schema file used to generate JSON messages with fake data is based on the 
[json-data-generator](https://github.com/vincentrussell/json-data-generator) library. This library
allows for the structuring of a sample JSON schema and injection of common faker functions to 
instruct the data generator of what type of fake data to create in each field. See the 
json-data-generator [docs](https://github.com/vincentrussell/json-data-generator) for more 
information on the faker functions.

#### Example Schema File
Below is an example schema file which generates fake game event payloads with random data.

```javascript
{
  "eventId": "{{uuid()}}",
  "eventTimestamp": {{timestamp()}},
  "ipv4": "{{ipv4()}}",
  "ipv6": "{{ipv6()}}",
  "country": "{{country()}}",
  "username": "{{username()}}",
  "quest": "{{random("A Break In the Ice", "Ghosts of Perdition", "Survive the Low Road")}}",
  "score": {{integer(100, 10000)}},
  "completed": {{bool()}}
}
```

#### Example Output Data
Based on the above schema, the below would be an example of a message which would be output to the
Pub/Sub topic.

```javascript
{
  "eventId": "5dacca34-163b-42cb-872e-fe3bad7bffa9",
  "eventTimestamp": 1537729128894,
  "ipv4": "164.215.241.55",
  "ipv6": "e401:58fc:93c5:689b:4401:206f:4734:2740",
  "country": "Montserrat",
  "username": "asellers",
  "quest": "A Break In the Ice",
  "score": 2721,
  "completed": false
}
```
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
export APP_ROOT=/template/streaming-benchmark
export COMMAND_SPEC=${APP_ROOT}/resources/streaming-benchmark-command-spec.json
```
* Build and push image to Google Container Repository

```sh
mvn clean package -Dimage=${TARGET_GCR_IMAGE} \
                  -Dbase-container-image=${BASE_CONTAINER_IMAGE} \
                  -Dbase-container-image.version=${BASE_CONTAINER_IMAGE_VERSION} \
                  -Dapp-root=${APP_ROOT} \
                  -Dcommand-spec=${COMMAND_SPEC}
```

#### Creating Template Spec

Create template spec in Cloud Storage with path to container image in Google Container Repository and pipeline metadata.

```json
{
	"image": "gcr.io/project-id/image-name",
	"metadata": {
		"name": "Streaming pipelines Benchmarking",
		"description": "Publishes the messages at specified qps to benchmark performance of streaming pipelines",
		"parameters": [{
				"name": "autoscalingAlgorithm",
				"label": "autoscaling Algorithm",
				"helpText": "autoscalingAlgorithm",
				"paramType": "TEXT"
			},
			{
				"name": "schemaLocation",
				"label": "Location of Schema file in json format",
				"helpText": "GCS path of schema location. ex: gs://MyBucket/file.json",
				"regexes": [
					"^gs:\\/\\/[^\\n\\r]+$"
				],
				"paramType": "GCS_READ_FILE"
			},
			{
				"name": "topic",
				"label": "PubSub Topic name",
				"helpText": "Name of pubsub topic. ex: projects/<project-id>/topics/<topic-id>",
				"regexes": [
					"^projects/.+/topics/.+"
				],
				"paramType": "PUBSUB_TOPIC"
			},
			{
				"name": "qps",
				"label": "Messages per second",
				"helpText": "Messages per second",
				"regexes": [
					"^[1-9]+$"
				],
				"paramType": "TEXT"
			}
		]
	   },
	"sdk_info": {
			"language": "JAVA"
		}
}
```

### Testing Template

The template unit tests can be run using:

```sh
mvn test
```
### Executing Template
The template requires the following parameters:
* schemaLocation: GCS Location of schema file in json format (e.g: gs://<path-to-schema-location-in-gcs>).
* pubsubTopic: PubSub Topic id.
* qps: Queries Per Second

Template can be executed using the following gcloud command:
```sh
TEMPLATE_SPEC_GCSPATH=gs://path/to/template-spec
SCHEMA_LOCATION=gs://path/to/schemafile.json
PUBSUB_TOPIC=projects/$PROJECT/topics/<topic-id>
QPS=1

gcloud beta dataflow jobs run $JOB_NAME \
        --project=$PROJECT --region=us-central1 --flex-template  \
        --gcs-location=$TEMPLATE_SPEC_GCSPATH \
        --parameters autoscalingAlgorithm="THROUGHPUT_BASED",schemaLocation=$SCHEMA_LOCATION,topic=$PUBSUB_TOPIC,qps=$QPS
```
 *Note*: Additional options such as max workers, service account can be specified in the parameters section as shown below:

 ```sh
  --parameters autoscalingAlgorithm="THROUGHPUT_BASED",schemaLocation=$SCHEMA_LOCATION,topic=$PUBSUB_TOPIC,qps=$QPS,maxNumWorkers=5,serviceAccount=$serviceAccount
```
