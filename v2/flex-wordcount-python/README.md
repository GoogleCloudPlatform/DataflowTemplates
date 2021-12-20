# Python Wordcount with file output format support.

The Wordcount pipeline demonstrates how to use the new Flex Templates
feature in Dataflow to create a template out of practically any Dataflow pipeline. This pipeline
does not use any [ValueProvider](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/value_provider.py) to accept user inputs and is built like any other non-templated
Dataflow pipeline. This pipeline also allows the user to change the job
graph depending on the value provided for an option at runtime
(*--format=text|avro|parquet*)

We make the pipeline ready for reuse by "packaging" the pipeline artifacts (python file + [command specs](spec/python_command_spec.json))
in a Docker container. In order to simplify the process of packaging the pipeline into a container we
utilize [Google Cloud Build](https://cloud.google.com/cloud-build/).

We preinstall all the dependencies needed to *compile and execute* the pipeline
into a container using a custom [Dockerfile](Dockerfile).

In this example, we are using the following base image for Python 2.7:

`gcr.io/dataflow-templates-base/python2-template-launcher-base`

We will utilize Google Cloud Builds ability to build a container using a Dockerfile as documented in the [quickstart](https://cloud.google.com/cloud-build/docs/quickstart-docker).


## Pipeline

[WordCount Example](wordcount.py) -
The Wordcount pipeline is a batch pipeline which performs a wordcount on an
input text file (via *--input* flag) and writes the word count to GCS. The
output format is determined by the value of the *--format* flag which can be set
to either *text, avro or parquet*.

## Getting Started

### Requirements

* Latest gcloud SDK
* Enable necessary Google Cloud APIs (Cloud Build, Google Container Registry
  etc)


### Building the Flex Template

Build the container and push to GCR repository

```sh
cd <directory containing the Dockerfile>
PROJECT=my-project
TARGET_GCR_IMAGE=gcr.io/${PROJECT}/flex-template-python2-wc
gcloud builds submit --project=${PROJECT} --tag ${TARGET_GCR_IMAGE} .
```

### Create the Flex Template image spec

Create the following image spec file and store it on Google Cloud Storage

```sh
{
  "docker_template_spec": {
    "docker_image": "gcr.io/project/my-image-name"
  }
}
```

### Executing the Flex Template

```sh
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT}/templates:launch"
JOB_NAME="flex-template-python2-wc-`date +%Y%m%d-%H%M%S-%N`"
INPUT_FILE="gs://path/to/input/file"
OUTPUT_FILE="gs://path/to/output/dir"
FORMAT="text"
time curl -X POST -H "Content-Type: application/json"     \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     "${TEMPLATES_LAUNCH_API}"`
     `"?validateOnly=false"`
     `"&dynamicTemplate.gcsPath=gs://path/to/image/spec"`
     `"&dynamicTemplate.stagingLocation=gs://path/to/stagingLocation" \
     -d '
      {
       "jobName":"'$JOB_NAME'",
       "parameters": {
                   "input":"'$INPUT_FILE'",
                   "output":"'$OUTPUT_FILE'",
                   "format":"'$FORMAT'",
        }
       }
      '
```
