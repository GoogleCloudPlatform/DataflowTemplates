
Google Cloud to Neo4j template
---
The Google Cloud to Neo4j template lets you import a dataset into a Neo4j
database through a Dataflow job, sourcing data from CSV files hosted in Google
Cloud Storage buckets. It also lets you to manipulate and transform the data at
various steps of the import. You can use the template for both first-time imports
and incremental imports.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/google-cloud-to-neo4j)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Google_Cloud_to_Neo4j).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **jobSpecUri** (Path to the job specification file): The path to the job specification file, which contains the configuration for source and target metadata.
* **neo4jConnectionUri** (Path to the Neo4j connection metadata): The path to Neo4j connection metadata JSON file.

### Optional Parameters

* **optionsJson** (Options JSON): Options JSON. Use runtime tokens. (Example: {token1:value1,token2:value2}). Defaults to empty.
* **readQuery** (Query SQL): Override SQL query. Defaults to empty.
* **inputFilePattern** (Path to Text File): Override text file pattern (Example: gs://your-bucket/path/*.json). Defaults to empty.
* **disabledAlgorithms** (Disabled algorithms to override jdk.tls.disabledAlgorithms): Comma-separated algorithms to disable. If this value is set to `none` then no algorithm is disabled. Use with care, because the algorithms that are disabled by default are known to have either vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** (Extra files to stage in the workers): Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files will be saved under the `/extra_files` directory in each worker (Example: gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-neo4j/src/main/java/com/google/cloud/teleport/v2/neo4j/templates/GoogleCloudToNeo4j.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Google_Cloud_to_Neo4j" \
-pl v2/googlecloud-to-neo4j \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Google_Cloud_to_Neo4j
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Google_Cloud_to_Neo4j"

### Required
export JOB_SPEC_URI=<jobSpecUri>
export NEO4J_CONNECTION_URI=<neo4jConnectionUri>

### Optional
export OPTIONS_JSON=""
export READ_QUERY=""
export INPUT_FILE_PATTERN=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow flex-template run "google-cloud-to-neo4j-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "jobSpecUri=$JOB_SPEC_URI" \
  --parameters "neo4jConnectionUri=$NEO4J_CONNECTION_URI" \
  --parameters "optionsJson=$OPTIONS_JSON" \
  --parameters "readQuery=$READ_QUERY" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "disabledAlgorithms=$DISABLED_ALGORITHMS" \
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export JOB_SPEC_URI=<jobSpecUri>
export NEO4J_CONNECTION_URI=<neo4jConnectionUri>

### Optional
export OPTIONS_JSON=""
export READ_QUERY=""
export INPUT_FILE_PATTERN=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="google-cloud-to-neo4j-job" \
-DtemplateName="Google_Cloud_to_Neo4j" \
-Dparameters="jobSpecUri=$JOB_SPEC_URI,neo4jConnectionUri=$NEO4J_CONNECTION_URI,optionsJson=$OPTIONS_JSON,readQuery=$READ_QUERY,inputFilePattern=$INPUT_FILE_PATTERN,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-pl v2/googlecloud-to-neo4j \
-am
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Here is an example of Terraform command:


```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "google_cloud_to_neo4j" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Google_Cloud_to_Neo4j"
  name              = "google-cloud-to-neo4j"
  region            = var.region
  parameters        = {
    jobSpecUri = "<jobSpecUri>"
    neo4jConnectionUri = "<neo4jConnectionUri>"
  }
}
```
