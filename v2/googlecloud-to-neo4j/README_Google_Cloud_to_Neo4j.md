
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

### Required parameters

* **jobSpecUri** : The path to the job specification file, which contains the configuration for source and target metadata.

### Optional parameters

* **neo4jConnectionUri** : The path to the Neo4j connection metadata JSON file.
* **neo4jConnectionSecretId** : The secret ID for the Neo4j connection metadata. This is an alternative to the GCS path option.
* **optionsJson** : Options JSON. Use runtime tokens. (Example: {token1:value1,token2:value2}). Defaults to empty.
* **readQuery** : Override SQL query. Defaults to empty.
* **inputFilePattern** : Override text file pattern (Example: gs://your-bucket/path/*.json). Defaults to empty.
* **disabledAlgorithms** : Comma separated algorithms to disable. If this value is set to none, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** : Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. (Example: gs://<BUCKET>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>).



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
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-f v2/googlecloud-to-neo4j
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

### Optional
export NEO4J_CONNECTION_URI=<neo4jConnectionUri>
export NEO4J_CONNECTION_SECRET_ID=<neo4jConnectionSecretId>
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
  --parameters "neo4jConnectionSecretId=$NEO4J_CONNECTION_SECRET_ID" \
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

### Optional
export NEO4J_CONNECTION_URI=<neo4jConnectionUri>
export NEO4J_CONNECTION_SECRET_ID=<neo4jConnectionSecretId>
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
-Dparameters="jobSpecUri=$JOB_SPEC_URI,neo4jConnectionUri=$NEO4J_CONNECTION_URI,neo4jConnectionSecretId=$NEO4J_CONNECTION_SECRET_ID,optionsJson=$OPTIONS_JSON,readQuery=$READ_QUERY,inputFilePattern=$INPUT_FILE_PATTERN,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-f v2/googlecloud-to-neo4j
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/googlecloud-to-neo4j/terraform/Google_Cloud_to_Neo4j
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

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
    # neo4jConnectionUri = "<neo4jConnectionUri>"
    # neo4jConnectionSecretId = "<neo4jConnectionSecretId>"
    # optionsJson = "{token1:value1,token2:value2}"
    # readQuery = ""
    # inputFilePattern = "gs://your-bucket/path/*.json"
    # disabledAlgorithms = "SSLv3, RC4"
    # extraFilesToStage = "gs://<BUCKET>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>"
  }
}
```
