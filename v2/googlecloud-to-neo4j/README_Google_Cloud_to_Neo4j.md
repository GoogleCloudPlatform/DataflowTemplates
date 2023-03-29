Google Cloud to Neo4j Template
---
Copy data from Google Cloud (BigQuery, Text) into Neo4j.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **jobSpecUri** (Path to job configuration file): The path to the job specification file, which contains the configuration for source and target metadata.
* **neo4jConnectionUri** (Path to Neo4j connection metadata): Path to Neo4j connection metadata JSON file.
* **optionsJson** (Options JSON): Options JSON. Use runtime tokens. (Example: {token1:value1,token2:value2}).

### Optional Parameters

* **readQuery** (Query SQL): Override SQL query (optional). Defaults to empty.
* **inputFilePattern** (Path to Text File): Override text file pattern (optional) (Example: gs://your-bucket/path/*.json). Defaults to empty.
* **disabledAlgorithms** (Disabled algorithms to override jdk.tls.disabledAlgorithms): Comma separated algorithms to disable. If this value is set to "none" then dk.tls.disabledAlgorithms is set to "". Use with care, as the algorithms disabled by default are known to have either vulnerabilities or performance issues. For example: SSLv3, RC4.
* **extraFilesToStage** (Extra files to stage in the workers): Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files will be saved under the `/extra_files` directory in each worker. (Example: gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id).

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

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

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Google_Cloud_to_Neo4j"

### Mandatory
export JOB_SPEC_URI=<jobSpecUri>
export NEO4J_CONNECTION_URI=<neo4jConnectionUri>
export OPTIONS_JSON=<optionsJson>

### Optional
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

### Mandatory
export JOB_SPEC_URI=<jobSpecUri>
export NEO4J_CONNECTION_URI=<neo4jConnectionUri>
export OPTIONS_JSON=<optionsJson>

### Optional
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
