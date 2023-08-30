
BigQuery to MongoDB template
---
The BigQuery to MongoDB template is a batch pipeline that reads rows from a
BigQuery and writes them to MongoDB as documents. Currently each row is stored as
a document.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/bigquery-to-mongodb)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=BigQuery_to_MongoDB).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **mongoDbUri** (MongoDB Connection URI): URI to connect to MongoDB Atlas. Defaults to: mongouri.
* **database** (MongoDB Database): Database in MongoDB to store the collection. (Example: my-db). Defaults to: db.
* **collection** (MongoDB collection): Name of the collection inside MongoDB database. (Example: my-collection). Defaults to: collection.
* **inputTableSpec** (BigQuery source table): BigQuery source table spec. (Example: bigquery-project:dataset.input_table). Defaults to: bqtable.

### Optional Parameters




## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-mongodb/src/main/java/com/google/cloud/teleport/v2/mongodb/templates/BigQueryToMongoDb.java)

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
-DtemplateName="BigQuery_to_MongoDB" \
-pl v2/googlecloud-to-mongodb \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/BigQuery_to_MongoDB
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/BigQuery_to_MongoDB"

### Required
export MONGO_DB_URI=mongouri
export DATABASE=db
export COLLECTION=collection
export INPUT_TABLE_SPEC=bqtable

### Optional

gcloud dataflow flex-template run "bigquery-to-mongodb-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "mongoDbUri=$MONGO_DB_URI" \
  --parameters "database=$DATABASE" \
  --parameters "collection=$COLLECTION" \
  --parameters "inputTableSpec=$INPUT_TABLE_SPEC"
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
export MONGO_DB_URI=mongouri
export DATABASE=db
export COLLECTION=collection
export INPUT_TABLE_SPEC=bqtable

### Optional

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="bigquery-to-mongodb-job" \
-DtemplateName="BigQuery_to_MongoDB" \
-Dparameters="mongoDbUri=$MONGO_DB_URI,database=$DATABASE,collection=$COLLECTION,inputTableSpec=$INPUT_TABLE_SPEC" \
-pl v2/googlecloud-to-mongodb \
-am
```
