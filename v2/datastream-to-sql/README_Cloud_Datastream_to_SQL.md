
Datastream to SQL template
---
The Datastream to SQL template is a streaming pipeline that reads <a
href="https://cloud.google.com/datastream/docs">Datastream</a> data and
replicates it into any MySQL or PostgreSQL database. The template reads data from
Cloud Storage using Pub/Sub notifications and replicates this data into SQL
replica tables.

The template does not support data definition language (DDL) and expects that all
tables already exist in the database. Replication uses Dataflow stateful
transforms to filter stale data and ensure consistency in out of order data. For
example, if a more recent version of a row has already passed through, a late
arriving version of that row is ignored. The data manipulation language (DML)
that executes is a best attempt to perfectly replicate source to target data. The
DML statements executed follow the following rules:.

If a primary key exists, insert and update operations use upsert syntax (ie.
<code>INSERT INTO table VALUES (...) ON CONFLICT (...) DO UPDATE</code>).
If primary keys exist, deletes are replicated as a delete DML.
If no primary key exists, both insert and update operations are inserted into the
table.
If no primary keys exist, deletes are ignored.
If you are using the Oracle to Postgres utilities, add <code>ROWID</code> in SQL
as the primary key when none exists.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/datastream-to-sql)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_Datastream_to_SQL).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **inputFilePattern** : The file location for the Datastream files in Cloud Storage to replicate. This file location is typically the root path for the stream.
* **databaseHost** : The SQL host to connect on.
* **databaseUser** : The SQL user with all required permissions to write to all tables in replication.
* **databasePassword** : The password for the SQL user.

### Optional parameters

* **gcsPubSubSubscription** : The Pub/Sub subscription with Datastream file notifications. For example, `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_ID>`.
* **inputFileFormat** : The format of the output file produced by Datastream. For example, `avro` or `json`. Defaults to `avro`.
* **streamName** : The name or template for the stream to poll for schema information. The default value is `{_metadata_stream}`.
* **rfcStartDateTime** : The starting DateTime used to fetch from Cloud Storage (https://tools.ietf.org/html/rfc3339). Defaults to: 1970-01-01T00:00:00.00Z.
* **dataStreamRootUrl** : Datastream API Root URL. Defaults to: https://datastream.googleapis.com/.
* **databaseType** : The database type to write to (for example, Postgres). Defaults to: postgres.
* **databasePort** : The SQL database port to connect to. The default value is `5432`.
* **databaseName** : The name of the SQL database to connect to. The default value is `postgres`.
* **schemaMap** : A map of key/values used to dictate schema name changes (ie. old_name:new_name,CaseError:case_error). Defaults to empty.
* **customConnectionString** : Optional connection string which will be used instead of the default database string.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/datastream-to-sql/src/main/java/com/google/cloud/teleport/v2/templates/DataStreamToSQL.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-DtemplateName="Cloud_Datastream_to_SQL" \
-f v2/datastream-to-sql
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_Datastream_to_SQL
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_Datastream_to_SQL"

### Required
export INPUT_FILE_PATTERN=<inputFilePattern>
export DATABASE_HOST=<databaseHost>
export DATABASE_USER=<databaseUser>
export DATABASE_PASSWORD=<databasePassword>

### Optional
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export INPUT_FILE_FORMAT=avro
export STREAM_NAME=<streamName>
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export DATA_STREAM_ROOT_URL=https://datastream.googleapis.com/
export DATABASE_TYPE=postgres
export DATABASE_PORT=5432
export DATABASE_NAME=postgres
export SCHEMA_MAP=""
export CUSTOM_CONNECTION_STRING=""

gcloud dataflow flex-template run "cloud-datastream-to-sql-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputFilePattern=$INPUT_FILE_PATTERN" \
  --parameters "gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION" \
  --parameters "inputFileFormat=$INPUT_FILE_FORMAT" \
  --parameters "streamName=$STREAM_NAME" \
  --parameters "rfcStartDateTime=$RFC_START_DATE_TIME" \
  --parameters "dataStreamRootUrl=$DATA_STREAM_ROOT_URL" \
  --parameters "databaseType=$DATABASE_TYPE" \
  --parameters "databaseHost=$DATABASE_HOST" \
  --parameters "databasePort=$DATABASE_PORT" \
  --parameters "databaseUser=$DATABASE_USER" \
  --parameters "databasePassword=$DATABASE_PASSWORD" \
  --parameters "databaseName=$DATABASE_NAME" \
  --parameters "schemaMap=$SCHEMA_MAP" \
  --parameters "customConnectionString=$CUSTOM_CONNECTION_STRING"
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
export INPUT_FILE_PATTERN=<inputFilePattern>
export DATABASE_HOST=<databaseHost>
export DATABASE_USER=<databaseUser>
export DATABASE_PASSWORD=<databasePassword>

### Optional
export GCS_PUB_SUB_SUBSCRIPTION=<gcsPubSubSubscription>
export INPUT_FILE_FORMAT=avro
export STREAM_NAME=<streamName>
export RFC_START_DATE_TIME=1970-01-01T00:00:00.00Z
export DATA_STREAM_ROOT_URL=https://datastream.googleapis.com/
export DATABASE_TYPE=postgres
export DATABASE_PORT=5432
export DATABASE_NAME=postgres
export SCHEMA_MAP=""
export CUSTOM_CONNECTION_STRING=""

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-datastream-to-sql-job" \
-DtemplateName="Cloud_Datastream_to_SQL" \
-Dparameters="inputFilePattern=$INPUT_FILE_PATTERN,gcsPubSubSubscription=$GCS_PUB_SUB_SUBSCRIPTION,inputFileFormat=$INPUT_FILE_FORMAT,streamName=$STREAM_NAME,rfcStartDateTime=$RFC_START_DATE_TIME,dataStreamRootUrl=$DATA_STREAM_ROOT_URL,databaseType=$DATABASE_TYPE,databaseHost=$DATABASE_HOST,databasePort=$DATABASE_PORT,databaseUser=$DATABASE_USER,databasePassword=$DATABASE_PASSWORD,databaseName=$DATABASE_NAME,schemaMap=$SCHEMA_MAP,customConnectionString=$CUSTOM_CONNECTION_STRING" \
-f v2/datastream-to-sql
```

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
cd v2/datastream-to-sql/terraform/Cloud_Datastream_to_SQL
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

resource "google_dataflow_flex_template_job" "cloud_datastream_to_sql" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_Datastream_to_SQL"
  name              = "cloud-datastream-to-sql"
  region            = var.region
  parameters        = {
    inputFilePattern = "<inputFilePattern>"
    databaseHost = "<databaseHost>"
    databaseUser = "<databaseUser>"
    databasePassword = "<databasePassword>"
    # gcsPubSubSubscription = "<gcsPubSubSubscription>"
    # inputFileFormat = "avro"
    # streamName = "<streamName>"
    # rfcStartDateTime = "1970-01-01T00:00:00.00Z"
    # dataStreamRootUrl = "https://datastream.googleapis.com/"
    # databaseType = "postgres"
    # databasePort = "5432"
    # databaseName = "postgres"
    # schemaMap = ""
    # customConnectionString = ""
  }
}
```
