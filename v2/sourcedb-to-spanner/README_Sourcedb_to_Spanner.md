
SourceDB to Spanner template
---
The SourceDB to Spanner template is a batch pipeline that copies data from a
relational database into an existing Spanner database. This pipeline uses JDBC to
connect to the relational database. You can use this template to copy data from
any relational database with available JDBC drivers into Spanner.

For an extra layer of protection, you can also pass in a Cloud KMS key along with
a Base64-encoded username, password, and connection string parameters encrypted
with the Cloud KMS key. See the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a> for additional details on encrypting your
username, password, and connection string parameters.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Sourcedb_to_Spanner_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

#### Required Parameters
* **sourceConfigURL** (Configuration to connect to the source database): Can be the JDBC URL or the location of the sharding config. (Example: jdbc:mysql://10.10.10.10:3306/testdb or gs://test1/shard.conf)
* **username** (username of the source database): The username which can be used to connect to the source database.
* **password** (username of the source database): The username which can be used to connect to the source database.
* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.
* **outputDirectory** (GCS path of the ouput directory): The GCS path of the Directory where all error and skipped events are dumped to be used during migrations

#### Optional Parameters
* **jdbcDriverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **jdbcDriverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **tables** (Colon seperated list of tables to migrate): Tables that will be migrated to Spanner. Leave this empty if all tables are to be migrated. (Example: table1:table2).
* **numPartitions** (Number of partitions to create per table): A table is split into partitions and loaded independently. Use higher number of partitions for larger tables. (Example: 1000).
* **spannerHost** (Cloud Spanner Endpoint): Use this endpoint to connect to Spanner. (Example: https://batch-spanner.googleapis.com)
* **maxConnections** (Number of connections to create per source database): The max number of connections that can be used at any given time at source. (Example: 100)
* **sessionFilePath** (GCS path of the session file): The GCS path of the schema mapping file to be used during migrations



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToSpanner.java)

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
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-pl v2/sourcedb-to-spanner \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Sourcedb_to_Spanner_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Sourcedb_to_Spanner_Flex"

### Required
export SOURCE_CONFIG_URL=<sourceConfigURL>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export JDBC_DRIVER_JARS=""
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export USERNAME=""
export PASSWORD=""
export TABLES=""
export NUM_PARTITIONS=0
export SPANNER_HOST=https://batch-spanner.googleapis.com
export MAX_CONNECTIONS=0
export SESSION_FILE_PATH=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export DEFAULT_LOG_LEVEL=INFO

gcloud dataflow flex-template run "sourcedb-to-spanner-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "jdbcDriverJars=$JDBC_DRIVER_JARS" \
  --parameters "jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME" \
  --parameters "sourceConfigURL=$SOURCE_CONFIG_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "tables=$TABLES" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "maxConnections=$MAX_CONNECTIONS" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "disabledAlgorithms=$DISABLED_ALGORITHMS" \
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
  --parameters "defaultLogLevel=$DEFAULT_LOG_LEVEL"
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
export SOURCE_CONFIG_URL=<sourceConfigURL>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export JDBC_DRIVER_JARS=""
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export USERNAME=""
export PASSWORD=""
export TABLES=""
export NUM_PARTITIONS=0
export SPANNER_HOST=https://batch-spanner.googleapis.com
export MAX_CONNECTIONS=0
export SESSION_FILE_PATH=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export DEFAULT_LOG_LEVEL=INFO

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="sourcedb-to-spanner-flex-job" \
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-Dparameters="jdbcDriverJars=$JDBC_DRIVER_JARS,jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME,sourceConfigURL=$SOURCE_CONFIG_URL,username=$USERNAME,password=$PASSWORD,tables=$TABLES,numPartitions=$NUM_PARTITIONS,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,projectId=$PROJECT_ID,spannerHost=$SPANNER_HOST,maxConnections=$MAX_CONNECTIONS,sessionFilePath=$SESSION_FILE_PATH,outputDirectory=$OUTPUT_DIRECTORY,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,defaultLogLevel=$DEFAULT_LOG_LEVEL" \
-f v2/sourcedb-to-spanner
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Here is an example of Terraform configuration:


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

resource "google_dataflow_flex_template_job" "sourcedb_to_spanner_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Sourcedb_to_Spanner_Flex"
  name              = "sourcedb-to-spanner-flex"
  region            = var.region
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    projectId = "<projectId>"
    sourceConfigURL = "jdbc:mysql://some-host:3306/sampledb"
    username = "<username>"
    password = "<password>"
    outputDirectory = "gs://your-bucket/dir"

    # jdbcDriverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    # jdbcDriverClassName = "com.mysql.jdbc.Driver"
    # tables = "<tables>"
    # numPartitions = "<numPartitions>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # maxConnections = 100
    # disabledAlgorithms = "SSLv3, RC4"
    # sessionFilePath = "gs://your-bucket/file.txt"
  }
}
```
