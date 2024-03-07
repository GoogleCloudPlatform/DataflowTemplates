
Sourcedb to Spanner template
---
The SourceDB to Spanner template is a batch pipeline that copies data from a
relational database into an existing Spanner database. This pipeline uses JDBC to
connect to the relational database. You can use this template to copy data from
any relational database with available JDBC drivers into Spanner. This currently
only supports a limited set of types of MySQL.

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

### Required Parameters

* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.

### Optional Parameters

* **jdbcDriverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **jdbcDriverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **sourceConnectionURL** (Connection URL to connect to the source database.): The JDBC connection URL string. For example, `jdbc:mysql://some-host:3306/sampledb`. Can be passed in as a string that's Base64-encoded and then encrypted with a Cloud KMS key. Currently supported sources: MySQL (Example: jdbc:mysql://some-host:3306/sampledb).
* **sourceConnectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **username** (JDBC connection username.): The username to be used for the JDBC connection. Can be passed in as a Base64-encoded string encrypted with a Cloud KMS key.
* **password** (JDBC connection password.): The password to be used for the JDBC connection. Can be passed in as a Base64-encoded string encrypted with a Cloud KMS key.
* **partitionColumns** (The name of a column of numeric type that will be used for partitioning.): If this parameter is provided (along with `table`), JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only Long partition columns are supported. The partition columns are expected to be the same in number as the tables.
* **tables** (Comma-separated names of the tables in the source database.): Tables to read from using partitions.
* **numPartitions** (The number of partitions.): The number of partitions. This, along with the lower and upper bound, form partitions strides for generated WHERE clause expressions used to split the partition column evenly. When the input is less than 1, the number is set to 1.
* **spannerHost** (Cloud Spanner Endpoint to call): The Cloud Spanner endpoint to call in the template. (Example: https://batch-spanner.googleapis.com). Defaults to: https://batch-spanner.googleapis.com.
* **ignoreColumns** (Source database columns to ignore): A comma separated list of (table:column1;column2) to exclude from writing to Spanner (Example: table1:column1;column2,table2:column1).
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

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/SourceDbToSpanner.java)

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
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-f v2/sourcedb-to-spanner
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
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>

### Optional
export JDBC_DRIVER_JARS=<jdbcDriverJars>
export JDBC_DRIVER_CLASS_NAME=<jdbcDriverClassName>
export SOURCE_CONNECTION_URL=<sourceConnectionURL>
export SOURCE_CONNECTION_PROPERTIES=<sourceConnectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export PARTITION_COLUMNS=<partitionColumns>
export TABLES=<tables>
export NUM_PARTITIONS=<numPartitions>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export IGNORE_COLUMNS=<ignoreColumns>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow flex-template run "sourcedb-to-spanner-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "jdbcDriverJars=$JDBC_DRIVER_JARS" \
  --parameters "jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME" \
  --parameters "sourceConnectionURL=$SOURCE_CONNECTION_URL" \
  --parameters "sourceConnectionProperties=$SOURCE_CONNECTION_PROPERTIES" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "partitionColumns=$PARTITION_COLUMNS" \
  --parameters "tables=$TABLES" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "ignoreColumns=$IGNORE_COLUMNS" \
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
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>

### Optional
export JDBC_DRIVER_JARS=<jdbcDriverJars>
export JDBC_DRIVER_CLASS_NAME=<jdbcDriverClassName>
export SOURCE_CONNECTION_URL=<sourceConnectionURL>
export SOURCE_CONNECTION_PROPERTIES=<sourceConnectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export PARTITION_COLUMNS=<partitionColumns>
export TABLES=<tables>
export NUM_PARTITIONS=<numPartitions>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export IGNORE_COLUMNS=<ignoreColumns>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="sourcedb-to-spanner-flex-job" \
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-Dparameters="jdbcDriverJars=$JDBC_DRIVER_JARS,jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME,sourceConnectionURL=$SOURCE_CONNECTION_URL,sourceConnectionProperties=$SOURCE_CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,partitionColumns=$PARTITION_COLUMNS,tables=$TABLES,numPartitions=$NUM_PARTITIONS,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,projectId=$PROJECT_ID,spannerHost=$SPANNER_HOST,ignoreColumns=$IGNORE_COLUMNS,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-f v2/sourcedb-to-spanner
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
cd v2/sourcedb-to-spanner/terraform/Sourcedb_to_Spanner_Flex
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

resource "google_dataflow_flex_template_job" "sourcedb_to_spanner_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Sourcedb_to_Spanner_Flex"
  name              = "sourcedb-to-spanner-flex"
  region            = var.region
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    projectId = "<projectId>"
    # jdbcDriverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    # jdbcDriverClassName = "com.mysql.jdbc.Driver"
    # sourceConnectionURL = "jdbc:mysql://some-host:3306/sampledb"
    # sourceConnectionProperties = "unicode=true;characterEncoding=UTF-8"
    # username = "<username>"
    # password = "<password>"
    # partitionColumns = "<partitionColumns>"
    # tables = "<tables>"
    # numPartitions = "<numPartitions>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # ignoreColumns = "table1:column1;column2,table2:column1"
    # disabledAlgorithms = "SSLv3, RC4"
    # extraFilesToStage = "gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id"
  }
}
```
