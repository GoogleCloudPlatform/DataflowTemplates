
Iceberg to SqlServer (YAML) template
---
The Iceberg to SqlServer template is a batch pipeline that reads data from an
Iceberg table and outputs the records to a SqlServer database table.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **table**: A fully-qualified table identifier, e.g., my_dataset.my_table. For example, `my_dataset.my_table`.
* **catalogName**: The name of the Iceberg catalog that contains the table. For example, `my_hadoop_catalog`.
* **catalogProperties**: A map of properties for setting up the Iceberg catalog. For example, `{"type": "hadoop", "warehouse": "gs://your-bucket/warehouse"}`.
* **jdbcUrl**: The JDBC connection URL. For example, `jdbc:sqlserver://localhost:12345;databaseName=your-db`.
* **location**: The name of the database table to write data to. For example, `public.my_destination_table`.

### Optional parameters

* **configProperties**: A map of properties to pass to the Hadoop Configuration. For example, `{"fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"}`.
* **drop**: A list of field names to drop. Mutually exclusive with 'keep' and 'only'. For example, `["field_to_drop_1", "field_to_drop_2"]`.
* **filter**: A filter expression to apply to records from the Iceberg table. For example, `age > 18`.
* **keep**: A list of field names to keep. Mutually exclusive with 'drop' and 'only'. For example, `["field_to_keep_1", "field_to_keep_2"]`.
* **username**: The database username. For example, `my_user`.
* **password**: The database password. For example, `my_secret_password`.
* **driverClassName**: The fully-qualified class name of the JDBC driver to use. For example, `com.microsoft.sqlserver.jdbc.SQLServerDriver`. Defaults to: com.microsoft.sqlserver.jdbc.SQLServerDriver.
* **driverJars**: A comma-separated list of GCS paths to the JDBC driver JAR files. For example, `gs://your-bucket/mssql-jdbc-12.2.0.jre11.jar`.
* **connectionProperties**: A semicolon-separated list of key-value pairs for the JDBC connection. For example, `key1=value1;key2=value2`.
* **connectionInitSql**: A list of SQL statements to execute when a new connection is established. For example, `["SET TIME ZONE UTC"]`.
* **jdbcType**: Specifies the type of JDBC source. An appropriate default driver will be packaged. For example, `mssql`.
* **writeStatement**: The SQL query for inserting records, with placeholders for values. For example, `INSERT INTO my_table (col1, col2) VALUES(?, ?)`.
* **batchSize**: The number of records to group together for each write. For example, `1000`.
* **autosharding**: If true, a dynamic number of shards will be used for writing. For example, `False`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/IcebergToSqlServerYaml.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl yaml
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
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="Iceberg_To_SqlServer_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Iceberg_To_SqlServer_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Iceberg_To_SqlServer_Yaml"

### Required
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export JDBC_URL=<jdbcUrl>
export LOCATION=<location>

### Optional
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export FILTER=<filter>
export KEEP=<keep>
export USERNAME=<username>
export PASSWORD=<password>
export DRIVER_CLASS_NAME=com.microsoft.sqlserver.jdbc.SQLServerDriver
export DRIVER_JARS=<driverJars>
export CONNECTION_PROPERTIES=<connectionProperties>
export CONNECTION_INIT_SQL=<connectionInitSql>
export JDBC_TYPE=mssql
export WRITE_STATEMENT=<writeStatement>
export BATCH_SIZE=<batchSize>
export AUTOSHARDING=<autosharding>

gcloud dataflow flex-template run "iceberg-to-sqlserver-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "table=$TABLE" \
  --parameters "catalogName=$CATALOG_NAME" \
  --parameters "catalogProperties=$CATALOG_PROPERTIES" \
  --parameters "configProperties=$CONFIG_PROPERTIES" \
  --parameters "drop=$DROP" \
  --parameters "filter=$FILTER" \
  --parameters "keep=$KEEP" \
  --parameters "jdbcUrl=$JDBC_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "driverJars=$DRIVER_JARS" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "connectionInitSql=$CONNECTION_INIT_SQL" \
  --parameters "jdbcType=$JDBC_TYPE" \
  --parameters "location=$LOCATION" \
  --parameters "writeStatement=$WRITE_STATEMENT" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "autosharding=$AUTOSHARDING"
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
export TABLE=<table>
export CATALOG_NAME=<catalogName>
export CATALOG_PROPERTIES=<catalogProperties>
export JDBC_URL=<jdbcUrl>
export LOCATION=<location>

### Optional
export CONFIG_PROPERTIES=<configProperties>
export DROP=<drop>
export FILTER=<filter>
export KEEP=<keep>
export USERNAME=<username>
export PASSWORD=<password>
export DRIVER_CLASS_NAME=com.microsoft.sqlserver.jdbc.SQLServerDriver
export DRIVER_JARS=<driverJars>
export CONNECTION_PROPERTIES=<connectionProperties>
export CONNECTION_INIT_SQL=<connectionInitSql>
export JDBC_TYPE=mssql
export WRITE_STATEMENT=<writeStatement>
export BATCH_SIZE=<batchSize>
export AUTOSHARDING=<autosharding>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="iceberg-to-sqlserver-yaml-job" \
-DtemplateName="Iceberg_To_SqlServer_Yaml" \
-Dparameters="table=$TABLE,catalogName=$CATALOG_NAME,catalogProperties=$CATALOG_PROPERTIES,configProperties=$CONFIG_PROPERTIES,drop=$DROP,filter=$FILTER,keep=$KEEP,jdbcUrl=$JDBC_URL,username=$USERNAME,password=$PASSWORD,driverClassName=$DRIVER_CLASS_NAME,driverJars=$DRIVER_JARS,connectionProperties=$CONNECTION_PROPERTIES,connectionInitSql=$CONNECTION_INIT_SQL,jdbcType=$JDBC_TYPE,location=$LOCATION,writeStatement=$WRITE_STATEMENT,batchSize=$BATCH_SIZE,autosharding=$AUTOSHARDING" \
-f yaml
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
cd v2/yaml/terraform/Iceberg_To_SqlServer_Yaml
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

resource "google_dataflow_flex_template_job" "iceberg_to_sqlserver_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Iceberg_To_SqlServer_Yaml"
  name              = "iceberg-to-sqlserver-yaml"
  region            = var.region
  parameters        = {
    table = "<table>"
    catalogName = "<catalogName>"
    catalogProperties = "<catalogProperties>"
    jdbcUrl = "<jdbcUrl>"
    location = "<location>"
    # configProperties = "<configProperties>"
    # drop = "<drop>"
    # filter = "<filter>"
    # keep = "<keep>"
    # username = "<username>"
    # password = "<password>"
    # driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # driverJars = "<driverJars>"
    # connectionProperties = "<connectionProperties>"
    # connectionInitSql = "<connectionInitSql>"
    # jdbcType = "mssql"
    # writeStatement = "<writeStatement>"
    # batchSize = <batchSize>
    # autosharding = "<autosharding>"
  }
}
```
