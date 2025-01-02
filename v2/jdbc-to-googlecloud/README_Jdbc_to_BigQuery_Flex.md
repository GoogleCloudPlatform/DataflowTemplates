
JDBC to BigQuery with BigQuery Storage API support template
---
The JDBC to BigQuery template is a batch pipeline that copies data from a
relational database table into an existing BigQuery table. This pipeline uses
JDBC to connect to the relational database. You can use this template to copy
data from any relational database with available JDBC drivers into BigQuery.

For an extra layer of protection, you can also pass in a Cloud KMS key along with
a Base64-encoded username, password, and connection string parameters encrypted
with the Cloud KMS key. See the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a> for additional details on encrypting your
username, password, and connection string parameters.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Jdbc_to_BigQuery_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **driverJars**: The comma-separated list of driver JAR files. For example, `gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar`.
* **driverClassName**: The JDBC driver class name. For example, `com.mysql.jdbc.Driver`.
* **connectionURL**: The JDBC connection URL string. For example, `jdbc:mysql://some-host:3306/sampledb`. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string. Note the difference between an Oracle non-RAC database connection string (`jdbc:oracle:thin:@some-host:<port>:<sid>`) and an Oracle RAC database connection string (`jdbc:oracle:thin:@//some-host[:<port>]/<service_name>`). For example, `jdbc:mysql://some-host:3306/sampledb`.
* **outputTable**: The BigQuery output table location. For example, `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`.
* **bigQueryLoadingTemporaryDirectory**: The temporary directory for the BigQuery loading process. For example, `gs://your-bucket/your-files/temp_dir`.

### Optional parameters

* **connectionProperties**: The properties string to use for the JDBC connection. The format of the string must be `[propertyName=property;]*`.For more information, see Configuration Properties (https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html) in the MySQL documentation. For example, `unicode=true;characterEncoding=UTF-8`.
* **username**: The username to use for the JDBC connection. Can be passed in as a string that's encrypted with a Cloud KMS key, or can be a Secret Manager secret in the form projects/{project}/secrets/{secret}/versions/{secret_version}.
* **password**: The password to use for the JDBC connection. Can be passed in as a string that's encrypted with a Cloud KMS key, or can be a Secret Manager secret in the form projects/{project}/secrets/{secret}/versions/{secret_version}.
* **query**: The query to run on the source to extract the data. Note that some JDBC SQL and BigQuery types, although sharing the same name, have some differences. Some important SQL -> BigQuery type mappings to keep in mind are `DATETIME --> TIMESTAMP`. Type casting may be required if your schemas do not match. For example, `select * from sampledb.sample_table`.
* **KMSEncryptionKey**: The Cloud KMS encryption key to use to decrypt the username, password, and connection string. If you  pass in a Cloud KMS key, you must also encrypt the username, password, and connection string. For example, `projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key`.
* **useColumnAlias**: If set to `true`, the pipeline uses the column alias (`AS`) instead of the column name to map the rows to BigQuery. Defaults to `false`.
* **isTruncate**: If set to `true`, the pipeline truncates before loading data into BigQuery. Defaults to `false`, which causes the pipeline to append data.
* **partitionColumn**: If this parameter is provided with the name of the `table` defined as an optional parameter, JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only supports `Long` partition columns.
* **table**: The table to read from when using partitions. This parameter also accepts a subquery in parentheses. For example, `(select id, name from Person) as subq`.
* **numPartitions**: The number of partitions. With the lower and upper bound, this value forms partition strides for generated `WHERE` clause expressions that are used to split the partition column evenly. When the input is less than `1`, the number is set to `1`.
* **lowerBound**: The lower bound to use in the partition scheme. If not provided, this value is automatically inferred by Apache Beam for the supported types.
* **upperBound**: The upper bound to use in the partition scheme. If not provided, this value is automatically inferred by Apache Beam for the supported types.
* **fetchSize**: The number of rows to be fetched from database at a time. Not used for partitioned reads. Defaults to: 50000.
* **createDisposition**: The BigQuery CreateDisposition to use. For example, `CREATE_IF_NEEDED` or `CREATE_NEVER`. Defaults to: CREATE_NEVER.
* **bigQuerySchemaPath**: The Cloud Storage path for the BigQuery JSON schema. If `createDisposition` is set to `CREATE_IF_NEEDED`, this parameter must be specified. For example, `gs://your-bucket/your-schema.json`.
* **outputDeadletterTable**: The BigQuery table to use for messages that failed to reach the output table, formatted as `"PROJECT_ID:DATASET_NAME.TABLE_NAME"`. If the table doesn't exist, it is created when the pipeline runs. If this parameter is not specified, the pipeline will fail on write errors.This parameter can only be specified if `useStorageWriteApi` or `useStorageWriteApiAtLeastOnce` is set to true.
* **disabledAlgorithms**: Comma separated algorithms to disable. If this value is set to `none`, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. For example, `SSLv3, RC4`.
* **extraFilesToStage**: Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. For example, `gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>`.
* **useStorageWriteApi**: If `true`, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce**: When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/jdbc-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToBigQuery.java)

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
-DtemplateName="Jdbc_to_BigQuery_Flex" \
-f v2/jdbc-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Jdbc_to_BigQuery_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Jdbc_to_BigQuery_Flex"

### Required
export DRIVER_JARS=<driverJars>
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionURL>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export CONNECTION_PROPERTIES=<connectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export QUERY=<query>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export USE_COLUMN_ALIAS=false
export IS_TRUNCATE=false
export PARTITION_COLUMN=<partitionColumn>
export TABLE=<table>
export NUM_PARTITIONS=<numPartitions>
export LOWER_BOUND=<lowerBound>
export UPPER_BOUND=<upperBound>
export FETCH_SIZE=50000
export CREATE_DISPOSITION=CREATE_NEVER
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

gcloud dataflow flex-template run "jdbc-to-bigquery-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "driverJars=$DRIVER_JARS" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "connectionURL=$CONNECTION_URL" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "query=$QUERY" \
  --parameters "outputTable=$OUTPUT_TABLE" \
  --parameters "bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY" \
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY" \
  --parameters "useColumnAlias=$USE_COLUMN_ALIAS" \
  --parameters "isTruncate=$IS_TRUNCATE" \
  --parameters "partitionColumn=$PARTITION_COLUMN" \
  --parameters "table=$TABLE" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "lowerBound=$LOWER_BOUND" \
  --parameters "upperBound=$UPPER_BOUND" \
  --parameters "fetchSize=$FETCH_SIZE" \
  --parameters "createDisposition=$CREATE_DISPOSITION" \
  --parameters "bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH" \
  --parameters "outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE" \
  --parameters "disabledAlgorithms=$DISABLED_ALGORITHMS" \
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE"
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
export DRIVER_JARS=<driverJars>
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionURL>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export CONNECTION_PROPERTIES=<connectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export QUERY=<query>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export USE_COLUMN_ALIAS=false
export IS_TRUNCATE=false
export PARTITION_COLUMN=<partitionColumn>
export TABLE=<table>
export NUM_PARTITIONS=<numPartitions>
export LOWER_BOUND=<lowerBound>
export UPPER_BOUND=<upperBound>
export FETCH_SIZE=50000
export CREATE_DISPOSITION=CREATE_NEVER
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export OUTPUT_DEADLETTER_TABLE=<outputDeadletterTable>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="jdbc-to-bigquery-flex-job" \
-DtemplateName="Jdbc_to_BigQuery_Flex" \
-Dparameters="driverJars=$DRIVER_JARS,driverClassName=$DRIVER_CLASS_NAME,connectionURL=$CONNECTION_URL,connectionProperties=$CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,query=$QUERY,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,KMSEncryptionKey=$KMSENCRYPTION_KEY,useColumnAlias=$USE_COLUMN_ALIAS,isTruncate=$IS_TRUNCATE,partitionColumn=$PARTITION_COLUMN,table=$TABLE,numPartitions=$NUM_PARTITIONS,lowerBound=$LOWER_BOUND,upperBound=$UPPER_BOUND,fetchSize=$FETCH_SIZE,createDisposition=$CREATE_DISPOSITION,bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH,outputDeadletterTable=$OUTPUT_DEADLETTER_TABLE,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
-f v2/jdbc-to-googlecloud
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
cd v2/jdbc-to-googlecloud/terraform/Jdbc_to_BigQuery_Flex
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

resource "google_dataflow_flex_template_job" "jdbc_to_bigquery_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Jdbc_to_BigQuery_Flex"
  name              = "jdbc-to-bigquery-flex"
  region            = var.region
  parameters        = {
    driverJars = "<driverJars>"
    driverClassName = "<driverClassName>"
    connectionURL = "<connectionURL>"
    outputTable = "<outputTable>"
    bigQueryLoadingTemporaryDirectory = "<bigQueryLoadingTemporaryDirectory>"
    # connectionProperties = "<connectionProperties>"
    # username = "<username>"
    # password = "<password>"
    # query = "<query>"
    # KMSEncryptionKey = "<KMSEncryptionKey>"
    # useColumnAlias = "false"
    # isTruncate = "false"
    # partitionColumn = "<partitionColumn>"
    # table = "<table>"
    # numPartitions = "<numPartitions>"
    # lowerBound = "<lowerBound>"
    # upperBound = "<upperBound>"
    # fetchSize = "50000"
    # createDisposition = "CREATE_NEVER"
    # bigQuerySchemaPath = "<bigQuerySchemaPath>"
    # outputDeadletterTable = "<outputDeadletterTable>"
    # disabledAlgorithms = "<disabledAlgorithms>"
    # extraFilesToStage = "<extraFilesToStage>"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
  }
}
```
