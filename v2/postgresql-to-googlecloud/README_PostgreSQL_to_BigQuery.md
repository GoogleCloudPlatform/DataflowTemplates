
PostgreSQL to BigQuery template
---
The PostgreSQL to BigQuery template is a batch pipeline that copies data from a
PostgreSQL table into an existing BigQuery table. This pipeline uses JDBC to
connect to PostgreSQL. For an extra layer of protection, you can also pass in a
Cloud KMS key along with Base64-encoded username, password, and connection string
parameters encrypted with the Cloud KMS key. For more information about
encrypting your username, password, and connection string parameters, see the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a>.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/postgresql-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=PostgreSQL_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **connectionURL** : The JDBC connection URL string. Can be passed in as a string that's Base64-encoded and then encrypted with a Cloud KMS key. (Example: jdbc:postgresql://some-host:5432/sampledb).
* **outputTable** : The BigQuery output table location. (Example: <PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>).
* **bigQueryLoadingTemporaryDirectory** : The temporary directory for the BigQuery loading process. (Example: gs://your-bucket/your-files/temp_dir).

### Optional parameters

* **connectionProperties** : The properties string to use for the JDBC connection. The format of the string must be `[propertyName=property;]*`.For more information, see Configuration Properties (https://dev.mysql.com/doc/connector-j/8.1/en/connector-j-reference-configuration-properties.html) in the MySQL documentation. (Example: unicode=true;characterEncoding=UTF-8).
* **username** : The username to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string.
* **password** : The password to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string.
* **query** : The query to run on the source to extract the data. Note that some JDBC SQL and BigQuery types, although sharing the same name, have some differences. Some important SQL -> BigQuery type mappings to keep in mind are `DATETIME --> TIMESTAMP`. Type casting may be required if your schemas do not match. (Example: select * from sampledb.sample_table).
* **KMSEncryptionKey** : The Cloud KMS encryption key to use to decrypt the username, password, and connection string. If you  pass in a Cloud KMS key, you must also encrypt the username, password, and connection string. (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **useColumnAlias** : If set to `true`, the pipeline uses the column alias (`AS`) instead of the column name to map the rows to BigQuery. Defaults to `false`.
* **isTruncate** : If set to `true`, the pipeline truncates before loading data into BigQuery. Defaults to `false`, which causes the pipeline to append data.
* **partitionColumn** : If this parameter is provided with the name of the `table` defined as an optional parameter, JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only supports `Long` partition columns.
* **table** : The table to read from when using partitions. This parameter also accepts a subquery in parentheses. (Example: (select id, name from Person) as subq).
* **numPartitions** : The number of partitions. With the lower and upper bound, this value forms partition strides for generated `WHERE` clause expressions that are used to split the partition column evenly. When the input is less than `1`, the number is set to `1`.
* **lowerBound** : The lower bound to use in the partition scheme. If not provided, this value is automatically inferred by Apache Beam for the supported types.
* **upperBound** : The upper bound to use in the partition scheme. If not provided, this value is automatically inferred by Apache Beam for the supported types.
* **fetchSize** : The number of rows to be fetched from database at a time. Not used for partitioned reads. Defaults to: 50000.
* **createDisposition** : The BigQuery CreateDisposition to use. For example, `CREATE_IF_NEEDED` or `CREATE_NEVER`. Defaults to: CREATE_NEVER.
* **bigQuerySchemaPath** : The Cloud Storage path for the BigQuery JSON schema. If `createDisposition` is set to `CREATE_IF_NEEDED`, this parameter must be specified. (Example: gs://your-bucket/your-schema.json).
* **disabledAlgorithms** : Comma separated algorithms to disable. If this value is set to `none`, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** : Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. (Example: gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>).
* **useStorageWriteApi** : If `true`, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **useStorageWriteApiAtLeastOnce** : When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/postgresql-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/PostgreSQLToBigQuery.java)

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
-DtemplateName="PostgreSQL_to_BigQuery" \
-f v2/postgresql-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PostgreSQL_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PostgreSQL_to_BigQuery"

### Required
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
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

gcloud dataflow flex-template run "postgresql-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
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
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="postgresql-to-bigquery-job" \
-DtemplateName="PostgreSQL_to_BigQuery" \
-Dparameters="connectionURL=$CONNECTION_URL,connectionProperties=$CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,query=$QUERY,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,KMSEncryptionKey=$KMSENCRYPTION_KEY,useColumnAlias=$USE_COLUMN_ALIAS,isTruncate=$IS_TRUNCATE,partitionColumn=$PARTITION_COLUMN,table=$TABLE,numPartitions=$NUM_PARTITIONS,lowerBound=$LOWER_BOUND,upperBound=$UPPER_BOUND,fetchSize=$FETCH_SIZE,createDisposition=$CREATE_DISPOSITION,bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
-f v2/postgresql-to-googlecloud
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
cd v2/postgresql-to-googlecloud/terraform/PostgreSQL_to_BigQuery
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

resource "google_dataflow_flex_template_job" "postgresql_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PostgreSQL_to_BigQuery"
  name              = "postgresql-to-bigquery"
  region            = var.region
  parameters        = {
    connectionURL = "jdbc:postgresql://some-host:5432/sampledb"
    outputTable = "<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>"
    bigQueryLoadingTemporaryDirectory = "gs://your-bucket/your-files/temp_dir"
    # connectionProperties = "unicode=true;characterEncoding=UTF-8"
    # username = "<username>"
    # password = "<password>"
    # query = "select * from sampledb.sample_table"
    # KMSEncryptionKey = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key"
    # useColumnAlias = "false"
    # isTruncate = "false"
    # partitionColumn = "<partitionColumn>"
    # table = "(select id, name from Person) as subq"
    # numPartitions = "<numPartitions>"
    # lowerBound = "<lowerBound>"
    # upperBound = "<upperBound>"
    # fetchSize = "50000"
    # createDisposition = "CREATE_NEVER"
    # bigQuerySchemaPath = "gs://your-bucket/your-schema.json"
    # disabledAlgorithms = "SSLv3, RC4"
    # extraFilesToStage = "gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>"
    # useStorageWriteApi = "false"
    # useStorageWriteApiAtLeastOnce = "false"
  }
}
```
