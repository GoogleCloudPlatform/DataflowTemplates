
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

### Required Parameters

* **driverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **driverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **connectionURL** (JDBC connection URL string.): The JDBC connection URL string. For example, `jdbc:mysql://some-host:3306/sampledb`. Can be passed in as a string that's Base64-encoded and then encrypted with a Cloud KMS key. Note the difference between an Oracle non-RAC database connection string (`jdbc:oracle:thin:@some-host:<port>:<sid>`) and an Oracle RAC database connection string (`jdbc:oracle:thin:@//some-host[:<port>]/<service_name>`). (Example: jdbc:mysql://some-host:3306/sampledb).
* **outputTable** (BigQuery output table): BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects. (Example: <my-project>:<my-dataset>.<my-table>).
* **bigQueryLoadingTemporaryDirectory** (Temporary directory for BigQuery loading process): The temporary directory for the BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).

### Optional Parameters

* **connectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **username** (JDBC connection username.): The username to be used for the JDBC connection. Can be passed in as a Base64-encoded string encrypted with a Cloud KMS key.
* **password** (JDBC connection password.): The password to be used for the JDBC connection. Can be passed in as a Base64-encoded string encrypted with a Cloud KMS key.
* **query** (JDBC source SQL query): The query to be run on the source to extract the data. (Example: select * from sampledb.sample_table).
* **KMSEncryptionKey** (Google Cloud KMS key): Cloud KMS Encryption Key to decrypt the username, password, and connection string. If Cloud KMS key is passed in, the username, password, and connection string must all be passed in encrypted. (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **useColumnAlias** (Whether to use column alias to map the rows.): If enabled (set to true) the pipeline will consider column alias ("AS") instead of the column name to map the rows to BigQuery. Defaults to false.
* **isTruncate** (Whether to truncate data before writing): If enabled (set to true) the pipeline will truncate before loading data into BigQuery. Defaults to false, which is used to only append data.
* **partitionColumn** (The name of a column of numeric type that will be used for partitioning.): If this parameter is provided (along with `table`), JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only Long partition columns are supported.
* **table** (Name of the table in the external database.): Table to read from using partitions. This parameter also accepts a subquery in parentheses. (Example: (select id, name from Person) as subq).
* **numPartitions** (The number of partitions.): The number of partitions. This, along with the lower and upper bound, form partitions strides for generated WHERE clause expressions used to split the partition column evenly. When the input is less than 1, the number is set to 1.
* **lowerBound** (Lower bound of partition column.): Lower bound used in the partition scheme. If not provided, it is automatically inferred by Beam (for the supported types).
* **upperBound** (Upper bound of partition column): Upper bound used in partition scheme. If not provided, it is automatically inferred by Beam (for the supported types).
* **disabledAlgorithms** (Disabled algorithms to override jdk.tls.disabledAlgorithms): Comma separated algorithms to disable. If this value is set to `none` then no algorithm is disabled. Use with care, as the algorithms disabled by default are known to have either vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** (Extra files to stage in the workers): Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files will be saved under the `/extra_files` directory in each worker (Example: gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id).
* **useStorageWriteApi** (Use BigQuery Storage Write API): If enabled (set to true) the pipeline will use Storage Write API when writing the data to BigQuery (see https://cloud.google.com/blog/products/data-analytics/streaming-data-into-bigquery-using-storage-write-api). Defaults to: false.
* **useStorageWriteApiAtLeastOnce** (Use at at-least-once semantics in BigQuery Storage Write API): This parameter takes effect only if "Use BigQuery Storage Write API" is enabled. If enabled the at-least-once semantics will be used for Storage Write API, otherwise exactly-once semantics will be used. Defaults to: false.



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
-DtemplateName="Jdbc_to_BigQuery_Flex" \
-pl v2/jdbc-to-googlecloud \
-am
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
-Dparameters="driverJars=$DRIVER_JARS,driverClassName=$DRIVER_CLASS_NAME,connectionURL=$CONNECTION_URL,connectionProperties=$CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,query=$QUERY,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,KMSEncryptionKey=$KMSENCRYPTION_KEY,useColumnAlias=$USE_COLUMN_ALIAS,isTruncate=$IS_TRUNCATE,partitionColumn=$PARTITION_COLUMN,table=$TABLE,numPartitions=$NUM_PARTITIONS,lowerBound=$LOWER_BOUND,upperBound=$UPPER_BOUND,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
-pl v2/jdbc-to-googlecloud \
-am
```
