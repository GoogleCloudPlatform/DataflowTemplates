JDBC to BigQuery with BigQuery Storage API support Template
---
A pipeline that reads from a JDBC source and writes to a BigQuery table. JDBC connection string, user name and password can be passed in directly as plaintext or encrypted using the Google Cloud KMS API.  If the parameter KMSEncryptionKey is specified, connectionURL, username, and password should be all in encrypted format.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Jdbc_to_BigQuery_Flex).

A sample curl command for the KMS API encrypt endpoint: curl -s -X POST "https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt"  -d "{\"plaintext\":\"PasteBase64EncodedString\"}" -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" -H "Content-Type: application/json".

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **driverJars** (Cloud Storage paths for JDBC drivers): Comma separate Cloud Storage paths for JDBC drivers. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **driverClassName** (JDBC driver class name.): JDBC driver class name to use. (Example: com.mysql.jdbc.Driver).
* **connectionURL** (JDBC connection URL string.): Url connection string to connect to the JDBC source. Connection string can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS. (Example: jdbc:mysql://some-host:3306/sampledb).
* **query** (JDBC source SQL query.): Query to be executed on the source to extract the data. (Example: select * from sampledb.sample_table).
* **outputTable** (BigQuery output table): BigQuery table location to write the output to. The name should be in the format <project>:<dataset>.<table_name>. The table's schema must match input objects.
* **bigQueryLoadingTemporaryDirectory** (Temporary directory for BigQuery loading process): Temporary directory for BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).

### Optional Parameters

* **connectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **username** (JDBC connection username.): User name to be used for the JDBC connection. User name can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **password** (JDBC connection password.): Password to be used for the JDBC connection. Password can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **KMSEncryptionKey** (Google Cloud KMS key): If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **useColumnAlias** (Whether to use column alias to map the rows.): If enabled (set to true) the pipeline will consider column alias ("AS") instead of the column name to map the rows to BigQuery. Defaults to false.
* **isTruncate** (Whether to truncate data before writing): If enabled (set to true) the pipeline will truncate before loading data into BigQuery. Defaults to false, which is used to only append data.
* **disabledAlgorithms** (Disabled algorithms to override jdk.tls.disabledAlgorithms): Comma separated algorithms to disable. If this value is set to "none" then dk.tls.disabledAlgorithms is set to "". Use with care, as the algorithms disabled by default are known to have either vulnerabilities or performance issues. For example: SSLv3, RC4.
* **extraFilesToStage** (Extra files to stage in the workers): Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files will be saved under the `/extra_files` directory in each worker. (Example: gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id).
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
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToBigQuery.java)

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
-pl v2/googlecloud-to-googlecloud \
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
export QUERY=<query>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export CONNECTION_PROPERTIES=<connectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export USE_COLUMN_ALIAS=false
export IS_TRUNCATE=false
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
export QUERY=<query>
export OUTPUT_TABLE=<outputTable>
export BIG_QUERY_LOADING_TEMPORARY_DIRECTORY=<bigQueryLoadingTemporaryDirectory>

### Optional
export CONNECTION_PROPERTIES=<connectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export USE_COLUMN_ALIAS=false
export IS_TRUNCATE=false
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
-Dparameters="driverJars=$DRIVER_JARS,driverClassName=$DRIVER_CLASS_NAME,connectionURL=$CONNECTION_URL,connectionProperties=$CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,query=$QUERY,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,KMSEncryptionKey=$KMSENCRYPTION_KEY,useColumnAlias=$USE_COLUMN_ALIAS,isTruncate=$IS_TRUNCATE,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
-pl v2/googlecloud-to-googlecloud \
-am
```
