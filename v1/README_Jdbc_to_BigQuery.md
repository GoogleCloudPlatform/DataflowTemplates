
JDBC to BigQuery (Legacy) template
---
The JDBC to BigQuery template is a batch pipeline that copies data from a
relational database table into an existing BigQuery table. This pipeline uses
JDBC to connect to the relational database. You can use this template to copy
data from any relational database with available JDBC drivers into BigQuery. For
an extra layer of protection, you can also pass in a Cloud KMS key along with a
Base64-encoded username, password, and connection string parameters encrypted
with the Cloud KMS key. See the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a> for additional details on encrypting your
username, password, and connection string parameters.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Jdbc_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **driverJars** (Cloud Storage paths for JDBC drivers): Comma separate Cloud Storage paths for JDBC drivers. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **driverClassName** (JDBC driver class name.): JDBC driver class name to use. (Example: com.mysql.jdbc.Driver).
* **connectionURL** (JDBC connection URL string. Connection string can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.): Url connection string to connect to the JDBC source. (Example: jdbc:mysql://some-host:3306/sampledb).
* **query** (JDBC source SQL query.): Query to be executed on the source to extract the data. If a Cloud Storage path is given (gs://...), the query will be fetched from that file. (Example: select * from sampledb.sample_table).
* **outputTable** (BigQuery output table): BigQuery table location to write the output to. The table's schema must match the input objects.
* **bigQueryLoadingTemporaryDirectory** (Temporary directory for BigQuery loading process): Temporary directory for BigQuery loading process (Example: gs://your-bucket/your-files/temp_dir).

### Optional Parameters

* **connectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **username** (JDBC connection username.): User name to be used for the JDBC connection. User name can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **password** (JDBC connection password.): Password to be used for the JDBC connection. Password can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **KMSEncryptionKey** (Google Cloud KMS key): If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **useColumnAlias** (Whether to use column alias to map the rows.): If enabled (set to true) the pipeline will consider column alias ("AS") instead of the column name to map the rows to BigQuery. Defaults to false.
* **disabledAlgorithms** (Disabled algorithms to override jdk.tls.disabledAlgorithms): Comma-separated algorithms to disable. If this value is set to `none` then no algorithm is disabled. Use with care, because the algorithms that are disabled by default are known to have either vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** (Extra files to stage in the workers): Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files will be saved under the `/extra_files` directory in each worker. (Example: gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/templates/JdbcToBigQuery.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command before proceeding:

```shell
mvn clean install -pl plugins/templates-maven-plugin -am
```

### Building Template

This template is a Classic Template, meaning that the pipeline code will be
executed only once and the pipeline will be saved to Google Cloud Storage for
further reuse. Please check [Creating classic Dataflow templates](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates)
and [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)
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
-DtemplateName="Jdbc_to_BigQuery" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Jdbc_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Jdbc_to_BigQuery"

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
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow jobs run "jdbc-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
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
  --parameters "disabledAlgorithms=$DISABLED_ALGORITHMS" \
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/jobs/run


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
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="jdbc-to-bigquery-job" \
-DtemplateName="Jdbc_to_BigQuery" \
-Dparameters="driverJars=$DRIVER_JARS,driverClassName=$DRIVER_CLASS_NAME,connectionURL=$CONNECTION_URL,connectionProperties=$CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,query=$QUERY,outputTable=$OUTPUT_TABLE,bigQueryLoadingTemporaryDirectory=$BIG_QUERY_LOADING_TEMPORARY_DIRECTORY,KMSEncryptionKey=$KMSENCRYPTION_KEY,useColumnAlias=$USE_COLUMN_ALIAS,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-pl v1 \
-am
```
