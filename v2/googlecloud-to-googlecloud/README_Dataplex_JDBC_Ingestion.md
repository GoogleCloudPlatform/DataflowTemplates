
Dataplex JDBC Ingestion template
---
A pipeline that reads from a JDBC source and writes to to a Dataplex asset, which
can be either a BigQuery dataset or a Cloud Storage bucket. JDBC connection
string, user name and password can be passed in directly as plaintext or
encrypted using the Google Cloud KMS API. If the parameter KMSEncryptionKey is
specified, connectionURL, username, and password should be all in encrypted
format. A sample curl command for the KMS API encrypt endpoint: curl -s -X POST
"https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt"
-d "{\"plaintext\":\"PasteBase64EncodedString\"}" -H "Authorization: Bearer
$(gcloud auth application-default print-access-token)" -H "Content-Type:
application/json".


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Dataplex_JDBC_Ingestion).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **connectionURL** (JDBC connection URL string.): Url connection string to connect to the JDBC source. Connection string can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS. (Example: jdbc:mysql://some-host:3306/sampledb).
* **driverClassName** (JDBC driver class name.): JDBC driver class name to use. (Example: com.mysql.jdbc.Driver).
* **driverJars** (Cloud Storage paths for JDBC drivers): Comma separated Cloud Storage paths for JDBC drivers. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **query** (JDBC source SQL query.): Query to be executed on the source to extract the data. (Example: select * from sampledb.sample_table).
* **outputTable** (BigQuery output table or Cloud Storage top folder name): BigQuery table location or Cloud Storage top folder name to write the output to. If it's a BigQuery table location, the table’s schema must match the source query schema and should in the format of some-project-id:somedataset.sometable. If it's a Cloud Storage top folder, just provide the top folder name.
* **outputAsset** (Dataplex output asset ID): Dataplex output asset ID to which the results are stored to. Should be in the format of projects/your-project/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset-name>.

### Optional Parameters

* **connectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **username** (JDBC connection username.): User name to be used for the JDBC connection. User name can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **password** (JDBC connection password.): Password to be used for the JDBC connection. Password can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **KMSEncryptionKey** (Google Cloud KMS key): If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **partitioningScheme** (The partition scheme when writing the file.): The partition scheme when writing the file. Format: DAILY or MONTHLY or HOURLY. Defaults to: DAILY.
* **paritionColumn** (The partition column on which the partition is based.): The partition column on which the partition is based. The column type must be of timestamp/date format.
* **writeDisposition** (BigQuery write disposition type): Strategy to employ if the target file/table exists. If the table exists - should it overwrite/append or fail the load. Format: WRITE_APPEND or WRITE_TRUNCATE or WRITE_EMPTY. Only supported for writing to BigQuery. Defaults to: WRITE_EMPTY.
* **fileFormat** (Output file format in Cloud Storage.): Output file format in Cloud Storage. Format: PARQUET or AVRO. Defaults to: PARQUET.
* **useColumnAlias** (Whether to use column alias to map the rows.): If enabled (set to true) the pipeline will consider column alias ("AS") instead of the column name to map the rows to BigQuery. Defaults to false.
* **updateDataplexMetadata** (Update Dataplex metadata.): Whether to update Dataplex metadata for the newly created entities. Only supported for Cloud Storage destination. If enabled, the pipeline will automatically copy the schema from source to the destination Dataplex entities, and the automated Dataplex Discovery won't run for them. Use this flag in cases where you have managed schema at the source. Defaults to: false.
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
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/DataplexJdbcIngestion.java)

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
-DtemplateName="Dataplex_JDBC_Ingestion" \
-pl v2/googlecloud-to-googlecloud \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Dataplex_JDBC_Ingestion
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Dataplex_JDBC_Ingestion"

### Required
export CONNECTION_URL=<connectionURL>
export DRIVER_CLASS_NAME=<driverClassName>
export DRIVER_JARS=<driverJars>
export QUERY=<query>
export OUTPUT_TABLE=<outputTable>
export OUTPUT_ASSET=<outputAsset>

### Optional
export CONNECTION_PROPERTIES=<connectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export PARTITIONING_SCHEME=DAILY
export PARITION_COLUMN=<paritionColumn>
export WRITE_DISPOSITION=WRITE_EMPTY
export FILE_FORMAT=PARQUET
export USE_COLUMN_ALIAS=false
export UPDATE_DATAPLEX_METADATA=false
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

gcloud dataflow flex-template run "dataplex-jdbc-ingestion-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "connectionURL=$CONNECTION_URL" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "driverJars=$DRIVER_JARS" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "query=$QUERY" \
  --parameters "outputTable=$OUTPUT_TABLE" \
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY" \
  --parameters "outputAsset=$OUTPUT_ASSET" \
  --parameters "partitioningScheme=$PARTITIONING_SCHEME" \
  --parameters "paritionColumn=$PARITION_COLUMN" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "fileFormat=$FILE_FORMAT" \
  --parameters "useColumnAlias=$USE_COLUMN_ALIAS" \
  --parameters "updateDataplexMetadata=$UPDATE_DATAPLEX_METADATA" \
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
export DRIVER_CLASS_NAME=<driverClassName>
export DRIVER_JARS=<driverJars>
export QUERY=<query>
export OUTPUT_TABLE=<outputTable>
export OUTPUT_ASSET=<outputAsset>

### Optional
export CONNECTION_PROPERTIES=<connectionProperties>
export USERNAME=<username>
export PASSWORD=<password>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export PARTITIONING_SCHEME=DAILY
export PARITION_COLUMN=<paritionColumn>
export WRITE_DISPOSITION=WRITE_EMPTY
export FILE_FORMAT=PARQUET
export USE_COLUMN_ALIAS=false
export UPDATE_DATAPLEX_METADATA=false
export USE_STORAGE_WRITE_API=false
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="dataplex-jdbc-ingestion-job" \
-DtemplateName="Dataplex_JDBC_Ingestion" \
-Dparameters="connectionURL=$CONNECTION_URL,driverClassName=$DRIVER_CLASS_NAME,driverJars=$DRIVER_JARS,connectionProperties=$CONNECTION_PROPERTIES,username=$USERNAME,password=$PASSWORD,query=$QUERY,outputTable=$OUTPUT_TABLE,KMSEncryptionKey=$KMSENCRYPTION_KEY,outputAsset=$OUTPUT_ASSET,partitioningScheme=$PARTITIONING_SCHEME,paritionColumn=$PARITION_COLUMN,writeDisposition=$WRITE_DISPOSITION,fileFormat=$FILE_FORMAT,useColumnAlias=$USE_COLUMN_ALIAS,updateDataplexMetadata=$UPDATE_DATAPLEX_METADATA,useStorageWriteApi=$USE_STORAGE_WRITE_API,useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
-pl v2/googlecloud-to-googlecloud \
-am
```
