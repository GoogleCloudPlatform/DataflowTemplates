JDBC to Pub/Sub Template
---
A batch pipeline which ingests data from JDBC source and writes to a pre-existing Pub/Sub topic as a JSON string. JDBC connection string, user name and password can be passed in directly as plaintext or encrypted using the Google Cloud KMS API.  If the parameter KMSEncryptionKey is specified, connectionUrl, username, and password should be all in encrypted format. A sample curl command for the KMS API encrypt endpoint: curl -s -X POST "https://cloudkms.googleapis.com/v1/projects/your-project/locations/your-path/keyRings/your-keyring/cryptoKeys/your-key:encrypt"  -d "{\"plaintext\":"PasteBase64EncodedString\"}"  -H "Authorization: Bearer $(gcloud auth application-default print-access-token)"  -H "Content-Type: application/json".

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-pubsub)
on how to use it without having to build from sources.

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Mandatory Parameters

* **driverClassName** (JDBC driver class name.): JDBC driver class name to use. (Example: com.mysql.jdbc.Driver).
* **connectionUrl** (JDBC connection URL string.): Url connection string to connect to the JDBC source. Connection string can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS. (Example: jdbc:mysql://some-host:3306/sampledb).
* **driverJars** (Cloud Storage paths for JDBC drivers): Comma separate Cloud Storage paths for JDBC drivers. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **query** (JDBC source SQL query.): Query to be executed on the source to extract the data. (Example: select * from sampledb.sample_table).
* **outputTopic** (Output Pub/Sub topic): The name of the topic to which data should published, in the format of 'projects/your-project-id/topics/your-topic-name' (Example: projects/your-project-id/topics/your-topic-name).

### Optional Parameters

* **username** (JDBC connection username.): User name to be used for the JDBC connection. User name can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **password** (JDBC connection password.): Password to be used for the JDBC connection. Password can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **connectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **KMSEncryptionKey** (Google Cloud KMS key): If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **disabledAlgorithms** (Disabled algorithms to override jdk.tls.disabledAlgorithms): Comma separated algorithms to disable. If this value is set to "none" then dk.tls.disabledAlgorithms is set to "". Use with care, as the algorithms disabled by default are known to have either vulnerabilities or performance issues. For example: SSLv3, RC4.
* **extraFilesToStage** (Extra files to stage in the workers): Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files will be saved under the `/extra_files` directory in each worker. (Example: gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id).

## Getting Started

### Requirements

* Java 11
* Maven
* Valid resources for mandatory parameters.
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
    * `gcloud auth login`
    * `gcloud auth application-default login`

The following instructions use the
[Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin)
. Install the plugin with the following command to proceed:

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
-DtemplateName="Jdbc_to_PubSub" \
-pl v2/googlecloud-to-googlecloud \
-am
```

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Jdbc_to_PubSub
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with that template at any time using `gcloud`, you can use:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Jdbc_to_PubSub"

### Mandatory
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionUrl>
export DRIVER_JARS=<driverJars>
export QUERY=<query>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export USERNAME=<username>
export PASSWORD=<password>
export CONNECTION_PROPERTIES=<connectionProperties>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow flex-template run "jdbc-to-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "connectionUrl=$CONNECTION_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "driverJars=$DRIVER_JARS" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "query=$QUERY" \
  --parameters "outputTopic=$OUTPUT_TOPIC" \
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY" \
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

### Mandatory
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionUrl>
export DRIVER_JARS=<driverJars>
export QUERY=<query>
export OUTPUT_TOPIC=<outputTopic>

### Optional
export USERNAME=<username>
export PASSWORD=<password>
export CONNECTION_PROPERTIES=<connectionProperties>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="jdbc-to-pubsub-job" \
-DtemplateName="Jdbc_to_PubSub" \
-Dparameters="driverClassName=$DRIVER_CLASS_NAME,connectionUrl=$CONNECTION_URL,username=$USERNAME,password=$PASSWORD,driverJars=$DRIVER_JARS,connectionProperties=$CONNECTION_PROPERTIES,query=$QUERY,outputTopic=$OUTPUT_TOPIC,KMSEncryptionKey=$KMSENCRYPTION_KEY,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-pl v2/googlecloud-to-googlecloud \
-am
```
