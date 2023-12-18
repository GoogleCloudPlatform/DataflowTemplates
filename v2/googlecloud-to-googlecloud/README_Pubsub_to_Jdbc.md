
Pub/Sub to JDBC template
---
The Pub/Sub to Java Database Connectivity (JDBC) template is a streaming pipeline
that ingests data from a pre-existing Cloud Pub/Sub subscription as JSON strings,
and writes the resulting records to JDBC.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-jdbc)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Pubsub_to_Jdbc).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **inputSubscription** (Pub/Sub input subscription): Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' (Example: projects/your-project-id/subscriptions/your-subscription-name).
* **driverClassName** (JDBC driver class name.): JDBC driver class name to use. (Example: com.mysql.jdbc.Driver).
* **connectionUrl** (JDBC connection URL string.): Url connection string to connect to the JDBC source. Connection string can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS. (Example: jdbc:mysql://some-host:3306/sampledb).
* **driverJars** (Cloud Storage paths for JDBC drivers): Comma separate Cloud Storage paths for JDBC drivers. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **statement** (Statement which will be executed against the database.): SQL statement which will be executed to write to the database. The statement must specify the column names of the table in any order. Only the values of the specified column names will be read from the json and added to the statement. (Example: INSERT INTO tableName (column1, column2) VALUES (?,?)).
* **outputDeadletterTopic** (Output deadletter Pub/Sub topic): The Pub/Sub topic to publish deadletter records to. The name should be in the format of projects/your-project-id/topics/your-topic-name.

### Optional Parameters

* **username** (JDBC connection username.): User name to be used for the JDBC connection. User name can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **password** (JDBC connection password.): Password to be used for the JDBC connection. Password can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **connectionProperties** (JDBC connection property string.): Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. (Example: unicode=true;characterEncoding=UTF-8).
* **KMSEncryptionKey** (Google Cloud KMS encryption key): If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt (Example: projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}).
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



[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/PubsubToJdbc.java)

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
-DtemplateName="Pubsub_to_Jdbc" \
-pl v2/googlecloud-to-googlecloud \
-am
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Pubsub_to_Jdbc
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Pubsub_to_Jdbc"

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionUrl>
export DRIVER_JARS=<driverJars>
export STATEMENT=<statement>
export OUTPUT_DEADLETTER_TOPIC=<outputDeadletterTopic>

### Optional
export USERNAME=<username>
export PASSWORD=<password>
export CONNECTION_PROPERTIES=<connectionProperties>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow flex-template run "pubsub-to-jdbc-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "connectionUrl=$CONNECTION_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "driverJars=$DRIVER_JARS" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "statement=$STATEMENT" \
  --parameters "outputDeadletterTopic=$OUTPUT_DEADLETTER_TOPIC" \
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

### Required
export INPUT_SUBSCRIPTION=<inputSubscription>
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_URL=<connectionUrl>
export DRIVER_JARS=<driverJars>
export STATEMENT=<statement>
export OUTPUT_DEADLETTER_TOPIC=<outputDeadletterTopic>

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
-DjobName="pubsub-to-jdbc-job" \
-DtemplateName="Pubsub_to_Jdbc" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,driverClassName=$DRIVER_CLASS_NAME,connectionUrl=$CONNECTION_URL,username=$USERNAME,password=$PASSWORD,driverJars=$DRIVER_JARS,connectionProperties=$CONNECTION_PROPERTIES,statement=$STATEMENT,outputDeadletterTopic=$OUTPUT_DEADLETTER_TOPIC,KMSEncryptionKey=$KMSENCRYPTION_KEY,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-pl v2/googlecloud-to-googlecloud \
-am
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

resource "google_dataflow_flex_template_job" "pubsub_to_jdbc" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Pubsub_to_Jdbc"
  name              = "pubsub-to-jdbc"
  region            = var.region
  parameters        = {
    inputSubscription = "projects/your-project-id/subscriptions/your-subscription-name"
    driverClassName = "com.mysql.jdbc.Driver"
    connectionUrl = "jdbc:mysql://some-host:3306/sampledb"
    driverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    statement = "INSERT INTO tableName (column1, column2) VALUES (?,?)"
    outputDeadletterTopic = "<outputDeadletterTopic>"
    # username = "<username>"
    # password = "<password>"
    # connectionProperties = "unicode=true;characterEncoding=UTF-8"
    # KMSEncryptionKey = "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}"
    # disabledAlgorithms = "SSLv3, RC4"
    # extraFilesToStage = "gs://your-bucket/file.txt,projects/project-id/secrets/secret-id/versions/version-id"
  }
}
```
