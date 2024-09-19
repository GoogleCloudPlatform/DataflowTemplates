
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

### Required parameters

* **inputSubscription** : The Pub/Sub input subscription to read from. (Example: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>).
* **driverClassName** : The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **connectionUrl** : The JDBC connection URL string. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string. (Example: jdbc:mysql://some-host:3306/sampledb).
* **driverJars** : Comma separated Cloud Storage paths for JDBC drivers.  (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **statement** : The statement to run against the database. The statement must specify the column names of the table in any order. Only the values of the specified column names are read from the JSON and added to the statement.  (Example: INSERT INTO tableName (column1, column2) VALUES (?,?)).
* **outputDeadletterTopic** : The Pub/Sub topic to forward undeliverable messages to.  (Example: projects/<PROJECT_ID>/topics/<TOPIC_NAME>).

### Optional parameters

* **username** : The username to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded.
* **password** : The password to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded.
* **connectionProperties** : The properties string to use for the JDBC connection. The string must use the format `[propertyName=property;]*`.  (Example: unicode=true;characterEncoding=UTF-8).
* **KMSEncryptionKey** : The Cloud KMS Encryption Key to use to decrypt the username, password, and connection string. If a Cloud KMS key is passed in, the username, password, and connection string must all be passed in encrypted. (Example: projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}).
* **disabledAlgorithms** : Comma separated algorithms to disable. If this value is set to `none`, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** : Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. (Example: gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>).

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
-DtemplateName="Pubsub_to_Jdbc" \
-f v2/googlecloud-to-googlecloud
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
-f v2/googlecloud-to-googlecloud
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
cd v2/googlecloud-to-googlecloud/terraform/Pubsub_to_Jdbc
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

resource "google_dataflow_flex_template_job" "pubsub_to_jdbc" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Pubsub_to_Jdbc"
  name              = "pubsub-to-jdbc"
  region            = var.region
  parameters        = {
    inputSubscription = "projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>"
    driverClassName = "com.mysql.jdbc.Driver"
    connectionUrl = "jdbc:mysql://some-host:3306/sampledb"
    driverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    statement = "INSERT INTO tableName (column1, column2) VALUES (?,?)"
    outputDeadletterTopic = "projects/<PROJECT_ID>/topics/<TOPIC_NAME>"
    # username = "<username>"
    # password = "<password>"
    # connectionProperties = "unicode=true;characterEncoding=UTF-8"
    # KMSEncryptionKey = "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}"
    # disabledAlgorithms = "SSLv3, RC4"
    # extraFilesToStage = "gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>"
  }
}
```
