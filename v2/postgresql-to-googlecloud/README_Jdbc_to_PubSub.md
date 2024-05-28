
JDBC to Pub/Sub template
---
The Java Database Connectivity (JDBC) to Pub/Sub template is a batch pipeline
that ingests data from JDBC source and writes the resulting records to a
pre-existing Pub/Sub topic as a JSON string.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/jdbc-to-pubsub)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Jdbc_to_PubSub).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **driverClassName** : The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **connectionUrl** : The JDBC connection URL string. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. Remove whitespace characters from the Base64-encoded string.  (Example: jdbc:mysql://some-host:3306/sampledb).
* **driverJars** : Comma-separated Cloud Storage paths for JDBC drivers. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **query** : The query to run on the source to extract the data. (Example: select * from sampledb.sample_table).
* **outputTopic** : The Pub/Sub topic to publish to, in the format projects/<PROJECT_ID>/topics/<TOPIC_NAME>. (Example: projects/your-project-id/topics/your-topic-name).

### Optional parameters

* **username** : The username to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. For example, `echo -n 'some_username' | glcloud kms encrypt --location=my_location --keyring=mykeyring --key=mykey --plaintext-file=- --ciphertext-file=- | base64`.
* **password** : The password to use for the JDBC connection. You can pass in this value as a string that's encrypted with a Cloud KMS key and then Base64-encoded. For example, `echo -n 'some_password' | glcloud kms encrypt --location=my_location --keyring=mykeyring --key=mykey --plaintext-file=- --ciphertext-file=- | base64`.
* **connectionProperties** : The properties string to use for the JDBC connection. The format of the string must be `[propertyName=property;]*`.  (Example: unicode=true;characterEncoding=UTF-8).
* **KMSEncryptionKey** : The Cloud KMS Encryption Key to use to decrypt the username, password, and connection string. If a Cloud KMS key is passed in, the username, password, and connection string must all be passed in encrypted and base64 encoded. (Example: projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key).
* **disabledAlgorithms** : Comma separated algorithms to disable. If this value is set to none, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. (Example: SSLv3, RC4).
* **extraFilesToStage** : Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. (Example: gs://<BUCKET>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>).
* **defaultLogLevel** : Set Log level in the workers. Supported options are OFF, ERROR, WARN, INFO, DEBUG, TRACE. Defaults to INFO.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/postgresql-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToPubsub.java)

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
-DtemplateName="Jdbc_to_PubSub" \
-f v2/jdbc-to-googlecloud
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

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Jdbc_to_PubSub"

### Required
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
export DEFAULT_LOG_LEVEL=INFO

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
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
  --parameters "defaultLogLevel=$DEFAULT_LOG_LEVEL"
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
export DEFAULT_LOG_LEVEL=INFO

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="jdbc-to-pubsub-job" \
-DtemplateName="Jdbc_to_PubSub" \
-Dparameters="driverClassName=$DRIVER_CLASS_NAME,connectionUrl=$CONNECTION_URL,username=$USERNAME,password=$PASSWORD,driverJars=$DRIVER_JARS,connectionProperties=$CONNECTION_PROPERTIES,query=$QUERY,outputTopic=$OUTPUT_TOPIC,KMSEncryptionKey=$KMSENCRYPTION_KEY,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,defaultLogLevel=$DEFAULT_LOG_LEVEL" \
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
cd v2/jdbc-to-googlecloud/terraform/Jdbc_to_PubSub
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

resource "google_dataflow_flex_template_job" "jdbc_to_pubsub" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Jdbc_to_PubSub"
  name              = "jdbc-to-pubsub"
  region            = var.region
  parameters        = {
    driverClassName = "com.mysql.jdbc.Driver"
    connectionUrl = "jdbc:mysql://some-host:3306/sampledb"
    driverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    query = "select * from sampledb.sample_table"
    outputTopic = "projects/your-project-id/topics/your-topic-name"
    # username = "<username>"
    # password = "<password>"
    # connectionProperties = "unicode=true;characterEncoding=UTF-8"
    # KMSEncryptionKey = "projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key"
    # disabledAlgorithms = "SSLv3, RC4"
    # extraFilesToStage = "gs://<BUCKET>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>"
    # defaultLogLevel = "INFO"
  }
}
```
