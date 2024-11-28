
JDBC to Pub/Sub Auto template
---
The Java Database Connectivity (JDBC) to Pub/Sub template is a batch pipeline
that ingests data from JDBC source and writes the resulting records to a
pre-existing Pub/Sub topic as a JSON string.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **driverClassName**: JDBC driver class name to use. For example, `com.mysql.jdbc.Driver`.
* **connectionUrl**: Url connection string to connect to the JDBC source. Connection string can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS. For example, `jdbc:mysql://some-host:3306/sampledb`.
* **driverJars**: Comma separate Cloud Storage paths for JDBC drivers. For example, `gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar`.
* **query**: Query to be executed on the source to extract the data. For example, `select * from sampledb.sample_table`.
* **outputTopic**: The name of the topic to publish data to. For example, `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`.

### Optional parameters

* **username**: User name to be used for the JDBC connection. User name can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **password**: Password to be used for the JDBC connection. Password can be passed in as plaintext or as a base64 encoded string encrypted by Google Cloud KMS.
* **connectionProperties**: Properties string to use for the JDBC connection. Format of the string must be [propertyName=property;]*. For example, `unicode=true;characterEncoding=UTF-8`.
* **KMSEncryptionKey**: If this parameter is provided, password, user name and connection string should all be passed in encrypted. Encrypt parameters using the KMS API encrypt endpoint. See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt For example, `projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key`.
* **partitionColumn**: If this parameter is provided (along with `table`), JdbcIO reads the table in parallel by executing multiple instances of the query on the same table (subquery) using ranges. Currently, only Long partition columns are supported.
* **table**: Table to read from using partitions. This parameter also accepts a subquery in parentheses. For example, `(select id, name from Person) as subq`.
* **numPartitions**: The number of partitions. This, along with the lower and upper bound, form partitions strides for generated WHERE clause expressions used to split the partition column evenly. When the input is less than 1, the number is set to 1.
* **lowerBound**: Lower bound used in the partition scheme. If not provided, it is automatically inferred by Beam (for the supported types).
* **upperBound**: Upper bound used in partition scheme. If not provided, it is automatically inferred by Beam (for the supported types).



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/postgresql-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToPubSubAuto.java)

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
-DtemplateName="Jdbc_to_PubSub_Auto" \
-f v2/jdbc-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Jdbc_to_PubSub_Auto
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Jdbc_to_PubSub_Auto"

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
export PARTITION_COLUMN=<partitionColumn>
export TABLE=<table>
export NUM_PARTITIONS=<numPartitions>
export LOWER_BOUND=<lowerBound>
export UPPER_BOUND=<upperBound>

gcloud dataflow flex-template run "jdbc-to-pubsub-auto-job" \
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
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY" \
  --parameters "partitionColumn=$PARTITION_COLUMN" \
  --parameters "table=$TABLE" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "lowerBound=$LOWER_BOUND" \
  --parameters "upperBound=$UPPER_BOUND" \
  --parameters "outputTopic=$OUTPUT_TOPIC"
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
export PARTITION_COLUMN=<partitionColumn>
export TABLE=<table>
export NUM_PARTITIONS=<numPartitions>
export LOWER_BOUND=<lowerBound>
export UPPER_BOUND=<upperBound>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="jdbc-to-pubsub-auto-job" \
-DtemplateName="Jdbc_to_PubSub_Auto" \
-Dparameters="driverClassName=$DRIVER_CLASS_NAME,connectionUrl=$CONNECTION_URL,username=$USERNAME,password=$PASSWORD,driverJars=$DRIVER_JARS,connectionProperties=$CONNECTION_PROPERTIES,query=$QUERY,KMSEncryptionKey=$KMSENCRYPTION_KEY,partitionColumn=$PARTITION_COLUMN,table=$TABLE,numPartitions=$NUM_PARTITIONS,lowerBound=$LOWER_BOUND,upperBound=$UPPER_BOUND,outputTopic=$OUTPUT_TOPIC" \
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
cd v2/jdbc-to-googlecloud/terraform/Jdbc_to_PubSub_Auto
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

resource "google_dataflow_flex_template_job" "jdbc_to_pubsub_auto" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Jdbc_to_PubSub_Auto"
  name              = "jdbc-to-pubsub-auto"
  region            = var.region
  parameters        = {
    driverClassName = "<driverClassName>"
    connectionUrl = "<connectionUrl>"
    driverJars = "<driverJars>"
    query = "<query>"
    outputTopic = "<outputTopic>"
    # username = "<username>"
    # password = "<password>"
    # connectionProperties = "<connectionProperties>"
    # KMSEncryptionKey = "<KMSEncryptionKey>"
    # partitionColumn = "<partitionColumn>"
    # table = "<table>"
    # numPartitions = "<numPartitions>"
    # lowerBound = "<lowerBound>"
    # upperBound = "<upperBound>"
  }
}
```
