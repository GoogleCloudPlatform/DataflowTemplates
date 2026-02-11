
PubSub to AlloyDb (YAML) template
---
The PubSub to AlloyDb template is a streaming pipeline which ingests data from a
PubSub topic, executes a user-defined mapping, and writes the resulting records
to AlloyDb. Any errors which occur in the transformation of the data are written
to a separate Pub/Sub topic.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **subscription**: Pub/Sub subscription to read the input from. For example, `projects/your-project-id/subscriptions/your-subscription-name`.
* **format**: The message format. One of: AVRO, JSON, PROTO, RAW, or STRING. For example, `JSON`.
* **schema**: A schema is required if data format is JSON, AVRO or PROTO. For JSON, this is a JSON schema. For AVRO and PROTO, this is the full schema definition.
* **language**: The language used to define (and execute) the expressions and callables in fields. For example, `python`.
* **fields**: The output fields to compute, each mapping to the expression or callable that creates them.
* **url**: The JDBC URL for connecting to AlloyDb instance. Format: jdbc:postgresql:///db?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/<PROJECT>/locations/<REGION>/clusters/<CLUSTER>/instances/<INSTANCE>&alloydbIpType=PRIVATE For example, `jdbc:postgresql:///mydatabase?socketFactory=com.google.cloud.alloydb.SocketFactory&alloydbInstanceName=projects/my-project/locations/us-central1/clusters/my-cluster/instances/my-instance&alloydbIpType=PRIVATE`.
* **username**: Username for AlloyDb authentication For example, `postgres`.
* **password**: Password for AlloyDb authentication.
* **location**: The name of the table where records will be written For example, `my_table`.
* **writeStatement**: The SQL INSERT or UPSERT statement template for writing records to AlloyDb For example, `INSERT INTO table_name (col1, col2) VALUES (?, ?)`.
* **outputDeadLetterPubSubTopic**: Pub/Sub error topic for failed transformation messages. For example, `projects/your-project-id/topics/your-error-topic-name`.

### Optional parameters

* **attributes**: List of attribute keys whose values will be flattened into the output message as additional fields.
* **attributeMap**: Name of a field in which to store the full set of attributes associated with this message.
* **idAttribute**: The attribute on incoming Pub/Sub messages to use as a unique record identifier. When specified, the value of this attribute will be used for deduplication of messages.
* **timestampAttribute**: Message value to use as element timestamp. If None, uses message publishing time as the timestamp.
* **windowing**: Windowing options - see https://beam.apache.org/documentation/sdks/yaml/#windowing.
* **driverClassName**: The JDBC driver class name for connecting to AlloyDb For example, `org.postgresql.Driver`.
* **connectionProperties**: Optional connection properties as key-value pairs. Example: sslmode=require;connectTimeout=10 For example, `sslmode=require;connectTimeout=10`.
* **connectionInitSql**: A list of SQL statements to execute when a new connection is established. For example, `["SET TIME ZONE UTC"]`.
* **batchSize**: Number of records to batch before writing to the database For example, `100`.
* **autosharding**: Whether to use autosharding for distributing writes across multiple connections.
* **network**: The VPC network where AlloyDB is located. For example, `default`.
* **subnetwork**: The subnetwork for Dataflow workers. Required for custom VPCs. For example, `regions/us-central1/subnetworks/my-subnet`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=yaml/src/main/java/com/google/cloud/teleport/templates/yaml/PubSubToAlloyDbYaml.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

#### Validating the Template

This template has a validation command that is used to check code quality.

```shell
mvn clean install -PtemplatesValidate \
-DskipTests -am \
-pl yaml
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
export ARTIFACT_REGISTRY_REPO=<region>-docker.pkg.dev/$PROJECT/<repo>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DartifactRegistry="$ARTIFACT_REGISTRY_REPO" \
-DstagePrefix="templates" \
-DtemplateName="PubSub_To_AlloyDb_Yaml" \
-f yaml
```

The `-DartifactRegistry` parameter can be specified to set the artifact registry repository of the Flex Templates image.
If not provided, it defaults to `gcr.io/<project>`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/PubSub_To_AlloyDb_Yaml
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/PubSub_To_AlloyDb_Yaml"

### Required
export SUBSCRIPTION=<subscription>
export FORMAT=<format>
export SCHEMA=<schema>
export LANGUAGE=<language>
export FIELDS=<fields>
export URL=<url>
export USERNAME=<username>
export PASSWORD=<password>
export LOCATION=<location>
export WRITE_STATEMENT=<writeStatement>
export OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC=<outputDeadLetterPubSubTopic>

### Optional
export ATTRIBUTES=<attributes>
export ATTRIBUTE_MAP=<attributeMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export WINDOWING=<windowing>
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_PROPERTIES=<connectionProperties>
export CONNECTION_INIT_SQL=<connectionInitSql>
export BATCH_SIZE=<batchSize>
export AUTOSHARDING=<autosharding>
export NETWORK=<network>
export SUBNETWORK=<subnetwork>

gcloud dataflow flex-template run "pubsub-to-alloydb-yaml-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "subscription=$SUBSCRIPTION" \
  --parameters "format=$FORMAT" \
  --parameters "schema=$SCHEMA" \
  --parameters "attributes=$ATTRIBUTES" \
  --parameters "attributeMap=$ATTRIBUTE_MAP" \
  --parameters "idAttribute=$ID_ATTRIBUTE" \
  --parameters "timestampAttribute=$TIMESTAMP_ATTRIBUTE" \
  --parameters "language=$LANGUAGE" \
  --parameters "fields=$FIELDS" \
  --parameters "windowing=$WINDOWING" \
  --parameters "url=$URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "driverClassName=$DRIVER_CLASS_NAME" \
  --parameters "connectionProperties=$CONNECTION_PROPERTIES" \
  --parameters "connectionInitSql=$CONNECTION_INIT_SQL" \
  --parameters "location=$LOCATION" \
  --parameters "writeStatement=$WRITE_STATEMENT" \
  --parameters "batchSize=$BATCH_SIZE" \
  --parameters "autosharding=$AUTOSHARDING" \
  --parameters "network=$NETWORK" \
  --parameters "subnetwork=$SUBNETWORK" \
  --parameters "outputDeadLetterPubSubTopic=$OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC"
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
export SUBSCRIPTION=<subscription>
export FORMAT=<format>
export SCHEMA=<schema>
export LANGUAGE=<language>
export FIELDS=<fields>
export URL=<url>
export USERNAME=<username>
export PASSWORD=<password>
export LOCATION=<location>
export WRITE_STATEMENT=<writeStatement>
export OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC=<outputDeadLetterPubSubTopic>

### Optional
export ATTRIBUTES=<attributes>
export ATTRIBUTE_MAP=<attributeMap>
export ID_ATTRIBUTE=<idAttribute>
export TIMESTAMP_ATTRIBUTE=<timestampAttribute>
export WINDOWING=<windowing>
export DRIVER_CLASS_NAME=<driverClassName>
export CONNECTION_PROPERTIES=<connectionProperties>
export CONNECTION_INIT_SQL=<connectionInitSql>
export BATCH_SIZE=<batchSize>
export AUTOSHARDING=<autosharding>
export NETWORK=<network>
export SUBNETWORK=<subnetwork>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="pubsub-to-alloydb-yaml-job" \
-DtemplateName="PubSub_To_AlloyDb_Yaml" \
-Dparameters="subscription=$SUBSCRIPTION,format=$FORMAT,schema=$SCHEMA,attributes=$ATTRIBUTES,attributeMap=$ATTRIBUTE_MAP,idAttribute=$ID_ATTRIBUTE,timestampAttribute=$TIMESTAMP_ATTRIBUTE,language=$LANGUAGE,fields=$FIELDS,windowing=$WINDOWING,url=$URL,username=$USERNAME,password=$PASSWORD,driverClassName=$DRIVER_CLASS_NAME,connectionProperties=$CONNECTION_PROPERTIES,connectionInitSql=$CONNECTION_INIT_SQL,location=$LOCATION,writeStatement=$WRITE_STATEMENT,batchSize=$BATCH_SIZE,autosharding=$AUTOSHARDING,network=$NETWORK,subnetwork=$SUBNETWORK,outputDeadLetterPubSubTopic=$OUTPUT_DEAD_LETTER_PUB_SUB_TOPIC" \
-f yaml
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
cd v2/yaml/terraform/PubSub_To_AlloyDb_Yaml
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

resource "google_dataflow_flex_template_job" "pubsub_to_alloydb_yaml" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/PubSub_To_AlloyDb_Yaml"
  name              = "pubsub-to-alloydb-yaml"
  region            = var.region
  parameters        = {
    subscription = "<subscription>"
    format = "<format>"
    schema = "<schema>"
    language = "<language>"
    fields = "<fields>"
    url = "<url>"
    username = "<username>"
    password = "<password>"
    location = "<location>"
    writeStatement = "<writeStatement>"
    outputDeadLetterPubSubTopic = "<outputDeadLetterPubSubTopic>"
    # attributes = "<attributes>"
    # attributeMap = "<attributeMap>"
    # idAttribute = "<idAttribute>"
    # timestampAttribute = "<timestampAttribute>"
    # windowing = "<windowing>"
    # driverClassName = "<driverClassName>"
    # connectionProperties = "<connectionProperties>"
    # connectionInitSql = "<connectionInitSql>"
    # batchSize = "<batchSize>"
    # autosharding = "<autosharding>"
    # network = "<network>"
    # subnetwork = "<subnetwork>"
  }
}
```
