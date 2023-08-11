Cassandra to Cloud Bigtable Template
---
A pipeline to import a Apache Cassandra table into Cloud Bigtable.

:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cassandra-to-bigtable)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cassandra_To_Cloud_Bigtable).


:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters

* **cassandraHosts** (Cassandra Hosts): Comma separated value list of hostnames or ips of the Cassandra nodes.
* **cassandraKeyspace** (Cassandra Keyspace): Cassandra Keyspace where the table to be migrated can be located.
* **cassandraTable** (Cassandra Table): The name of the Cassandra table to Migrate.
* **bigtableProjectId** (Bigtable Project ID): The Project ID where the target Bigtable Instance is running.
* **bigtableInstanceId** (Target Bigtable Instance): The target Bigtable Instance where you want to write the data.
* **bigtableTableId** (Target Bigtable Table): The target Bigtable table where you want to write the data.

### Optional Parameters

* **cassandraPort** (Cassandra Port): The port where cassandra can be reached. Defaults to 9042.
* **defaultColumnFamily** (The Default Bigtable Column Family): This specifies the default column family to write data into. If no columnFamilyMapping is specified all Columns will be written into this column family. Default value is "default".
* **rowKeySeparator** (The Row Key Separator): All primary key fields will be appended to form your Bigtable Row Key. The rowKeySeparator allows you to specify a character separator. Default separator is '#'.
* **splitLargeRows** (If true, large rows will be split into multiple MutateRows requests): The flag for enabling splitting of large rows into multiple MutateRows requests. Note that when a large row is split between multiple API calls, the updates to the row are not atomic. .



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=/v1/src/main/java/com/google/cloud/teleport/bigtable/CassandraToBigtable.java)

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
-DtemplateName="Cassandra_To_Cloud_Bigtable" \
-pl v1 \
-am
```

The `-DgcpTempLocation=<temp-bucket-name>` parameter can be specified to set the GCS bucket used by the DataflowRunner to write
temp files to during serialization. The path used will be `gs://<temp-bucket-name>/temp/`.

The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Classic Template was staged! gs://<bucket-name>/templates/Cassandra_To_Cloud_Bigtable
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/Cassandra_To_Cloud_Bigtable"

### Required
export CASSANDRA_HOSTS=<cassandraHosts>
export CASSANDRA_KEYSPACE=<cassandraKeyspace>
export CASSANDRA_TABLE=<cassandraTable>
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>

### Optional
export CASSANDRA_PORT="9042"
export DEFAULT_COLUMN_FAMILY="default"
export ROW_KEY_SEPARATOR="#"
export SPLIT_LARGE_ROWS=<splitLargeRows>

gcloud dataflow jobs run "cassandra-to-cloud-bigtable-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "cassandraHosts=$CASSANDRA_HOSTS" \
  --parameters "cassandraPort=$CASSANDRA_PORT" \
  --parameters "cassandraKeyspace=$CASSANDRA_KEYSPACE" \
  --parameters "cassandraTable=$CASSANDRA_TABLE" \
  --parameters "bigtableProjectId=$BIGTABLE_PROJECT_ID" \
  --parameters "bigtableInstanceId=$BIGTABLE_INSTANCE_ID" \
  --parameters "bigtableTableId=$BIGTABLE_TABLE_ID" \
  --parameters "defaultColumnFamily=$DEFAULT_COLUMN_FAMILY" \
  --parameters "rowKeySeparator=$ROW_KEY_SEPARATOR" \
  --parameters "splitLargeRows=$SPLIT_LARGE_ROWS"
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
export CASSANDRA_HOSTS=<cassandraHosts>
export CASSANDRA_KEYSPACE=<cassandraKeyspace>
export CASSANDRA_TABLE=<cassandraTable>
export BIGTABLE_PROJECT_ID=<bigtableProjectId>
export BIGTABLE_INSTANCE_ID=<bigtableInstanceId>
export BIGTABLE_TABLE_ID=<bigtableTableId>

### Optional
export CASSANDRA_PORT="9042"
export DEFAULT_COLUMN_FAMILY="default"
export ROW_KEY_SEPARATOR="#"
export SPLIT_LARGE_ROWS=<splitLargeRows>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cassandra-to-cloud-bigtable-job" \
-DtemplateName="Cassandra_To_Cloud_Bigtable" \
-Dparameters="cassandraHosts=$CASSANDRA_HOSTS,cassandraPort=$CASSANDRA_PORT,cassandraKeyspace=$CASSANDRA_KEYSPACE,cassandraTable=$CASSANDRA_TABLE,bigtableProjectId=$BIGTABLE_PROJECT_ID,bigtableInstanceId=$BIGTABLE_INSTANCE_ID,bigtableTableId=$BIGTABLE_TABLE_ID,defaultColumnFamily=$DEFAULT_COLUMN_FAMILY,rowKeySeparator=$ROW_KEY_SEPARATOR,splitLargeRows=$SPLIT_LARGE_ROWS" \
-pl v1 \
-am
```
