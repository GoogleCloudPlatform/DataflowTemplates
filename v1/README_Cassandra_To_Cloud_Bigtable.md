
Cassandra to Cloud Bigtable template
---
The Apache Cassandra to Cloud Bigtable template copies a table from Apache
Cassandra to Cloud Bigtable. This template requires minimal configuration and
replicates the table structure in Cassandra as closely as possible in Cloud
Bigtable.

The Apache Cassandra to Cloud Bigtable template is useful for the following:
- Migrating Apache Cassandra database when short downtime is acceptable.
- Periodically replicating Cassandra tables to Cloud Bigtable for global serving.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/cassandra-to-bigtable)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cassandra_To_Cloud_Bigtable).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **cassandraHosts** : The hosts of the Apache Cassandra nodes in a comma-separated list.
* **cassandraKeyspace** : The Apache Cassandra keyspace where the table is located.
* **cassandraTable** : The Apache Cassandra table to copy.
* **bigtableProjectId** : The Google Cloud project ID associated with the Bigtable instance.
* **bigtableInstanceId** : The ID of the Bigtable instance that the Apache Cassandra table is copied to.
* **bigtableTableId** : The name of the Bigtable table that the Apache Cassandra table is copied to.

### Optional parameters

* **cassandraPort** : The TCP port to use to reach Apache Cassandra on the nodes. The default value is 9042.
* **defaultColumnFamily** : The name of the column family of the Bigtable table. The default value is default.
* **rowKeySeparator** : The separator used to build row-keys. The default value is '#'.
* **splitLargeRows** : The flag for enabling splitting of large rows into multiple MutateRows requests. Note that when a large row is split between multiple API calls, the updates to the row are not atomic. .



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v1/src/main/java/com/google/cloud/teleport/bigtable/CassandraToBigtable.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

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
-f v1
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
export CASSANDRA_PORT=9042
export DEFAULT_COLUMN_FAMILY=default
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
export CASSANDRA_PORT=9042
export DEFAULT_COLUMN_FAMILY=default
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
-f v1
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v1/terraform/Cassandra_To_Cloud_Bigtable
terraform init
terraform apply
```

To use
[dataflow_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_job)
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

resource "google_dataflow_job" "cassandra_to_cloud_bigtable" {

  provider          = google-beta
  template_gcs_path = "gs://dataflow-templates-${var.region}/latest/Cassandra_To_Cloud_Bigtable"
  name              = "cassandra-to-cloud-bigtable"
  region            = var.region
  temp_gcs_location = "gs://bucket-name-here/temp"
  parameters        = {
    cassandraHosts = "<cassandraHosts>"
    cassandraKeyspace = "<cassandraKeyspace>"
    cassandraTable = "<cassandraTable>"
    bigtableProjectId = "<bigtableProjectId>"
    bigtableInstanceId = "<bigtableInstanceId>"
    bigtableTableId = "<bigtableTableId>"
    # cassandraPort = "9042"
    # defaultColumnFamily = "default"
    # rowKeySeparator = ""#""
    # splitLargeRows = "<splitLargeRows>"
  }
}
```
