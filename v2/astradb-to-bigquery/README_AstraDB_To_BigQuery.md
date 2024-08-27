
AstraDB to BigQuery template
---
The AstraDB to BigQuery template is a batch pipeline that reads records from
AstraDB and writes them to BigQuery.

If the destination table doesn't exist in BigQuery, the pipeline creates a table
with the following values:
- The `Dataset ID` is inherited from the Cassandra keyspace.
- The `Table ID` is inherited from the Cassandra table.

The schema of the destination table is inferred from the source Cassandra table.
- `List` and `Set` are mapped to BigQuery `REPEATED` fields.
- `Map` are mapped to BigQuery `RECORD` fields.
- All other types are mapped to BigQuery fields with the corresponding types.
- Cassandra user-defined types (UDTs) and tuple data types are not supported.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/astradb-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=AstraDB_To_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **astraToken** : The token value or secret resource ID. (Example: AstraCS:abcdefghij).
* **astraDatabaseId** : The database unique identifier (UUID). (Example: cf7af129-d33a-498f-ad06-d97a6ee6eb7).
* **astraKeyspace** : The name of the Cassandra keyspace inside of the Astra database.
* **astraTable** : The name of the table inside of the Cassandra database. (Example: my_table).

### Optional parameters

* **astraQuery** : The query to use to filter rows instead of reading the whole table.
* **astraDatabaseRegion** : If not provided, a default is chosen, which is useful with multi-region databases.
* **minTokenRangesCount** : The minimal number of splits to use to distribute the query.
* **outputTableSpec** : The BigQuery table location to write the output to. Use the format `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`. The table's schema must match the input objects.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/astradb-to-bigquery/src/main/java/com/google/cloud/teleport/v2/astradb/templates/AstraDbToBigQuery.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin).

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
-DtemplateName="AstraDB_To_BigQuery" \
-f v2/astradb-to-bigquery
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/AstraDB_To_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/AstraDB_To_BigQuery"

### Required
export ASTRA_TOKEN=<astraToken>
export ASTRA_DATABASE_ID=<astraDatabaseId>
export ASTRA_KEYSPACE=<astraKeyspace>
export ASTRA_TABLE=<astraTable>

### Optional
export ASTRA_QUERY=<astraQuery>
export ASTRA_DATABASE_REGION=<astraDatabaseRegion>
export MIN_TOKEN_RANGES_COUNT=<minTokenRangesCount>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

gcloud dataflow flex-template run "astradb-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "astraToken=$ASTRA_TOKEN" \
  --parameters "astraDatabaseId=$ASTRA_DATABASE_ID" \
  --parameters "astraKeyspace=$ASTRA_KEYSPACE" \
  --parameters "astraTable=$ASTRA_TABLE" \
  --parameters "astraQuery=$ASTRA_QUERY" \
  --parameters "astraDatabaseRegion=$ASTRA_DATABASE_REGION" \
  --parameters "minTokenRangesCount=$MIN_TOKEN_RANGES_COUNT" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC"
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
export ASTRA_TOKEN=<astraToken>
export ASTRA_DATABASE_ID=<astraDatabaseId>
export ASTRA_KEYSPACE=<astraKeyspace>
export ASTRA_TABLE=<astraTable>

### Optional
export ASTRA_QUERY=<astraQuery>
export ASTRA_DATABASE_REGION=<astraDatabaseRegion>
export MIN_TOKEN_RANGES_COUNT=<minTokenRangesCount>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="astradb-to-bigquery-job" \
-DtemplateName="AstraDB_To_BigQuery" \
-Dparameters="astraToken=$ASTRA_TOKEN,astraDatabaseId=$ASTRA_DATABASE_ID,astraKeyspace=$ASTRA_KEYSPACE,astraTable=$ASTRA_TABLE,astraQuery=$ASTRA_QUERY,astraDatabaseRegion=$ASTRA_DATABASE_REGION,minTokenRangesCount=$MIN_TOKEN_RANGES_COUNT,outputTableSpec=$OUTPUT_TABLE_SPEC" \
-f v2/astradb-to-bigquery
```

#### Troubleshooting
If there are compilation errors related to template metadata or template plugin framework,
make sure the plugin dependencies are up-to-date by running:
```
mvn clean install -pl plugins/templates-maven-plugin,metadata -am
```
See [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/code-contributions.md#templates-plugin)
for more information.



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
cd v2/astradb-to-bigquery/terraform/AstraDB_To_BigQuery
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

resource "google_dataflow_flex_template_job" "astradb_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/AstraDB_To_BigQuery"
  name              = "astradb-to-bigquery"
  region            = var.region
  parameters        = {
    astraToken = "AstraCS:abcdefghij"
    astraDatabaseId = "cf7af129-d33a-498f-ad06-d97a6ee6eb7"
    astraKeyspace = "<astraKeyspace>"
    astraTable = "my_table"
    # astraQuery = "<astraQuery>"
    # astraDatabaseRegion = "<astraDatabaseRegion>"
    # minTokenRangesCount = "<minTokenRangesCount>"
    # outputTableSpec = "<outputTableSpec>"
  }
}
```
