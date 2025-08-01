
Sourcedb to Spanner template
---
The SourceDB to Spanner template is a batch pipeline that copies data from a
relational database into an existing Spanner database. This pipeline uses JDBC to
connect to the relational database. You can use this template to copy data from
any relational database with available JDBC drivers into Spanner. This currently
only supports a limited set of types of MySQL.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Sourcedb_to_Spanner_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **instanceId**: The destination Cloud Spanner instance.
* **databaseId**: The destination Cloud Spanner database.
* **projectId**: This is the name of the Cloud Spanner project.
* **outputDirectory**: This directory is used to dump the failed/skipped/filtered records in a migration.

### Optional parameters

* **sourceDbDialect**: Possible values are `CASSANDRA`, `MYSQL` and `POSTGRESQL`. Defaults to: MYSQL.
* **jdbcDriverJars**: The comma-separated list of driver JAR files. For example, `gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar`. Defaults to empty.
* **jdbcDriverClassName**: The JDBC driver class name. For example, `com.mysql.jdbc.Driver`. Defaults to: com.mysql.jdbc.Driver.
* **sourceConfigURL**: The JDBC connection URL string. For example, `jdbc:mysql://127.4.5.30:3306/my-db?autoReconnect=true&maxReconnects=10&unicode=true&characterEncoding=UTF-8` or the shard config. This parameter is required except for ASTRA_DB source. Defaults to empty.
* **username**: The username to be used for the JDBC connection. Defaults to empty.
* **password**: The password to be used for the JDBC connection. Defaults to empty.
* **tables**: Tables to migrate from source. Defaults to empty.
* **numPartitions**: The number of partitions. This, along with the lower and upper bound, form partitions strides for generated WHERE clause expressions used to split the partition column evenly. When the input is less than 1, the number is set to 1. Defaults to: 0.
* **fetchSize**: The number of rows to fetch per page read for JDBC source. If not set, the default of JdbcIO of 50_000 rows gets used. If source dialect is Mysql, please see the note below. This ultimately translated to Statement.setFetchSize call at Jdbc layer. It should ONLY be used if the default value throws memory errors.Note for MySql Source:  FetchSize is ignored by the Mysql connector unless, `useCursorFetch=true` is also part of the connection properties.In case, the fetchSize parameter is explicitly set, for MySql dialect, the pipeline will add `useCursorFetch=true` to the connection properties by default.
* **spannerHost**: The Cloud Spanner endpoint to call in the template. For example, `https://batch-spanner.googleapis.com`. Defaults to: https://batch-spanner.googleapis.com.
* **maxConnections**: Configures the JDBC connection pool on each worker with maximum number of connections. Use a negative number for no limit. For example, `-1`. Defaults to: 0.
* **sessionFilePath**: Session file path in Cloud Storage that contains mapping information from Spanner Migration Tool. Defaults to empty.
* **transformationJarPath**: Custom jar location in Cloud Storage that contains the custom transformation logic for processing records. Defaults to empty.
* **transformationClassName**: Fully qualified class name having the custom transformation logic. It is a mandatory field in case transformationJarPath is specified. Defaults to empty.
* **transformationCustomParameters**: String containing any custom parameters to be passed to the custom transformation class. Defaults to empty.
* **namespace**: Namespace to exported. For PostgreSQL, if no namespace is provided, 'public' will be used. Defaults to empty.
* **insertOnlyModeForSpannerMutations**: By default the pipeline uses Upserts to write rows to spanner. Which means existing rows would get overwritten. If InsertOnly mode is enabled, inserts would be used instead of upserts and existing rows won't be overwritten.
* **batchSizeForSpannerMutations**: BatchSize in bytes for Spanner Mutations. if set less than 0, default of Apache Beam's SpannerIO is used, which is 1MB. Set this to 0 or 10, to disable batching mutations.
* **spannerPriority**: The request priority for Cloud Spanner calls. The value must be one of: [`HIGH`,`MEDIUM`,`LOW`]. Defaults to `MEDIUM`.
* **tableOverrides**: These are the table name overrides from source to spanner. They are written in thefollowing format: [{SourceTableName1, SpannerTableName1}, {SourceTableName2, SpannerTableName2}]This example shows mapping Singers table to Vocalists and Albums table to Records. For example, `[{Singers, Vocalists}, {Albums, Records}]`. Defaults to empty.
* **columnOverrides**: These are the column name overrides from source to spanner. They are written in thefollowing format: [{SourceTableName1.SourceColumnName1, SourceTableName1.SpannerColumnName1}, {SourceTableName2.SourceColumnName1, SourceTableName2.SpannerColumnName1}]Note that the SourceTableName should remain the same in both the source and spanner pair. To override table names, use tableOverrides.The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively. For example, `[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]`. Defaults to empty.
* **schemaOverridesFilePath**: A file which specifies the table and the column name overrides from source to spanner. Defaults to empty.
* **uniformizationStageCountHint**: Hint for number of uniformization stages. Currently Applicable only for jdbc based sources like MySQL or PostgreSQL. Leave 0 or default to disable uniformization. Set to -1 for a log(numPartition) number of stages. If your source primary key space is uniformly distributed (for example an auto-incrementing key with sparse holes), it's based to leave it disabled. If your keyspace is not uniform, you might encounter a laggard VM in your dataflow run. In such a case, you can set it to -1 to enable uniformization. Manually setting it to values other than 0 or -1 would help you fine tune the tradeoff of the overhead added by uniformization stages and the  performance improvement due to better distribution of work.
* **astraDBToken**: AstraDB token, ignored for non-AstraDB dialects. This token is used to automatically download the securebundle by the tempalte. Defaults to empty.
* **astraDBDatabaseId**: AstraDB databaseID, ignored for non-AstraDB dialects. Defaults to empty.
* **astraDBKeySpace**: AstraDB keySpace, ignored for non-AstraDB dialects. Defaults to empty.
* **astraDBRegion**: AstraDB region, ignored for non-AstraDB dialects. Defaults to empty.
* **disabledAlgorithms**: Comma separated algorithms to disable. If this value is set to `none`, no algorithm is disabled. Use this parameter with caution, because the algorithms disabled by default might have vulnerabilities or performance issues. For example, `SSLv3, RC4`.
* **extraFilesToStage**: Comma separated Cloud Storage paths or Secret Manager secrets for files to stage in the worker. These files are saved in the /extra_files directory in each worker. For example, `gs://<BUCKET_NAME>/file.txt,projects/<PROJECT_ID>/secrets/<SECRET_ID>/versions/<VERSION_ID>`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/SourceDbToSpanner.java)

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
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-f v2/sourcedb-to-spanner
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Sourcedb_to_Spanner_Flex
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Sourcedb_to_Spanner_Flex"

### Required
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export SOURCE_DB_DIALECT=MYSQL
export JDBC_DRIVER_JARS=""
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export SOURCE_CONFIG_URL=""
export USERNAME=""
export PASSWORD=""
export TABLES=""
export NUM_PARTITIONS=0
export FETCH_SIZE=<fetchSize>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export MAX_CONNECTIONS=0
export SESSION_FILE_PATH=""
export TRANSFORMATION_JAR_PATH=""
export TRANSFORMATION_CLASS_NAME=""
export TRANSFORMATION_CUSTOM_PARAMETERS=""
export NAMESPACE=""
export INSERT_ONLY_MODE_FOR_SPANNER_MUTATIONS=false
export BATCH_SIZE_FOR_SPANNER_MUTATIONS=-1
export SPANNER_PRIORITY=MEDIUM
export TABLE_OVERRIDES=""
export COLUMN_OVERRIDES=""
export SCHEMA_OVERRIDES_FILE_PATH=""
export UNIFORMIZATION_STAGE_COUNT_HINT=0
export ASTRA_DBTOKEN=""
export ASTRA_DBDATABASE_ID=""
export ASTRA_DBKEY_SPACE=""
export ASTRA_DBREGION=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

gcloud dataflow flex-template run "sourcedb-to-spanner-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceDbDialect=$SOURCE_DB_DIALECT" \
  --parameters "jdbcDriverJars=$JDBC_DRIVER_JARS" \
  --parameters "jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME" \
  --parameters "sourceConfigURL=$SOURCE_CONFIG_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "tables=$TABLES" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "fetchSize=$FETCH_SIZE" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "maxConnections=$MAX_CONNECTIONS" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "transformationJarPath=$TRANSFORMATION_JAR_PATH" \
  --parameters "transformationClassName=$TRANSFORMATION_CLASS_NAME" \
  --parameters "transformationCustomParameters=$TRANSFORMATION_CUSTOM_PARAMETERS" \
  --parameters "namespace=$NAMESPACE" \
  --parameters "insertOnlyModeForSpannerMutations=$INSERT_ONLY_MODE_FOR_SPANNER_MUTATIONS" \
  --parameters "batchSizeForSpannerMutations=$BATCH_SIZE_FOR_SPANNER_MUTATIONS" \
  --parameters "spannerPriority=$SPANNER_PRIORITY" \
  --parameters "tableOverrides=$TABLE_OVERRIDES" \
  --parameters "columnOverrides=$COLUMN_OVERRIDES" \
  --parameters "schemaOverridesFilePath=$SCHEMA_OVERRIDES_FILE_PATH" \
  --parameters "uniformizationStageCountHint=$UNIFORMIZATION_STAGE_COUNT_HINT" \
  --parameters "astraDBToken=$ASTRA_DBTOKEN" \
  --parameters "astraDBDatabaseId=$ASTRA_DBDATABASE_ID" \
  --parameters "astraDBKeySpace=$ASTRA_DBKEY_SPACE" \
  --parameters "astraDBRegion=$ASTRA_DBREGION" \
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
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export SOURCE_DB_DIALECT=MYSQL
export JDBC_DRIVER_JARS=""
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export SOURCE_CONFIG_URL=""
export USERNAME=""
export PASSWORD=""
export TABLES=""
export NUM_PARTITIONS=0
export FETCH_SIZE=<fetchSize>
export SPANNER_HOST=https://batch-spanner.googleapis.com
export MAX_CONNECTIONS=0
export SESSION_FILE_PATH=""
export TRANSFORMATION_JAR_PATH=""
export TRANSFORMATION_CLASS_NAME=""
export TRANSFORMATION_CUSTOM_PARAMETERS=""
export NAMESPACE=""
export INSERT_ONLY_MODE_FOR_SPANNER_MUTATIONS=false
export BATCH_SIZE_FOR_SPANNER_MUTATIONS=-1
export SPANNER_PRIORITY=MEDIUM
export TABLE_OVERRIDES=""
export COLUMN_OVERRIDES=""
export SCHEMA_OVERRIDES_FILE_PATH=""
export UNIFORMIZATION_STAGE_COUNT_HINT=0
export ASTRA_DBTOKEN=""
export ASTRA_DBDATABASE_ID=""
export ASTRA_DBKEY_SPACE=""
export ASTRA_DBREGION=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="sourcedb-to-spanner-flex-job" \
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-Dparameters="sourceDbDialect=$SOURCE_DB_DIALECT,jdbcDriverJars=$JDBC_DRIVER_JARS,jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME,sourceConfigURL=$SOURCE_CONFIG_URL,username=$USERNAME,password=$PASSWORD,tables=$TABLES,numPartitions=$NUM_PARTITIONS,fetchSize=$FETCH_SIZE,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,projectId=$PROJECT_ID,spannerHost=$SPANNER_HOST,maxConnections=$MAX_CONNECTIONS,sessionFilePath=$SESSION_FILE_PATH,outputDirectory=$OUTPUT_DIRECTORY,transformationJarPath=$TRANSFORMATION_JAR_PATH,transformationClassName=$TRANSFORMATION_CLASS_NAME,transformationCustomParameters=$TRANSFORMATION_CUSTOM_PARAMETERS,namespace=$NAMESPACE,insertOnlyModeForSpannerMutations=$INSERT_ONLY_MODE_FOR_SPANNER_MUTATIONS,batchSizeForSpannerMutations=$BATCH_SIZE_FOR_SPANNER_MUTATIONS,spannerPriority=$SPANNER_PRIORITY,tableOverrides=$TABLE_OVERRIDES,columnOverrides=$COLUMN_OVERRIDES,schemaOverridesFilePath=$SCHEMA_OVERRIDES_FILE_PATH,uniformizationStageCountHint=$UNIFORMIZATION_STAGE_COUNT_HINT,astraDBToken=$ASTRA_DBTOKEN,astraDBDatabaseId=$ASTRA_DBDATABASE_ID,astraDBKeySpace=$ASTRA_DBKEY_SPACE,astraDBRegion=$ASTRA_DBREGION,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
-f v2/sourcedb-to-spanner
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
cd v2/sourcedb-to-spanner/terraform/Sourcedb_to_Spanner_Flex
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

resource "google_dataflow_flex_template_job" "sourcedb_to_spanner_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Sourcedb_to_Spanner_Flex"
  name              = "sourcedb-to-spanner-flex"
  region            = var.region
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    projectId = "<projectId>"
    outputDirectory = "<outputDirectory>"
    # sourceDbDialect = "MYSQL"
    # jdbcDriverJars = ""
    # jdbcDriverClassName = "com.mysql.jdbc.Driver"
    # sourceConfigURL = ""
    # username = ""
    # password = ""
    # tables = ""
    # numPartitions = "0"
    # fetchSize = "<fetchSize>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # maxConnections = "0"
    # sessionFilePath = ""
    # transformationJarPath = ""
    # transformationClassName = ""
    # transformationCustomParameters = ""
    # namespace = ""
    # insertOnlyModeForSpannerMutations = "false"
    # batchSizeForSpannerMutations = "-1"
    # spannerPriority = "MEDIUM"
    # tableOverrides = ""
    # columnOverrides = ""
    # schemaOverridesFilePath = ""
    # uniformizationStageCountHint = "0"
    # astraDBToken = ""
    # astraDBDatabaseId = ""
    # astraDBKeySpace = ""
    # astraDBRegion = ""
    # disabledAlgorithms = "<disabledAlgorithms>"
    # extraFilesToStage = "<extraFilesToStage>"
  }
}
```
