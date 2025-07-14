
SourceDB to Spanner template
---
The SourceDB to Spanner template is a batch pipeline that copies data from a
relational database into an existing Spanner database. This pipeline uses JDBC to
connect to the relational database. You can use this template to copy data from
any relational database with available JDBC drivers into Spanner.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Sourcedb_to_Spanner_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

#### Required Parameters
* **sourceConfigURL** (Configuration to connect to the source database): Can be the JDBC URL or the location of the sharding config. (Example: jdbc:mysql://10.10.10.10:3306/testdb or gs://test1/shard.conf)
* **username** (username of the source database): The username which can be used to connect to the source database.
* **password** (username of the source database): The username which can be used to connect to the source database.
* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.
* **outputDirectory** (GCS path of the output directory): The GCS path of the Directory where all error and skipped events are dumped to be used during migrations

#### Optional Parameters
* **jdbcDriverJars** (Comma-separated Cloud Storage path(s) of the JDBC driver(s)): The comma-separated list of driver JAR files. (Example: gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar).
* **jdbcDriverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **tables** (Colon seperated list of tables to migrate): Tables that will be migrated to Spanner. Leave this empty if all tables are to be migrated. (Example: table1:table2).
* **numPartitions** (Number of partitions to create per table): A table is split into partitions and loaded independently. Use higher number of partitions for larger tables. (Example: 1000).
* **spannerHost** (Cloud Spanner Endpoint): Use this endpoint to connect to Spanner. (Example: https://batch-spanner.googleapis.com)
* **maxConnections** (Number of connections to create per source database): The max number of connections that can be used at any given time at source. (Example: 100)
* **sessionFilePath** (GCS path of the session file): The GCS path of the schema mapping file to be used during migrations



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!
[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/JdbcToSpanner.java)

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
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-pl v2/sourcedb-to-spanner \
-am
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
export SOURCE_CONFIG_URL=<sourceConfigURL>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export JDBC_DRIVER_JARS=""
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export USERNAME=""
export PASSWORD=""
export TABLES=""
export NUM_PARTITIONS=0
export SPANNER_HOST=https://batch-spanner.googleapis.com
export MAX_CONNECTIONS=0
export SESSION_FILE_PATH=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export DEFAULT_LOG_LEVEL=INFO

gcloud dataflow flex-template run "sourcedb-to-spanner-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --additional-experiments="[\"disable_runner_v2\"]" \
  --parameters "jdbcDriverJars=$JDBC_DRIVER_JARS" \
  --parameters "jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME" \
  --parameters "sourceConfigURL=$SOURCE_CONFIG_URL" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "tables=$TABLES" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "maxConnections=$MAX_CONNECTIONS" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
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
export SOURCE_CONFIG_URL=<sourceConfigURL>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
export JDBC_DRIVER_JARS=""
export JDBC_DRIVER_CLASS_NAME=com.mysql.jdbc.Driver
export USERNAME=""
export PASSWORD=""
export TABLES=""
export NUM_PARTITIONS=0
export SPANNER_HOST=https://batch-spanner.googleapis.com
export MAX_CONNECTIONS=0
export SESSION_FILE_PATH=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export DEFAULT_LOG_LEVEL=INFO

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="sourcedb-to-spanner-flex-job" \
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-Dparameters="jdbcDriverJars=$JDBC_DRIVER_JARS,jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME,sourceConfigURL=$SOURCE_CONFIG_URL,username=$USERNAME,password=$PASSWORD,tables=$TABLES,numPartitions=$NUM_PARTITIONS,instanceId=$INSTANCE_ID,databaseId=$DATABASE_ID,projectId=$PROJECT_ID,spannerHost=$SPANNER_HOST,maxConnections=$MAX_CONNECTIONS,sessionFilePath=$SESSION_FILE_PATH,outputDirectory=$OUTPUT_DIRECTORY,disabledAlgorithms=$DISABLED_ALGORITHMS,extraFilesToStage=$EXTRA_FILES_TO_STAGE,defaultLogLevel=$DEFAULT_LOG_LEVEL" \
-f v2/sourcedb-to-spanner
```

## Cassandra to Spanner Bulk Migration
### Prerequisites
For bulk data migration from Cassandra to spanner, here are a few prerequisites you will need:

#### Prerequisite-1: Network Connectivity
1. Choose a VPC in the project where you would like to run the dataflow job (default is the VPC named `default` in the project).
2. Ensure that the VPC has network connectivity to nodes in your Cassandra Cluster. Depending on where your Cassandra Cluster is hosted, you might need one or more of the following steps:

       * Configure Firewalls in your GCP project and the place where the Cassandra Cluster is hosted to allow egress access from the Dataflow VPC to Cassandra nodes. The usual port used by Cassandra for Client/Server communication is 9042.
       * Network Peering between the VPCs where Cassandra is hosted and the dataflow VPC.
#### Prerequisite-2: Configuration File
You will need to upload the Cassandra driver configuration file to GCS.
You might already be using one for your production application, but in case you need to create one, you could refer to the [reference.conf](https://github.com/apache/cassandra-java-driver/blob/4.x/core/src/main/resources/reference.conf).
This configuration file would be referred by the Dataflow job to infer various client side parameters need to connect to Cassandra cluster.
For the smooth execution of the migration, ensure that these basic parameters are correct:
1. basic.contact-points: `basic.contact-points` must point to the ip addresses and port number of the nodes of the Cassandra cluster which are discoverable from the VPC where you will run the Dataflow job.
2. basic.session-keyspace: `basic.session-keyspace` much point to the keyspace which you would like to migrate to spanner as a part of this run. Note that a single run of a Dataflow job can migration one or more tables within a single Cassandra keyspace.
3. basic.load-balancing-policy.local-datacenter: `basic.load-balancing-policy.local-datacenter` must point to to the appropriate Cassandra datacenter.
4. Optionally, you would also need to ensure that these parameters are updated correctly:

       * `advanced.auth-provider`: with username/password credentials.
       * `basic.request.timeout`: keep this large enough for reading ring rages. If unset, defaults to 3600_000 milliseconds.
       * `connection.connect-timeout`: timeout for socket connection. If unset, defaults to 10_000 milliseconds.
#### Prerequisite-3: Cassandra Cluster Health
Please ensure the Cassandra Cluster you are migrating from is healthy and the tables you are migrating don't need any repairs for best performance.
#### Prerequisite-4: Spanner
You will need to provision a spanner database where you would like to migrate the data. The database would need to have tables with a schema that maps to the schema on the source.
The tables which are present both on Spanner and Cassandra would be the ones that are migrated.
#### Prerequisite-5: GCS
You would need a GCS bucket to stage your build, driver configuration file, and provide an output directory for DLQs.
### Run Migration

**Using the staged template**:

Follow [above](#staging-the-template) to build the template and stage it in GCS.
This step prints the path of the staged template which is passed as `TEMPLATE_SPEC_GCSPATH` below.

To start a job with the staged template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
### Basic Job Paramters
export PROJECT=<your-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Sourcedb_to_Spanner_Flex"
### The number of works controls the fanout of Dataflow job to read from Cassandra.
### While you might need to finetune this for best performance, a number close to number of nodes on Cassandra Cluster might be good place to start.
export MAX_WORKERS="<MAX_NUMBER_OF_DATAFLOW_WORKERS_TO_READ_FROM_CASSANDRA>"
export NUM_WORKERS="<INITIAL_NUMBER_OF_DATAFLOW_WORKERS_TO_READ_FROM_CASSANDRA>"
### The type of machine. `e2-standard-32` might be good starting point for most use cases.
eport  MACHINE_TYPE="<WORKER_MACHINE_TYPE>"

### Required
export SOURCE_CONFIG_URL=<gs://path/to/Cassandra-Driver-Config-File>
export INSTANCE_ID=<instanceId>
export DATABASE_ID=<databaseId>
export PROJECT_ID=<projectId>
#### Stores DLQ.
export OUTPUT_DIRECTORY=<outputDirectory>

### Optional
#### Use A session file in case you would like the Cassandra and Spanner Tables to have different names.
export SESSION_FILE_PATH=""
export DISABLED_ALGORITHMS=<disabledAlgorithms>
export EXTRA_FILES_TO_STAGE=<extraFilesToStage>
export DEFAULT_LOG_LEVEL=INFO
#### Set insert only mode to true, in case you would run bulk migration in parallel to dual writes.
#### This mode stops the bulk template from overwriting rows that already exist in spanner.
#### If you are not replicating live changes to spanner in parallel, you could choose to set this mode to false.
#### Setting this mode to false causes the bulk template to overwrite existing rows in spanner.
#### false is the default if unset.
export INSERT_ONLY_MODE_FOR_SPANNER_MUTATIONS="true"
#### Region for Dataflow workers (Required ony if you want to configure network and subnetwork.
expoert WORKER_REGION="${REGION}"
#### Network where you would like to run Dataflow. Defaults to default. This VPC must have access to Cassandra nodes you would like to migrate from.
export NETWORK="<VPC_NAME>"
#### Subnet where you would like to run Dataflow. Defaults to default. This subnet must have access to Cassandra nodes you would like to migrate from.
export SUBNETWORK="regions/${WORKER_REGION}/subnetworks/<SUBNET_NAME>"
#### Number of partitions for parallel read.
##### By default Apache Beam's CassandraIO sets NUM_PARTITIONS equals to number
##### of nodes on the Cassandra Cluster. This default does not give good performance
##### larger workloads as it limits the parallelization.
##### While specifcs would depend on many factors like number of Cassandra nodes, distribution
##### of partitions of the table across the nodes,
##### In general a partition of average size of 150 MB gives good throughput and might be a good place to start the fine-tuning.
NUM_PARTITIONS="<NUM_PARTITIONS>"
#### Disable Spanner Batch Writes.
BATCH_SIZE_FOR_SPANNER_MUTATIONS=1

gcloud dataflow flex-template run "sourcedb-to-spanner-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --network "$NETWORK" \
  --max-workers "$MAX_WORKERS" \
  --num-workers "$NUM_WORKERS" \
  --worker-machine-type "$MACHINE_TYPE" \
  --subnetwork "$SUBNETWORK" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --additional-experiments="[\"disable_runner_v2\"]" \
  --parameters "sourceDbDialect=CASSANDRA" \
  --parameters "insertOnlyModeForSpannerMutations=$INSERT_ONLY_MODE_FOR_SPANNER_MUTATIONS" \
  --parameters "sourceConfigURL=$SOURCE_CONFIG_URL" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "disabledAlgorithms=$DISABLED_ALGORITHMS" \
  --parameters "extraFilesToStage=$EXTRA_FILES_TO_STAGE" \
  --parameters "defaultLogLevel=$DEFAULT_LOG_LEVEL" \
  --parameters "numPartitions=${NUM_PARTITIONS}" \
  --parameters "batchSizeForSpannerMutations=${BATCH_SIZE_FOR_SPANNER_MUTATIONS}"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run
### Troubleshooting Cassandra Bulk Migration.
#### Read Timeout
It's possible that long-running range reads cause Read timeouts on Cassandra servers.
This would appear as `com.datastax.driver.core.exceptions.ReadTimeoutException` in the worker logs.
In case your job fails due to many exceptions like the above, here are a few steps you can follow for mitigation:
1. Increase the timeouts in the driver configuration file which are described in the [configuration-file](#prerequisite-2--configuration-file) section above.
2. Increase read_request_timeout and range_request_timeout on the server side.
3. In case the iops, or, data throughput of the dataflow job is close to the max throughput supported by your cassandra cluster, consider reducing  `maxWorkers` to limit the fanout of the dataflow job.
4. By default, Apache Beam sets `numPartitions` same as the number of Cassandra hosts. Increasing this by setting `numPartitions` can make each read smaller thereby avoiding the timeout.
#### Throughput on Spanner raises and falls in sharp bursts
It's possible that the default configuration could lead to spanner throughput raise and fall in sharp bursts. In case this is observed, you can disable spanner batch writes by setting `batchSizeForSpannerMutations` as 0.


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

resource "google_dataflow_flex_template_job" "sourcedb_to_spanner_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Sourcedb_to_Spanner_Flex"
  name              = "sourcedb-to-spanner-flex"
  region            = var.region
  parameters        = {
    instanceId = "<instanceId>"
    databaseId = "<databaseId>"
    projectId = "<projectId>"
    sourceConfigURL = "jdbc:mysql://some-host:3306/sampledb"
    username = "<username>"
    password = "<password>"
    outputDirectory = "gs://your-bucket/dir"

    # jdbcDriverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    # jdbcDriverClassName = "com.mysql.jdbc.Driver"
    # tables = "<tables>"
    # numPartitions = "<numPartitions>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # maxConnections = 100
    # disabledAlgorithms = "SSLv3, RC4"
    # sessionFilePath = "gs://your-bucket/file.txt"
  }
}
```
