
Sourcedb to Spanner template
---
The SourceDB to Spanner template is a batch pipeline that copies data from a
relational database into an existing Spanner database. This pipeline uses JDBC to
connect to the relational database. You can use this template to copy data from
any relational database with available JDBC drivers into Spanner. This currently
only supports a limited set of types of MySQL.

For an extra layer of protection, you can also pass in a Cloud KMS key along with
a Base64-encoded username, password, and connection string parameters encrypted
with the Cloud KMS key. See the <a
href="https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt">Cloud
KMS API encryption endpoint</a> for additional details on encrypting your
username, password, and connection string parameters.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/sourcedb-to-spanner)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Sourcedb_to_Spanner_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required Parameters
* **sourceDbURL** (JDBC URL of the source database): The URL which can be used to connect to the source database. (Example: jdbc:mysql://10.10.10.10:3306/testdb)
* **username** (username of the source database): The username which can be used to connect to the source database.
* **password** (username of the source database): The username which can be used to connect to the source database.
* **instanceId** (Cloud Spanner Instance Id.): The destination Cloud Spanner instance.
* **databaseId** (Cloud Spanner Database Id.): The destination Cloud Spanner database.
* **projectId** (Cloud Spanner Project Id.): This is the name of the Cloud Spanner project.

### Optional Parameters
* **jdbcDriverClassName** (JDBC driver class name): The JDBC driver class name. (Example: com.mysql.jdbc.Driver).
* **tables** (Comma seperated list of tables to migrate): Tables that will be migrated to Spanner. Leave this empty if all tabels are to be migrated. (Example: table1,table2).
* **tables** (Number of partitions to create per table): A table is split into partitions and loaded independently. Use higher number of partitions for larger tables. (Example: 1000).
* **spannerHost** (Cloud Spanner Endpoint): Use this endpoint to connect to Spanner. (Example: https://batch-spanner.googleapis.com)
* **maxConnections** (Number of connections to create per source database): The max number of connections that can be used at any given time at source. (Example: 100)
* **sessionFilePath** (GCS path of the session file): The GCS path of the schema mapping file to be used during migrations
* **DLQDirectory** (GCS path of the dead letter queue direcotry): The GCS path of the schema mapping file to be used during migrations



## Getting Started

### Requirements

* Java 11
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
# Adding timestamp just allows the name to be unique across multiple runs
JOB_NAME="<job-name>-`date +'%Y-%m-%d-%H-%M-%S-%N'`"
TEMPLATE_NAME="Sourcedb_to_Spanner_Flex"
PROJECT="<GCP-PROJECT-NAME>"

# Note this changes the default project of your bash session.
gcloud config set project $PROJECT


# Below are GCS paths, they could be in same bucket separated by folders.
TEMPLATE_BUCKET_NAME="<GCS_BUCKET_WHERE_TEMPLATE_IS_BUILT>"
TEMPLATE_PATH="${TEMPLATE_BUCKET_NAME}/templates/flex/${TEMPLATE_NAME}"
DLQ_DIRECTORY="<GCS_PATH_FOR_DLQ>"


# Source Parameters
SRC_USER_ID="<SOURCE_DB_USER_ID>"
SRC_PWD="<SOURCE_PASSWORD>"
SRC_IP="<SOURCE_ID>"
SOURCE_HOST="${SRC_IP}"
SOURCE_PORT="3306"
SOURCE_DB="<NAME_OF_SOURCE_DB>"

# Spanner details
SPANNER_INSTANCE_ID="<SPANNER_INSTANCE_ID>"
SPANNER_DB_ID="<SPANNER_DB_ID>"


# Dataflow Parameters
WORKER_REGION="<REGION_TO_SPAWN_DATAFLOW_WORKERS like us-central1>"
NETWORK="<VPC network to spawn dataflow workers>"
SUBNETWORK="regions/${WORKER_REGION}/subnetworks/<SUBNET_NAME>"

# Typical Configuration for dataflow scaling, please change as per load
MAX_WORKERS=10
NUM_WORKERS=10
WORKER_MACHINE_TYPE="n1-highmem-32"
# Override NUM_PARTITIONS for tall tables.
# NUM_PARTITIONS="4000" 

gcloud dataflow flex-template run "sourcedb-to-spanner-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "sourceHost=jdbc:mysql://${SOURCE_IP}:${SOURCE_PORT}/${SOURCE_DB}" \
  --parameters "username=$USERNAME" \
  --parameters "password=$PASSWORD" \
  --parameters "instanceId=$INSTANCE_ID" \
  --parameters "databaseId=$DATABASE_ID" \
  --parameters "projectId=$PROJECT_ID" \
  --parameters "jdbcDriverClassName=$JDBC_DRIVER_CLASS_NAME" \
  --parameters "tables=$TABLES" \
  --parameters "numPartitions=$NUM_PARTITIONS" \
  --parameters "spannerHost=$SPANNER_HOST" \
  --parameters "maxConnections=$MAX_CONNECTIONS" \
  --parameters "sessionFilePath=$SESSION_FILE_PATH" \
  --parameters "DLQDirectory=$DLQ_DIRECTORY"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
# Adding timestamp just allows the name to be unique across multiple runs
JOB_NAME="<job-name>-`date +'%Y-%m-%d-%H-%M-%S-%N'`"
TEMPLATE_NAME="Sourcedb_to_Spanner_Flex"
PROJECT="<GCP-PROJECT-NAME>"

# Note this changes the default project of your bash session.
gcloud config set project $PROJECT


# Below are GCS paths, they could be in same bucket separated by folders.
TEMPLATE_BUCKET_NAME="<GCS_BUCKET_WHERE_TEMPLATE_IS_BUILT>"
TEMPLATE_PATH="${TEMPLATE_BUCKET_NAME}/templates/flex/${TEMPLATE_NAME}"
DLQ_DIRECTORY="<GCS_PATH_FOR_DLQ>"


# Source Parameters
SRC_USER_ID="<SOURCE_DB_USER_ID>"
SRC_PWD="<SOURCE_PASSWORD>"
SRC_IP="<SOURCE_ID>"
SOURCE_HOST="${SRC_IP}"
SOURCE_PORT="3306"
SOURCE_DB="<NAME_OF_SOURCE_DB>"

# Spanner details
SPANNER_INSTANCE_ID="<SPANNER_INSTANCE_ID>"
SPANNER_DB_ID="<SPANNER_DB_ID>"


# Dataflow Parameters
WORKER_REGION="<REGION_TO_SPAWN_DATAFLOW_WORKERS like us-central1>"
NETWORK="<VPC network to spawn dataflow workers>"
SUBNETWORK="regions/${WORKER_REGION}/subnetworks/<SUBNET_NAME>"

# Typical Configuration for dataflow scaling, please change as per load
MAX_WORKERS=10
NUM_WORKERS=10
WORKER_MACHINE_TYPE="n1-highmem-32"
# Override NUM_PARTITIONS for tall tables.
# NUM_PARTITIONS="4000" 

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="sourcedb-to-spanner-flex-job" \
-DtemplateName="Sourcedb_to_Spanner_Flex" \
-Dparameters="sourceDbURL=jdbc:mysql://$SOURCE_HOST:$SOURCE_PORT/$SOURCE_DB",username=$SRC_USER_ID,password=$SOURCE_PWD,instanceId=$SPANNER_INSTANCE_ID,databaseId=$SPANNER_DB_ID,projectId=$PROJECT,DLQDirectory=$DLQ_DIRECTORY 
--additional-experiments=disable_runner_v2 \
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
    sourceDbURL = "jdbc:mysql://some-host:3306/sampledb"
    username = "<username>"
    password = "<password>"

    # jdbcDriverJars = "gs://your-bucket/driver_jar1.jar,gs://your-bucket/driver_jar2.jar"
    # jdbcDriverClassName = "com.mysql.jdbc.Driver"
    # tables = "<tables>"
    # numPartitions = "<numPartitions>"
    # spannerHost = "https://batch-spanner.googleapis.com"
    # maxConnections = 100
    # disabledAlgorithms = "SSLv3, RC4"
    # sessionFilePath = "gs://your-bucket/file.txt"
    # DLQDirectory = "gs://your-bucket/dir"
  }
}
```
