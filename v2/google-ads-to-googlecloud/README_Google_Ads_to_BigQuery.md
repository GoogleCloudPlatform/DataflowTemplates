
Google Ads to BigQuery template
---
The Google Ads to BigQuery template is a batch pipeline that reads Google Ads
reports and writes to BigQuery.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/google-ads-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Google_Ads_to_BigQuery).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **customerIds** : A list of Google Ads account IDs to use to execute the query. (Example: 12345,67890).
* **query** : The query to use to get the data. See Google Ads Query Language. For example: `SELECT campaign.id, campaign.name FROM campaign`. (Example: SELECT campaign.id, campaign.name FROM campaign).
* **qpsPerWorker** : The rate of query requests per second (QPS) to submit to Google Ads.  Divide the desired per pipeline QPS by the maximum number of workers. Avoid exceeding per-account or developer token limits. See Rate Limits (https://developers.google.com/google-ads/api/docs/best-practices/rate-limits).
* **googleAdsClientId** : The OAuth 2.0 client ID that identifies the application. See Create a client ID and client secret (https://developers.google.com/google-ads/api/docs/oauth/cloud-project#create_a_client_id_and_client_secret).
* **googleAdsClientSecret** : The OAuth 2.0 client secret that corresponds to the specified client ID. See Create a client ID and client secret (https://developers.google.com/google-ads/api/docs/oauth/cloud-project#create_a_client_id_and_client_secret).
* **googleAdsRefreshToken** : The OAuth 2.0 refresh token to use to connect to the Google Ads API. See 2-Step Verification (https://developers.google.com/google-ads/api/docs/oauth/2sv).
* **googleAdsDeveloperToken** : The Google Ads developer token to use to connect to the Google Ads API. See Obtain a developer token (https://developers.google.com/google-ads/api/docs/get-started/dev-token).
* **outputTableSpec** : The BigQuery output table location to write the output to. For example, `<PROJECT_ID>:<DATASET_NAME>.<TABLE_NAME>`.Depending on the `createDisposition` specified, the output table might be created automatically using the user provided Avro schema.

### Optional parameters

* **loginCustomerId** : A Google Ads manager account ID to use to access the account IDs. (Example: 12345).
* **bigQueryTableSchemaPath** : The Cloud Storage path to the BigQuery schema JSON file. If this value is not set, then the schema is inferred from the Proto schema. (Example: gs://MyBucket/bq_schema.json).
* **writeDisposition** : The BigQuery WriteDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload) value. For example, `WRITE_APPEND`, `WRITE_EMPTY`, or `WRITE_TRUNCATE`. Defaults to `WRITE_APPEND`.
* **createDisposition** : The BigQuery CreateDisposition (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload). For example, `CREATE_IF_NEEDED` and `CREATE_NEVER`. Defaults to `CREATE_IF_NEEDED`.



## Getting Started

### Requirements

* Java 11
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/google-ads-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/GoogleAdsToBigQuery.java)

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
-DtemplateName="Google_Ads_to_BigQuery" \
-f v2/google-ads-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Google_Ads_to_BigQuery
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
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Google_Ads_to_BigQuery"

### Required
export CUSTOMER_IDS=<customerIds>
export QUERY=<query>
export QPS_PER_WORKER=<qpsPerWorker>
export GOOGLE_ADS_CLIENT_ID=<googleAdsClientId>
export GOOGLE_ADS_CLIENT_SECRET=<googleAdsClientSecret>
export GOOGLE_ADS_REFRESH_TOKEN=<googleAdsRefreshToken>
export GOOGLE_ADS_DEVELOPER_TOKEN=<googleAdsDeveloperToken>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export LOGIN_CUSTOMER_ID=<loginCustomerId>
export BIG_QUERY_TABLE_SCHEMA_PATH=<bigQueryTableSchemaPath>
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED

gcloud dataflow flex-template run "google-ads-to-bigquery-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "loginCustomerId=$LOGIN_CUSTOMER_ID" \
  --parameters "customerIds=$CUSTOMER_IDS" \
  --parameters "query=$QUERY" \
  --parameters "qpsPerWorker=$QPS_PER_WORKER" \
  --parameters "bigQueryTableSchemaPath=$BIG_QUERY_TABLE_SCHEMA_PATH" \
  --parameters "googleAdsClientId=$GOOGLE_ADS_CLIENT_ID" \
  --parameters "googleAdsClientSecret=$GOOGLE_ADS_CLIENT_SECRET" \
  --parameters "googleAdsRefreshToken=$GOOGLE_ADS_REFRESH_TOKEN" \
  --parameters "googleAdsDeveloperToken=$GOOGLE_ADS_DEVELOPER_TOKEN" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "writeDisposition=$WRITE_DISPOSITION" \
  --parameters "createDisposition=$CREATE_DISPOSITION"
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
export CUSTOMER_IDS=<customerIds>
export QUERY=<query>
export QPS_PER_WORKER=<qpsPerWorker>
export GOOGLE_ADS_CLIENT_ID=<googleAdsClientId>
export GOOGLE_ADS_CLIENT_SECRET=<googleAdsClientSecret>
export GOOGLE_ADS_REFRESH_TOKEN=<googleAdsRefreshToken>
export GOOGLE_ADS_DEVELOPER_TOKEN=<googleAdsDeveloperToken>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export LOGIN_CUSTOMER_ID=<loginCustomerId>
export BIG_QUERY_TABLE_SCHEMA_PATH=<bigQueryTableSchemaPath>
export WRITE_DISPOSITION=WRITE_APPEND
export CREATE_DISPOSITION=CREATE_IF_NEEDED

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="google-ads-to-bigquery-job" \
-DtemplateName="Google_Ads_to_BigQuery" \
-Dparameters="loginCustomerId=$LOGIN_CUSTOMER_ID,customerIds=$CUSTOMER_IDS,query=$QUERY,qpsPerWorker=$QPS_PER_WORKER,bigQueryTableSchemaPath=$BIG_QUERY_TABLE_SCHEMA_PATH,googleAdsClientId=$GOOGLE_ADS_CLIENT_ID,googleAdsClientSecret=$GOOGLE_ADS_CLIENT_SECRET,googleAdsRefreshToken=$GOOGLE_ADS_REFRESH_TOKEN,googleAdsDeveloperToken=$GOOGLE_ADS_DEVELOPER_TOKEN,outputTableSpec=$OUTPUT_TABLE_SPEC,writeDisposition=$WRITE_DISPOSITION,createDisposition=$CREATE_DISPOSITION" \
-f v2/google-ads-to-googlecloud
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
cd v2/google-ads-to-googlecloud/terraform/Google_Ads_to_BigQuery
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

resource "google_dataflow_flex_template_job" "google_ads_to_bigquery" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Google_Ads_to_BigQuery"
  name              = "google-ads-to-bigquery"
  region            = var.region
  parameters        = {
    customerIds = "12345,67890"
    query = "SELECT campaign.id, campaign.name FROM campaign"
    qpsPerWorker = "<qpsPerWorker>"
    googleAdsClientId = "<googleAdsClientId>"
    googleAdsClientSecret = "<googleAdsClientSecret>"
    googleAdsRefreshToken = "<googleAdsRefreshToken>"
    googleAdsDeveloperToken = "<googleAdsDeveloperToken>"
    outputTableSpec = "<outputTableSpec>"
    # loginCustomerId = "12345"
    # bigQueryTableSchemaPath = "gs://MyBucket/bq_schema.json"
    # writeDisposition = "WRITE_APPEND"
    # createDisposition = "CREATE_IF_NEEDED"
  }
}
```
