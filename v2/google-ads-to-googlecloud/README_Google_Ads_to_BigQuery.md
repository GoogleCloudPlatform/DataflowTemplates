
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

### Required Parameters

* **customerIds** (Google Ads account IDs): A list of Google Ads account IDs for which to execute the query. (Example: 12345,67890).
* **query** (Google Ads Query Language query): See https://developers.google.com/google-ads/api/docs/query/overview. (Example: SELECT campaign.id, campaign.name FROM campaign).
* **qpsPerWorker** (Required Google Ads request rate per worker): Indicates rate of query requests per second to be submitted to Google Ads. Divide the desired per pipeline QPS by the maximum number of workers. Avoid exceeding per account or developer token limits. See https://developers.google.com/google-ads/api/docs/best-practices/rate-limits.
* **googleAdsClientId** (OAuth 2.0 Client ID identifying the application): See https://developers.google.com/google-ads/api/docs/oauth/overview.
* **googleAdsClientSecret** (OAuth 2.0 Client Secret for the specified Client ID): See https://developers.google.com/google-ads/api/docs/oauth/overview.
* **googleAdsRefreshToken** (OAuth 2.0 Refresh Token for the user connecting to the Google Ads API): See https://developers.google.com/google-ads/api/docs/oauth/overview.
* **googleAdsDeveloperToken** (Google Ads developer token for the user connecting to the Google Ads API): See https://developers.google.com/google-ads/api/docs/get-started/dev-token.
* **outputTableSpec** (BigQuery output table): BigQuery table location to write the output to. The name should be in the format `<project>:<dataset>.<table_name>`. The table's schema must match input objects.

### Optional Parameters

* **loginCustomerId** (Google Ads manager account ID): A Google Ads manager account ID for which to access the account IDs. (Example: 12345).
* **bigQueryTableSchemaPath** (BigQuery Table Schema Path): Cloud Storage path to the BigQuery schema JSON file. If this is not set, then the schema is inferred from the Proto schema. (Example: gs://MyBucket/bq_schema.json).
* **writeDisposition** (Write Disposition to use for BigQuery): BigQuery WriteDisposition. For example, WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE. Defaults to: WRITE_APPEND.
* **createDisposition** (Create Disposition to use for BigQuery): BigQuery CreateDisposition. For example, CREATE_IF_NEEDED, CREATE_NEVER. Defaults to: CREATE_IF_NEEDED.



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
-DtemplateName="Google_Ads_to_BigQuery" \
-pl v2/google-ads-to-googlecloud \
-am
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
-pl v2/google-ads-to-googlecloud \
-am
```

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
