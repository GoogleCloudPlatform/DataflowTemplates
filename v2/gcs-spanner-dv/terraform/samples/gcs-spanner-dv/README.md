# GCS to Spanner Data Validation

The provided sample in [`gcs-spanner-dv`](./) demonstrates how to easily launch the Dataflow job while automatically attaching all necessary IAM roles.

## What this sample does

The Terraform module will create the following Google Cloud resources:
1. **Dataflow Flex Template Job:** Uses `google_dataflow_flex_template_job` to launch the data validation pipeline.
2. **IAM Role Bindings:** Grants the Dataflow worker service account the required roles to run the validation:
   * `roles/dataflow.worker` (required to execute Dataflow jobs)
   * `roles/spanner.databaseReader` (required to read records from Spanner)
   * `roles/storage.objectAdmin` (required to read/write objects in Cloud Storage)
   * `roles/bigquery.dataEditor` (required to write validation reports to BigQuery)
   * `roles/bigquery.jobUser` (required to execute BigQuery load jobs)
   * `roles/monitoring.metricWriter` (required to write Dataflow metrics)
   * `roles/cloudprofiler.agent` (required for Cloud Profiler)

## Prerequisites

Before executing the sample, ensure you meet the following requirements:

1. **APIs Enabled**: The following APIs must be enabled in your Google Cloud Project:
   * Dataflow API (`dataflow.googleapis.com`)
   * Cloud Spanner API (`spanner.googleapis.com`)
   * Cloud Storage API (`storage.googleapis.com`)
   * BigQuery API (`bigquery.googleapis.com`)

2. **Terraform**: Make sure Terraform is installed locally and you are authenticated using `gcloud auth application-default login`.

## Usage

1. **Clone the repository**
   ```shell
   git clone https://github.com/GoogleCloudPlatform/DataflowTemplates.git
   cd DataflowTemplates/v2/gcs-spanner-dv/terraform/samples/gcs-spanner-dv
   ```

2. **Initialize Terraform**
   ```shell
   terraform init
   ```

3. **Configure the variables**
   Open the `terraform_simple.tfvars` file and modify the placeholder values (such as `project`, `instance_id`, `database_id`, `gcs_input_directory`, and `bigquery_dataset`) to match your environment.

4. **Review the execution plan**
   ```shell
   terraform plan -var-file=terraform_simple.tfvars
   ```

5. **Apply the configuration**
   ```shell
   terraform apply -var-file=terraform_simple.tfvars
   ```
