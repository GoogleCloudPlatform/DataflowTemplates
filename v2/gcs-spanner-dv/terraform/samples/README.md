# Terraform Samples for GCS to Spanner Data Validation

This directory contains Terraform deployment samples to run the [GCS to Spanner Data Validation](../../README_GCS_Spanner_Data_Validator.md) Dataflow pipeline. 

The provided sample in [`gcs-spanner-dv`](./gcs-spanner-dv/) demonstrates how to easily launch the Dataflow job while automatically attaching all necessary IAM roles.

## What this sample does

The Terraform module will create the following Google Cloud resources:
1. **Dataflow Flex Template Job:** Uses `google_dataflow_flex_template_job` to launch the data validation pipeline.
2. **IAM Role Bindings:** Grants the Dataflow worker service account the required roles to run the validation:
   * `roles/dataflow.worker`
   * `roles/spanner.databaseAdmin`
   * `roles/storage.objectViewer`
   * `roles/bigquery.dataEditor` (required to write validation reports to BigQuery)

## Prerequisites

Before executing the sample, ensure you meet the following requirements:

1. **APIs Enabled**: The following APIs must be enabled in your Google Cloud Project:
   * Dataflow API (`dataflow.googleapis.com`)
   * Cloud Spanner API (`spanner.googleapis.com`)
   * Cloud Storage API (`storage-component.googleapis.com`)
   * BigQuery API (`bigquery.googleapis.com`)

2. **Terraform**: Make sure Terraform is installed locally and you are authenticated using `gcloud auth application-default login`.

## Step-by-Step Usage Instructions

1. **Navigate to the sample directory**
   ```shell
   cd gcs-spanner-dv/
   ```

2. **Initialize Terraform**
   Download the required providers and initialize the state.
   ```shell
   terraform init
   ```

3. **Configure the variables**
   Open the `terraform_simple.tfvars` file and modify the placeholder values (such as `project`, `instance_id`, `database_id`, and `gcs_input_directory`) to match your environment.

4. **Plan the deployment**
   Review the resources that Terraform will create:
   ```shell
   terraform plan -var-file=terraform_simple.tfvars
   ```

5. **Apply the deployment**
   Execute the creation of the resources:
   ```shell
   terraform apply -var-file=terraform_simple.tfvars
   ```
   
   Once the command completes, Terraform will output the `dataflow_job_url`, which you can click to view the running job in the Google Cloud Console.
