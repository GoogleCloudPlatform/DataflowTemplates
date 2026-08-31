# GCS to Spanner Data Validation

This sample demonstrates how to easily launch the Dataflow job while automatically attaching all necessary IAM roles.

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

## Terraform permissions

In order to create the resources in this sample, the `Service account`/`User account` being used to run Terraform should have the required permissions.
There are two ways to add permissions -

1. Adding pre-defined roles to the service account running Terraform.
2. Creating a custom role with the granular permissions and attaching it to the service account running Terraform.

### Using custom role and granular permissions (recommended)

Following permissions are required -

```shell
- dataflow.jobs.cancel
- dataflow.jobs.create
- dataflow.jobs.updateContents
- iam.roles.get
- iam.serviceAccounts.actAs
- resourcemanager.projects.setIamPolicy
- storage.objects.create
- storage.objects.delete
- serviceusage.services.use
- serviceusage.services.enable
```

**Note**: Add the `roles/viewer` role as well to the service account.

### Using pre-defined roles

Following roles are required -

```shell
roles/dataflow.admin 
roles/iam.securityAdmin
roles/iam.serviceAccountUser
roles/storage.admin
roles/viewer
```

## Dataflow permissions

The Dataflow service account needs to be provided with the required roles. This sample will attempt to automatically bind the following roles to the specified service account: `roles/dataflow.worker`, `roles/spanner.databaseReader`, `roles/storage.objectAdmin`, `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/monitoring.metricWriter`, and `roles/cloudprofiler.agent`.

## Assumptions

It makes the following assumptions -

1. Appropriate permissions are added to the service account running Terraform to allow resource creation.
2. The BigQuery dataset to store validation reports is already created and correctly named in `var.bigquery_dataset`.
3. The Source AVRO records in GCS exist in `var.gcs_input_directory`.
4. A Spanner instance with database containing the destination records is created and accessible.
5. If the source and Spanner schema is not like-to-like (e.g., column/table renames), an SMT generated session file, or an overrides file needs to be provided containing the schema mapping information.

Given these assumptions, the job compares the source AVRO records against the Spanner database and writes mismatch reports and validation statistics to the specified BigQuery dataset.

## Description

This sample contains the following files -

1. `main.tf` - This contains the Terraform resources which will be created.
2. `outputs.tf` - This declares the outputs that will be output as part of running this terraform example.
3. `variables.tf` - This declares the input variables that are required to configure the resources.
4. `terraform.tf` - This contains the required providers and APIs/project configurations for this sample.
5. `terraform.tfvars` - This contains the minimal list of dummy inputs that need to be populated to run this example.

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
   cd DataflowTemplates/v2/gcs-spanner-dv/terraform/samples/simple-validation-job
   ```

2. **Initialize Terraform**
   ```shell
   terraform init
   ```

3. **Configure the variables**
   Open the `terraform.tfvars` file and modify the placeholder values (such as `project`, `instance_id`, `database_id`, `gcs_input_directory`, and `bigquery_dataset`) to match your environment.

4. **Review the execution plan**
   ```shell
   terraform plan -var-file=terraform.tfvars
   ```

5. **Apply the configuration**
   ```shell
   terraform apply -var-file=terraform.tfvars
   ```

This will launch the configured jobs and produce an output like below -

```shell
Apply complete! Resources: 1 added, 0 changed, 0 destroyed.

Outputs:

dataflow_job_id = [
  "2024-06-05_00_41_11-4759981257849547781",
]
dataflow_job_url = [
  "https://console.cloud.google.com/dataflow/jobs/us-central1/2024-06-05_00_41_11-4759981257849547781",
]
```

### Cleanup

Once the jobs have finished running, you can cleanup by running -

```shell
terraform destroy
```

## Observability

To monitor the data validation job, you can view the Dataflow job in the Google Cloud Console. The resulting metrics from the validation job such as matched records, mismatched records, and missing records are available in the BigQuery dataset specified in `var.bigquery_dataset`.

### Example BigQuery Queries

You can run the following SQL queries in the BigQuery console to inspect the validation results:

**1. Check overall validation summary per table:**

```sql
SELECT 
  table_name, 
  status, 
  matched_row_count,
  mismatch_row_count
FROM 
  `<YOUR_PROJECT_ID>.<YOUR_BIGQUERY_DATASET>.TableValidationStats`
ORDER BY 
  table_name;
```

**2. Inspect specific mismatched records:**

```sql
SELECT 
  table_name, 
  mismatch_type, 
  record_key, 
  source, 
  hash 
FROM 
  `<YOUR_PROJECT_ID>.<YOUR_BIGQUERY_DATASET>.MismatchedRecords` 
LIMIT 100;
```

## FAQ

### Dataflow job is failing with "Timeout in polling result file"

Dataflow has a 10-minute timeout within which the launcher VM logic should complete. There could be multiple reasons for it to take over 10 mins:

- **Job logs not present after the log "launcher VM started":** A sign would be there are only 3-4 log statements in the job logs. This is likely due to private Google access not being enabled for the subnetwork. Please enable private Google access in your network.

### Job graph is not loading/Custom counters not visible on Dataflow panel

For very large graphs, this can happen. The graph section would be empty and the counters won't load. But worry not, the validation should progress nonetheless. In such cases, the Dataflow custom metrics can be directly viewed on [Cloud monitoring](https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf).

### Data Validation is taking too long

There can be multiple reasons for this:

- **Check Spanner metrics:** Are memory/CPU limits being hit during reads? Consider increasing the number of nodes if Spanner is struggling to serve the reads.
- **Check Dataflow metrics:** Are memory/CPU limits being hit? Dataflow should autoscale to the required number of nodes.
    - CPU/memory limits being hit means the `max_workers` parameter might be too low. It is recommended to use smaller machines (`Ex: n1-standard-4`) for most workloads. However, if you have very large datasets, we recommend scaling up the number of workers.

### Configuring to run using a VPC

#### Dataflow

1. Set the `network` and the `subnetwork` parameters to run the Dataflow job inside a VPC. Specify [network](https://cloud.google.com/dataflow/docs/guides/specifying-networks#network_parameter) and [subnetwork](https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter) according to the linked guidelines.
2. Set the `ip_configuration` to `WORKER_IP_PRIVATE` to disable public IP addresses for the worker VMs.
3. If only certain network tags are allowlisted via a firewall, specify the network tags via the [additional-experiments flag](https://cloud.google.com/dataflow/docs/guides/routes-firewall#network-tags-flex) (e.g. `use_network_tags=allow-dataflow`). Dataflow automatically assigns
   the `dataflow` network tag if any network tag is additionally specified. You need
   specify the tag for both worker VMs and launcher VMs.


> **_NOTE:_** You can use a shared VPC by specifying the `host_project` in the subnet path.
> This will result in the Dataflow jobs being launched inside the shared VPC.
> Usage of shared VPC requires cross-project permissions. They
> are available as a Terraform
> template [here](../../../../spanner-common/terraform/samples/configure-shared-vpc/README.md).
> Dataflow service account permissions are
> documented [here](https://cloud.google.com/dataflow/docs/guides/specifying-networks#shared).


If you are facing issue with VPC connectivity, check the following Dataflow
[guide](https://cloud.google.com/dataflow/docs/guides/troubleshoot-networking)
to debug common networking issues.

### Specifying schema changes

By default, the validation job performs a like-like schema mapping between the source AVRO records and Spanner. Any schema changes between the source and Spanner can be specified using a `session file` or `overrides` parameters.

**We highly recommend using the schema overrides parameters (`table_overrides` and `column_overrides`) instead of a session file** when dealing with schema differences.

#### Using Schema Overrides (Recommended)

When passing schema overrides to the job, you must strictly follow the required `[{}]` bracket-brace format. If the format is not matched exactly, Dataflow will reject the configuration with a regex error.

*   **For `table_overrides`**: Use the format `[{OldTableName,NewTableName}]`.
    *   *Example:* `[{Singers, Vocalists}]`
*   **For `column_overrides`**: You **MUST** include the table name alongside the column names. Use the format `[{TableName.OldColumnName,TableName.NewColumnName}]`. Missing the table name will cause the pipeline to crash.
    *   *Example:* `[{Singers.SingerId, Singers.VocalistId}]`

You can pass these overrides directly to your Terraform configuration using `var.table_overrides` and `var.column_overrides`.

#### Using a Session File

If you prefer or need to use a session file, you can generate one using the Spanner Migration Tool (SMT):

1. Setup SMT and [launch the UI](https://googlecloudplatform.github.io/spanner-migration-tool/ui#launching-the-web-ui-for-spanner-migration-tool).
2. Perform a [schema conversion](https://googlecloudplatform.github.io/spanner-migration-tool/ui/schema-conv) and download the session file locally.

To provide this session file to Terraform:

1. Upload the SMT generated `session file` to a Cloud Storage bucket.
2. Set the `var.session_file_path` variable to the GCS path of your uploaded file (e.g. `gs://my-bucket/path/to/session.json`).

### Adding access to Terraform service account

#### Using custom role and granular permissions (recommended)

You can run the following gcloud command to create a custom role in your GCP project.

```shell
gcloud iam roles create dv_terraform_role --project=<YOUR-PROJECT-ID> --file=perms.yaml --quiet
```

The `YAML` file required for the above will be like so -

```shell
title: "Data Validation Terraform Role"
description: "Custom role for running Spanner Data Validation via Terraform."
stage: "GA"
includedPermissions:
- iam.roles.get
- iam.serviceAccounts.actAs
# ....add all permissions from the list defined in the 'Terraform permissions' section above.
```

Then attach the role to the service account -

```shell
gcloud projects add-iam-policy-binding <YOUR-PROJECT-ID> \
    --member="serviceAccount:<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com" \
    --role="projects/<YOUR-PROJECT-ID>/roles/dv_terraform_role"
```

#### Using pre-defined roles

You can run the following shell script to add roles to the service account being used to run Terraform. This will have to done by a user which has the authority to grant the specified roles to a service account -

```shell
#!/bin/bash

# Service account to be granted roles
SERVICE_ACCOUNT="<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com"

# Project ID where roles will be granted
PROJECT_ID="<YOUR-PROJECT-ID>"

# Array of roles to grant
ROLES=(
  "roles/dataflow.admin"                 # Required to create and manage Dataflow jobs
  "roles/iam.securityAdmin"              # Required to bind roles to the Dataflow worker SA
  "roles/iam.serviceAccountUser"         # Required to impersonate the Dataflow worker SA
  "roles/storage.admin"                  # Required to manage GCS staging objects
  "roles/viewer"                         # Required to fetch current project states
  "roles/serviceusage.serviceUsageAdmin" # Required to enable required APIs
)

# Loop through each role and grant it to the service account
for ROLE in "${ROLES[@]}"
do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="$ROLE"
done
```

### Verifying access in the Terraform service account

Verify that the custom role is attached to the service account -

```shell
gcloud projects get-iam-policy <YOUR-PROJECT-ID>  \
--flatten="bindings[].members" \
--format='table(bindings.role)' \
--filter="bindings.members:serviceAccount:<YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com"
```

Verify that the role has the correct set of permissions

```shell
gcloud iam roles describe dv_terraform_role --project=<YOUR-PROJECT-ID> 
```

### Impersonating the Terraform service account

#### Using GCE VM instance (recommended)

A GCE VM created using the service account setup above will automatically use the service account for all API requests triggered by Terraform. Running terraform from such a GCE VM does not require downloading service keys and is the recommended approach.

#### Using key file

1. Activate the service account -
   ```shell
   gcloud auth activate-service-account <YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com --key-file=path/to/key_file --project=project_id
   ```
2. Impersonate service account while fetching the ADC credentials -
   ```shell
   gcloud auth application-default login --impersonate-service-account <YOUR-SERVICE-ACCOUNT>@<YOUR-PROJECT-ID>.iam.gserviceaccount.com
   ```
