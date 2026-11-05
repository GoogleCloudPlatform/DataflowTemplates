---
name: smt-e2e-dataflow-debugging
description: >-
  Debugs logical errors and data discrepancies in Dataflow templates by launching jobs via Terraform and comparing source (e.g. Cloud SQL) vs destination (e.g. Spanner) data.
  Use ONLY when the pipeline launches and runs to completion (terminal state) but exhibits data discrepancies or logical issues.
  Do NOT use for debugging template startup/runtime crashes or staging/building new templates.
---

# Skill: Dataflow Template Logical Error Debugging

## Trigger
`/smt-e2e-dataflow-debugging <FILE_NAME>.tfvars`

## Scope
This skill is **STRICTLY** restricted to testing the following templates:
*   `gcs-spanner-dv`
*   `sourcedb-to-spanner`
*   `datastream-to-spanner`
*   `spanner-to-sourcedb`

## Goal
To debug logical errors and data discrepancies by comparing source data (e.g., Cloud SQL) with destination data (e.g., Spanner).

## Prerequisites
*   The Dataflow job must be able to launch and run to a terminal state (Succeeded, Failed) using the provided `.tfvars` file. This skill is NOT for debugging startup/runtime crashes.
*   `terraform` CLI installed and configured.
*   `gcloud` CLI installed and authenticated (`gcloud auth login`).
*   `mvn` (Maven) and a compatible JDK installed.
*   `git` installed.
*   Access to the source database (e.g., Cloud SQL) instance, database, and credentials.
*   Access to the destination Spanner instance and database.
*   Appropriate IAM permissions for Dataflow, Cloud SQL, Spanner, GCS, and Cloud Logging.
*   Database client tools installed (e.g., `psql`, `mysql` client, or willingness to use `gcloud sql connect`).

## Variables
*   `<FILE_NAME>.tfvars`: The Terraform variables file.
*   `<YOUR_PROJECT_ID>`: The target Google Cloud Project ID.
*   `<YOUR_REGION>`: The Google Cloud region for the Dataflow job.
*   `<JOB_ID>`: The Dataflow Job ID.
*   `<JOB_NAME>`: The name of the Dataflow job.
*   **Cloud SQL Connection Info (Extracted from .tfvars):**
    *   `<CSQL_INSTANCE>`: Cloud SQL instance name.
    *   `<CSQL_DATABASE>`: Cloud SQL database name.
    *   `<CSQL_USER>`: Cloud SQL username.
    *   `<CSQL_PASSWORD>`: Cloud SQL password (if applicable).
    *   `<CSQL_PROJECT>`: Project of the Cloud SQL instance.
*   **Spanner Connection Info (Extracted from .tfvars):**
    *   `<SPANNER_INSTANCE>`: Spanner instance name.
    *   `<SPANNER_DATABASE>`: Spanner database name.
    *   `<SPANNER_PROJECT>`: Project of the Spanner instance.
*   `<MAVEN_MODULE_PATH>`: The relative path to the template's maven module (e.g. `v2/sourcedb-to-spanner`).
*   `<TEMPLATE_NAME>`: The name of the template.
*   `<YOUR_STAGING_BUCKET>`: Cloud Storage bucket for staging.

## Global Agent Rules
* **Fail-Fast Protocol**: If any executed terminal command returns an error or non-zero exit code, STOP IMMEDIATELY. Output the error to the user and ask for intervention. Do not attempt autonomous retries.
* **Data Privacy**: Ensure no real PII, credentials, or production connection details are exposed in logs or command outputs.

## Workflow Phases

### Phase 1: Deployment & Job Monitoring
1.  **Deploy the Dataflow Job**:
    *   Navigate to the directory containing `<FILE_NAME>.tfvars`.
    *   Initialize Terraform:
        ```bash
        terraform init
        ```
    *   Launch the job:
        ```bash
        terraform apply --var-file=<FILE_NAME>.tfvars -auto-approve
        ```
2.  **Monitor Status**: Wait for the job to reach a terminal state (Succeeded or Failed). Monitor status via Google Cloud Console or `gcloud`.

### Phase 2: Log Collection & Initial Analysis
1.  **Retrieve Job IDs**: Infer `<JOB_ID>` and `<JOB_NAME>` from the Terraform output or by listing active jobs:
    ```bash
    gcloud dataflow jobs list --project=<YOUR_PROJECT_ID> --region=<YOUR_REGION>
    ```
2.  **Inspect Job Logs**: Retrieve job message logs to identify warnings or non-obvious issues:
    ```bash
    gcloud logging read 'resource.type="dataflow_step" AND resource.labels.job_id="<JOB_ID>" AND logName=~"projects/.*/logs/dataflow.googleapis.com%2Fjob-message"' \
      --project=<YOUR_PROJECT_ID> \
      --limit=200 \
      --format="table(timestamp, textPayload, severity)" --order=asc
    ```
3.  **Inspect Worker Logs**: Check worker execution logs for specific exceptions or stack traces:
    ```bash
    gcloud logging read 'resource.type="dataflow_step" AND logName=~"projects/.*/logs/dataflow.googleapis.com%2Fworker" AND resource.labels.job_id="<JOB_ID>"' \
      --project=<YOUR_PROJECT_ID> \
      --limit=500 \
      --format="table(timestamp, jsonPayload.message, severity)" --order=asc
    ```

### Phase 3: Data Inspection & Validation
1.  **Inspect Source Data (Cloud SQL)**: Connect to the Cloud SQL instance and query source tables:
    ```bash
    # For PostgreSQL
    gcloud sql connect <CSQL_INSTANCE> --user=<CSQL_USER> --project=<CSQL_PROJECT>
    # For MySQL
    gcloud sql connect <CSQL_INSTANCE> --user=<CSQL_USER> --project=<CSQL_PROJECT>
    ```
    Execute validation queries:
    ```sql
    SELECT COUNT(*) FROM your_source_table;
    SELECT * FROM your_source_table LIMIT 10;
    ```
2.  **Inspect Destination Data (Spanner)**: Execute SQL queries on the destination Spanner database:
    ```bash
    gcloud spanner databases execute-sql <SPANNER_DATABASE> \
      --instance=<SPANNER_INSTANCE> \
      --project=<SPANNER_PROJECT> \
      --sql="SELECT COUNT(*) FROM your_destination_table"

    gcloud spanner databases execute-sql <SPANNER_DATABASE> \
      --instance=<SPANNER_INSTANCE> \
      --project=<SPANNER_PROJECT> \
      --sql="SELECT * FROM your_destination_table LIMIT 10"
    ```

### Phase 4: Discrepancy Verification & Resolution
1.  **Analyze Differences**:
    *   **Row Counts**: Do source and destination row counts match?
    *   **Schema Mapping**: Are column types and names mapped correctly?
    *   **Value Assertions**: Check NULL values, string encoding, timestamps, and precision.
2.  **Code Correction**: Locate the transformation logic in the `.java` files under the `GoogleCloudPlatform/DataflowTemplates` repository and fix the bug.
3.  **Re-stage Template**: Rebuild and upload the updated Flex Template:
    ```bash
    mvn clean package -PtemplatesStage -DskipTests \
      -DprojectId="<YOUR_PROJECT_ID>" \
      -DbucketName="<YOUR_STAGING_BUCKET>" \
      -DstagePrefix="templates" \
      -DtemplateName="<TEMPLATE_NAME>" \
      -pl <MAVEN_MODULE_PATH> -am
    ```

### Phase 5: Re-testing & Iteration
1.  **Clean Destination Table**: Delete destination records to prepare for a clean run:
    ```bash
    gcloud spanner databases execute-sql <SPANNER_DATABASE> \
      --instance=<SPANNER_INSTANCE> \
      --project=<SPANNER_PROJECT> \
      --sql="DELETE FROM your_destination_table WHERE true"
    ```
2.  **Re-run Job**: Deploy the job again with the newly built template (Phase 1) and verify the fix.

## Important Considerations
*   **Idempotency**: Ensure clean test tables before re-running to avoid duplicate count errors.
*   **Data Volume**: Limit queries to subsets when working with large volumes.
*   **Terraform State**: Avoid modifying resources manually outside of Terraform to prevent state drift.