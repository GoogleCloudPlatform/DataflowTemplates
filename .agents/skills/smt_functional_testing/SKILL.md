---
name: smt-functional-testing
description: >-
  Functionally tests local Dataflow pipeline changes against the main branch using ephemeral GCP resources and gated approvals.
  Use ONLY when functionally testing one of these specific migration templates: gcs-spanner-dv, sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb.
  Skip entirely for other templates. Don't use for deploying templates to production or debugging a running production pipeline without testing.
---

# Skill: Dataflow PR Functional Testing (Modular & Gated)

## Trigger
`/smt-functional-testing`

## Scope
This skill is **STRICTLY** restricted to testing the following templates:
*   `gcs-spanner-dv`
*   `sourcedb-to-spanner`
*   `datastream-to-spanner`
*   `spanner-to-sourcedb`

If the template being tested is NOT one of the above, **DO NOT** use this skill. Skip it entirely.

## Goal
To functionally test local Dataflow pipeline changes against the `main` branch. The IDE agent acts as an orchestrator to analyze code, define topologies, provision isolated ephemeral environments, and verify data. **Pause for user approval before state-changing actions and halt immediately if any terminal command fails.**

## Prerequisites
* Local changes are committed or staged.
* `gcloud` and `terraform` CLIs are authenticated.
* The `/smt-e2e-dataflow-debugging` skill is active and available.
* A pre-built flex template path is provided by the user.
* A `.env.testing` file exists in the workspace root.

## Workspace Setup: `.env.testing` Schema
```env
# Required Fundamentals
PROJECT_ID=
REGION=
GCS_STAGING_BUCKET=
BUILT_TEMPLATE_GCS_PATH=
WORKER_MACHINE_TYPE="n2-standard-4"

# Infrastructure (Leave blank for agent to provision ephemerally)
SOURCE_INSTANCE_NAME=
SOURCE_DATABASE_NAME=
SOURCE_DB_USER=
SOURCE_DB_PASSWORD=
TARGET_SPANNER_INSTANCE=
TARGET_SPANNER_DATABASE=
CUSTOM_TRANSFORMATION_JAR_PATH=

## Global Agent Rules
* **Fail-Fast Protocol**: If any executed terminal command returns an error or non-zero exit code, STOP IMMEDIATELY. Output the error to the user and ask for intervention. Do not attempt autonomous retries.
* **Syntax Strictness**:
    * Spanner DDL: Ensure absolute accuracy. Rigorously verify that WHERE clauses in partial indexes are syntactically valid for Cloud Spanner because Cloud Spanner enforces a strict subset of SQL functions in WHERE clauses for partial indexes, and invalid functions will cause DDL execution to fail.
* **Configuration Files**: Ensure any `shardingContextFilePath` or similar configuration strictly adheres to a valid JSON map structure to avoid parsing issues during pipeline runtime.
* **Parameters**: Do not invent unrecognized Dataflow parameters or session file names.

## Workflow Phases

### Phase 1: Code Analysis & Test Case Generation
1.  **Sourcing State**: Execute `source .env.testing` in the terminal or load the variables into context. Generate a unique run ID: `export TEST_RUN_ID=$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 6)`.
2.  **Analyze Diff**: Execute `git diff main...` and analyze the output:
    *   Summarize the code changes in plain English.
    *   Identify the Dataflow pipeline components, classes, or transforms affected.
    *   Describe the potential impact on the pipeline's data flow and logic.
3.  **Determine Template Path**: 
    *   **No changes in pipeline code**: If no pipeline code changes are being tested, use the latest available public Dataflow template path (e.g., `gs://dataflow-templates-us-central1/latest/flex/Sourcedb_to_Spanner_Flex`).
    *   **New changes present in pipeline code**: If there are local changes, ask the user to stage the new changes. Provide the exact command to run with flags to minimize build time (e.g., if testing for <TEMPLATE_NAME>: `mvn clean package -PtemplatesStage -DskipTests -Dspotless.check.skip=true -Dcheckstyle.skip=true -DprojectId="$PROJECT_ID" -DbucketName="$GCS_STAGING_BUCKET" -DstagePrefix="templates-${TEST_RUN_ID}" -DtemplateName="<TEMPLATE_NAME>" -pl <RELATIVE_PATH_TO_TEMPLATE> -am`). Offer the user the option for you to run this command autonomously. Once staged, update `BUILT_TEMPLATE_GCS_PATH` in `.env.testing`.
4.  **Generate Test Matrix**: Based on the changes, generate test cases covering:
    *   Happy paths for the intended new/modified logic.
    *   Edge cases related to data types, nulls, empty inputs relevant to the changes.
    *   Error scenarios, including malformed data handling and Dead Letter Queue (DLQ) routing.
    *   Regression scenarios to ensure existing functionality isn't broken.
    *   *Format*: For each test case, briefly describe the scenario and the expected outcome.
5.  **Approval Gate 1 (Test Cases)**: Output the proposed test cases (and confirm the template staging plan). STOP AND WAIT. Do not proceed until the user explicitly approves.

### Phase 2: Environment Topology & Automated Provisioning
1.  **Topology & Sharding Analysis**: Before drafting any schemas, analyze the target pipeline to determine the correct architecture:
    *   **Direction & Dialect**: Identify the source and destination databases (e.g., sourcedb-to-spanner is SQL->Spanner; spanner-to-sourcedb is Spanner->SQL) and their expected dialects (e.g., PostgreSQL vs MySQL).
    *   **Sharding Requirement**: Determine if the tested code dictates a sharded test setup. If a sharded setup is required, explicitly design 2 physical database instances, each containing 2 logical schemas, yielding a total of 4 logical shards.
2.  **Design Setup**: Design the Source and Target table schemas (DDL) matching the identified dialects, and generate specific INSERT statements (DML) to simulate the scenarios.
    *   Ensure that all test cases and edge cases defined in Phase 1 will be comprehensively covered.
3.  **Custom/Sharding Transformation Planning**: If the test requires a custom or sharding transformation, specify the necessary Java logic here during the schema and data planning phase (as it directly impacts test cases and success data).
4.  **Database Credentials**: To insert schemas into CloudSQL, ask the user to enter `SOURCE_DB_USER` and `SOURCE_DB_PASSWORD` in the `.env.testing` file. Alternatively, let the user know they can choose to have you autonomously create a user and connect to the database using that.
5.  **Expected State & Success Criteria**: Define the EXPECTED data/state at the destination. Defining the success criteria for each data row is important and must be explicitly stated (i.e., what should the destination rows look like, what are the expected DLQ entries, etc.). All generated GCP resource names must include the `-${TEST_RUN_ID}` suffix.
6.  **Test Case Mapping**: Present the setup using the following format:

| Test Case ID | Description | Source Setup (Table/Data Highlights) | Expected Destination State & Success Criteria |
|---|---|---|---|
| TC01 | Happy Path | `users` table with standard input | Row exists in destination `users` with transformed fields. No DLQ entries. |

7.  **Approval Gate 2 (Environment)**: Output the Topology Analysis, Test Case Mapping table (with explicit success criteria), the raw DDL/DML, and any proposed custom transformation logic. STOP AND WAIT. Do not proceed until the user approves.
8.  **Execute Provisioning (Autonomous)**: Upon approval, autonomously save the DDL/DML to local files (e.g., `source_schema_${TEST_RUN_ID}.sql`, `target_schema_${TEST_RUN_ID}.ddl`) and execute the required `gcloud sql` and `gcloud spanner` provisioning commands in the terminal to create the schemas and insert the data. If a custom transformation was approved, autonomously build the JAR, upload it to GCS (`gs://${GCS_STAGING_BUCKET}/test_configs_${TEST_RUN_ID}/`), and update `CUSTOM_TRANSFORMATION_JAR_PATH` in `.env.testing` so Terraform uses it. Do NOT prompt the user to do these steps themselves.

### Phase 3: Configuration Staging
1.  **Generate Configs**: Based on the test cases and topology, generate necessary configuration files locally (e.g., session files for sharding, override files, DLQ sinks).
2.  **Approval Gate 3 (Configs)**: Output the contents of the files for review. STOP AND WAIT.
3.  **Execute Upload**: Upon approval, execute `gcloud storage cp` to upload the files to a unique path: `gs://${GCS_STAGING_BUCKET}/test_configs_${TEST_RUN_ID}/`.

### Phase 4: Job Execution via Terraform
1.  **Dynamic TFVars**: Generate and autonomously edit the Terraform variables file named `test_${TEST_RUN_ID}.tfvars` incorporating the dynamic resources from Phase 2, the uploaded configs from Phase 3, and the `BUILT_TEMPLATE_GCS_PATH` read from the `.env.testing` state.
2.  **Approval Gate 4 (TFVars)**: Output the contents of the generated `.tfvars` file (job parameters). STOP AND WAIT for user confirmation.
3.  **Stage the Job (Autonomous)**: Upon approval of the job parameters, autonomously execute the necessary commands (e.g., `terraform apply`) in the terminal to stage the job. Do NOT ask the user to execute it themselves.
4.  **Monitoring & Debugging Gate**: Pause the primary workflow here while the job is running. Monitor the terminal/context for the completion of the job execution. If you need to check the progress of the already staged job or debug any runtime errors, you should utilize the `/smt-e2e-dataflow-debugging` skill. Note: The `/smt-e2e-dataflow-debugging` skill is STRICTLY for checking progress and debugging an already staged Dataflow job, not for staging the job itself. DO NOT PROCEED to Phase 5 until a definitive completion signal is observed.

### Phase 5: Automated Verification & Teardown
1.  **Verification Queries**: Once Phase 4 is definitively complete, execute targeted terminal queries (`gcloud spanner databases execute-sql`, `gcloud sql execute`, etc.) against the destination tables and DLQs to verify the expected state for EACH test case.
2.  **Reporting**: Output a final Markdown report:

# Dataflow Test Report - Run ID: ${TEST_RUN_ID}
## Test Case Results:
| Test Case ID | Status | Expected | Actual | Notes |
|---|---|---|---|---|
| TC01 | PASS | ... | ... | |
| TC02 | FAIL | ... | ... | Field 'X' was not handled as expected |

3.  **Teardown Draft**: Draft comprehensive cleanup commands tailored to the specific topology provisioned in Phase 2:

```bash
# Example commands, adjust based on actual provisioned resources
gcloud sql instances delete ${SOURCE_INSTANCE_NAME}-${TEST_RUN_ID} --project=${PROJECT_ID} --quiet
gcloud spanner instances delete ${TARGET_SPANNER_INSTANCE}-${TEST_RUN_ID} --project=${PROJECT_ID} --quiet
gcloud storage rm -r gs://${GCS_STAGING_BUCKET}/test_configs_${TEST_RUN_ID}
rm -f test_${TEST_RUN_ID}.tfvars source_schema_${TEST_RUN_ID}.sql target_schema_${TEST_RUN_ID}.ddl
```

4.  **Approval Gate 5 (Cleanup)**: Output the teardown commands and ask the user for permission to execute them.
