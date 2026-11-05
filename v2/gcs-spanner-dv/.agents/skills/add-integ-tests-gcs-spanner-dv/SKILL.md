---
name: add-integ-tests-gcs-spanner-dv
description: >-
  Specific runner skill that creates integration tests for the gcs-spanner-dv (Data Validation) template.
---
# GCS-Spanner-DV Testing Orchestrator 

This is used to add test for the data validation template.

## Required Prompt Inputs
1. **Target Source Database Name**
2. **Reference Datatype Mapping Matrix File Path**

> [!IMPORTANT]
> **Initialization Check**: If ANY of the required prompt inputs are missing from the user's prompt, or if the environment config file does not exist, you **MUST HALT EXECUTION IMMEDIATELY**. Ask the user to provide the missing inputs before proceeding.

Please load and execute the `v2/spanner-common/.agents/skills/add-source-datatype-integ-test/SKILL.md` skill to generate a datatype integration test.

Create tests for the current target similar to the following tests.
1. com.google.cloud.teleport.v2.templates.endtoend.BulkMigrationAndValidationMySQLAllDataTypesE2EIT
2. com.google.cloud.teleport.v2.templates.endtoend.BulkMigrationAndValidationPostgreSQLAllDataTypesE2EIT

Run the tests for the target database and fix any issues encountered in the code.

CRITICAL CONSTRAINTS:
- Treat the provided Reference Mapping File as your absolute source of truth to derive baseline mapping schemas. You MUST strictly use this matrix to generate testing mappings. Do NOT perform independent type research.
- Use the `-DdirectRunnerTest` flag for iterative testing.Once the DirectRunner loop passes, you MUST perform a final execution directly against Cloud Dataflow (omitting the flag) and ensure that run completely succeeds before generating your final report.
- Upon completing your artifact report, ensure `RequestFeedback: false` is set so your status naturally changes back to idle. Do NOT pause waiting for conversational human feedback.

## Example User Prompt
```text
Please run the add-integ-tests-gcs-spanner-dv skill for the MySQL database. 
Use the reference matrix located at v2/gcs-spanner-dv/src/test/resources/mysql/mysql_datatype_mapping_matrix.csv.
```
