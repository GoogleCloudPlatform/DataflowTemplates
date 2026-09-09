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

## Execution Instructions
Take the following parameters as input.
1. **Target Source Database Name:** Extract from the user prompt.
2. **Reference Datatype Mapping Matrix File Path:** Extract from the user prompt.

Create tests for the current target similar to the following tests.
1. com.google.cloud.teleport.v2.templates.endtoend.BulkMigrationAndValidationMySQLAllDataTypesE2EIT
2. com.google.cloud.teleport.v2.templates.endtoend.BulkMigrationAndValidationPostgreSQLAllDataTypesE2EIT

Run the tests for the target database and fix any issues encountered in the code.

## Example User Prompt
```text
Please run the add-integ-tests-gcs-spanner-dv skill for the MySQL database. 
Use the reference matrix located at v2/gcs-spanner-dv/src/test/resources/mysql/mysql_datatype_mapping_matrix.csv.
```
