---
name: add-integ-tests-gcs-spanner-dv
description: >-
  Specific runner skill that delegates to the Template-Agnostic Meta-Test Orchestrator for the gcs-spanner-dv (Data Validation) template.
---
# GCS-Spanner-DV Testing Orchestrator 

This is a specific runner skill that acts as a wrapper around the global `meta-test-orchestrator` skill.

## Required Prompt Inputs
1. **Target Source Database Name**
2. **Reference Datatype Mapping Matrix File Path**
3. **Testing Environment Setup Path** (e.g. `testing_execution.env`)

> [!IMPORTANT]
> **Initialization Check**: If ANY of the required prompt inputs are missing from the user's prompt, or if the environment config file does not exist, you **MUST HALT EXECUTION IMMEDIATELY**. Ask the user to provide the missing inputs before proceeding.

## Execution Instructions
You must immediately load and execute the `v2/spanner-common/.agents/skills/meta-test-orchestrator/SKILL.md` skill, passing it the following statically mapped parameters:

1. **Target Source Database Name:** Extract from the user prompt.
2. **Reference Datatype Mapping Matrix File Path:** Extract from the user prompt.
3. **Testing Environment Setup Path:** Extract from the user prompt and assert its presence.
4. **Manifest File Path:** `v2/gcs-spanner-dv/src/test/manifest.yaml`.
5. **Target Template Path:** `v2/gcs-spanner-dv`
6. **Datatype Test Scenarios:** `dv-datatypes-googlesql`, `dv-datatypes-pgsql`

Yield execution completely to the `meta-test-orchestrator`. Instruct the Orchestrator to begin using these parameters.

## Example User Prompt
```text
Please run the add-integ-tests-gcs-spanner-dv skill for the MySQL database. 
Use the reference matrix located at v2/gcs-spanner-dv/src/test/resources/mysql/mysql_datatype_mapping_matrix.csv load the execution properties from testing_execution.env.
```
