---
name: add-integ-tests-datastream-to-spanner
description: >-
  Specific runner skill that delegates to the Template-Agnostic Meta-Test Orchestrator for the datastream-to-spanner (CDC) template.
---
# Datastream-to-Spanner Testing Orchestrator 

This is a specific runner skill that acts as a wrapper around the global `meta-test-orchestrator` skill.

## Required Prompt Inputs
1. **Target Source Database Name**
2. **Reference Datatype Mapping Matrix File Path**
3. **Testing Environment Setup Path**

> [!IMPORTANT]
> **Initialization Check**: If ANY of the required prompt inputs are missing from the user's prompt, or if the environment config file does not exist, you **MUST HALT EXECUTION IMMEDIATELY**. Ask the user to provide the missing inputs before proceeding.
> 
> **Source Support Validation**: Before proceeding to execution, you must verify if the template actually supports migrating from the provided `Target Source Database Name`. Inspect the template's source code (e.g., `DatastreamToSpanner.java` or `DatastreamToSpannerSourceConnectorRegistry.java`) to confirm this database dialect is implemented. If it is not supported, you **MUST HALT EXECUTION IMMEDIATELY** and inform the user.

## Execution Instructions
You must immediately load and execute the `v2/spanner-common/.agents/skills/meta-test-orchestrator/SKILL.md` skill, passing it the following statically mapped parameters:

1. **Target Source Database Name:** Extract from the user prompt.
2. **Reference Datatype Mapping Matrix File Path:** Extract from the user prompt.
3. **Testing Environment Setup Path:** Extract from the user prompt and assert its presence.
4. **Manifest File Path:** `v2/datastream-to-spanner/src/test/manifest.yaml`.
5. **Target Template Path:** `v2/datastream-to-spanner`
6. **Phase 1 Smoke Scenarios:** `live-it`
7. **Phase 2 Baseline Datatype Scenarios:** `live-datatypes`

Yield execution completely to the `meta-test-orchestrator`. Instruct the Orchestrator to begin Phase 1 using these parameters.

## Example User Prompt
```text
Please run the add-integ-tests-datastream-to-spanner skill for the MySQL database. 
Use the reference matrix located at v2/datastream-to-spanner/src/test/resources/mysql/mysql_datatype_mapping_matrix.csv load the execution properties from testing_execution.env.
```
