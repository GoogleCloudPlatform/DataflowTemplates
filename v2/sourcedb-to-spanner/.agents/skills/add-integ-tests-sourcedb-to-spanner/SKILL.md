---
name: add-integ-tests-sourcedb-to-spanner
description: >-
  Specific runner skill that delegates to the Template-Agnostic Meta-Test Orchestrator for the sourcedb-to-spanner (Bulk) template.
---
# SourceDB-to-Spanner Testing Orchestrator 

This is a specific runner skill that acts as a wrapper around the global `meta-test-orchestrator` skill.

## Required Prompt Inputs
1. **Target Source Database Name**
2. **Reference Mapping Matrix File Path**
3. **Environment Config Path** (e.g. `testing_execution.env`)

## Execution Instructions
You must immediately load and execute the `v2/spanner-common/.agents/skills/meta-test-orchestrator/SKILL.md` skill, passing it the following statically mapped parameters:

1. **Target Source Database Name:** Extract from the user prompt.
2. **Reference Mapping Matrix File Path:** Extract from the user prompt.
3. **Environment Config:** Extract from the user prompt and assert its presence.
4. **Manifest File Path:** `v2/sourcedb-to-spanner/src/test/manifest.yaml`.
5. **Target Template Path:** `v2/sourcedb-to-spanner`
6. **Phase 1 Smoke Scenarios:** `bulk-simple`
7. **Phase 2 Baseline Datatype Scenarios:** `bulk-datatypes`, `bulk-datatypes-pg`

Yield execution completely to the `meta-test-orchestrator`. Instruct the Orchestrator to begin Phase 1 using these parameters.

## Example User Prompt
```text
Please run the add-integ-tests-sourcedb-to-spanner skill for the oracle database. 
Use the reference matrix located at v2/sourcedb-to-spanner/src/test/resources/oracle/oracle_datatype_mapping_matrix.csv load the execution properties from testing_execution.env.
```
