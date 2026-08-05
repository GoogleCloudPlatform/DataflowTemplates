---
name: add-integ-tests-spanner-to-sourcedb
description: >-
  Specific runner skill that delegates to the Template-Agnostic Meta-Test Orchestrator for the spanner-to-sourcedb (Reverse Migration) template.
---
# Spanner-to-SourceDB Testing Orchestrator 

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
4. **Manifest File Path:** `v2/spanner-to-sourcedb/src/test/manifest.yaml`.
5. **Target Template Path:** `v2/spanner-to-sourcedb`
6. **Phase 1 Smoke Scenarios:** `reverse-it`
7. **Phase 2 Baseline Datatype Scenarios:** `reverse-datatypes`, `reverse-datatypes-pg`

Yield execution completely to the `meta-test-orchestrator`. Instruct the Orchestrator to begin Phase 1 using these parameters.

## Example User Prompt
```text
Please run the add-integ-tests-spanner-to-sourcedb skill for the oracle database. 
Use the reference matrix located at v2/spanner-to-sourcedb/src/test/resources/oracle/oracle_datatype_mapping_matrix.csv load the execution properties from testing_execution.env.
```
