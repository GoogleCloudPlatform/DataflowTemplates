---
name: meta-test-orchestrator
description: >-
  Template-agnostic Orchestrator Skill for generating and executing exhaustive testing suites for any migration template. It will generate the functional and datatype related test for the source.
---
# Template-Agnostic Orchestrator Meta-Skill

This skill instructs an AI Agent to act as the "Project Manager" for onboarding a new database source into ANY given Dataflow testing template. It automates the generation and execution of the template's entire testing suite by spawning specialized subagents.

---

## 1. Goal
Orchestrate a fully automated, 3-Phase pipeline to build, test, and verify every single scenario defined in the template's `src/test/manifest.yaml`. 

You MUST execute all spawned subagents **sequentially**. Do NOT run subagents concurrently, to prevent Maven staging conflicts.

---

## 2. Global Constraint: Orchestrator Heartbeats

> [!CRITICAL]
> **Resilience & Subagent Monitoring Rule**:
> Because the full execution pipeline can take hours, backend server maintenance restarts might occasionally drop background polling scripts, leaving your subagents stranded in a "waiting" state while tests actually finish in the cloud.
> - As the Orchestrator, you MUST maintain a proactive heartbeat. 
> - You MUST set a recurring `schedule` tool timer (e.g. `CronExpression: "*/2 * * * *"`, `IsDaemon: false`) dedicated to babysitting.
> - Whenever the heartbeat timer fires, explicitly use `manage_subagents list` to check statuses. If you notice a subagent stuck in `waiting_for_message` for an extended period, you must manually run `ssh ... tail` against the remote logs to check the pipeline's true status, and then explicitly use `send_message` to blast the subagent awake with the logs so it resumes working.

---

## 3. Initialization Requirements
When a user begins a session with this Meta-Skill, ensure you have the following inputs before starting:
1. **Target Source Database Name:**
2. **Reference Mapping Matrix File Path:**
3. **Testing Environment Setup Path:** Confirm that `testing_execution.env` is populated in the workspace root.
4. **Target Template Path:**
5. **Phase 1 Smoke Scenarios:** (comma-separated list of scenario IDs)
6. **Phase 2 Baseline Datatype Scenarios:** (comma-separated list of scenario IDs)
7. **Manifest File Path:**

> [!IMPORTANT]
> **Mapping Matrix Schema Validation**: 
> You must dynamically validate the user's provided `.csv` mapping file against the canonical schema before proceeding with any orchestration or code generation. 
> 1. Use the `read_url_content` tool to fetch the raw canonical reference from: `https://raw.githubusercontent.com/GoogleCloudPlatform/spanner-migration-tool/master/.agents/skills/source_research_helper/sampleOutput/mysql_datatype_mapping_matrix.csv`
> 2. Parse the headers (first line) of both the fetched sample matrix and the local file at `Reference Datatype Mapping Matrix File Path`.
> 3. Verify that *every* column header present in the fetched sample is also present in the local provided matrix (a subset match; the local matrix may contain extra custom columns, which is fine).

If ANY of the prompt inputs are missing, or if the `testing_execution.env` file does not exist in the root directory, or if the provided matrix is missing canonical headers, you **MUST HALT EXECUTION IMMEDIATELY**. Do not attempt to guess, hallucinate paths, or proceed. Output a direct question asking the user to provide the missing inputs, create the missing environment file, or fix the explicitly missing columns.

---

## 4. Orchestration Rules

1. **Sequential Threading:** You may only have **one** active subagent running at any time.
2. **Template Onboarding Report:** You MUST maintain a live markdown artifact called `template_onboarding_report.md`.
   - **Progress Tracker:** Maintain a high-level counter (e.g., `5 / 20 scenarios completed`).
   - **Consolidated Bug Log:** Aggregate all source-code bugs discovered by subagents and the fixes applied to the `<Target_Template_Path>` source code.
   - **Challenges:** Summarize any roadblocks or unsupported features (e.g., missing Spanner APIs).
3. **Error Escalation:** If a subagent exhausts its self-healing retries and fails, report back to the user with a summary of the roadblock before halting the pipeline.
4. **Artifact Relocation & Syncing:** Subagents frequently save their final migration reports (e.g., `test_automation_migration_report.md` and `live_logs`) into their isolated 'brain' execution sandboxes. As the parent Orchestrator, upon validating a subagent's success, you MUST natively copy their generated reports out of their system-isolated directories and move them directly into the correct workspace root directory using the following exact structure:
   - For Datatype tests: `src/test/resources/<target_db_name_lowercase>/reports/datatype_testing/<Scenario_ID>_<Timestamp>/`
   - For Functional tests: `src/test/resources/<target_db_name_lowercase>/reports/functional_testing/<Scenario_ID>_<Timestamp>/`
5. **Continuous Meta-Report Syncing:** You MUST copy the overarching meta-skill report (`template_onboarding_report.md`) and orchestration execution logs to a persistent directory in the workspace at `src/test/resources/<target_db_name_lowercase>/reports/meta-reports/` right from the beginning, and you must constantly update/overwrite this workspace file as tests run and statuses change iteratively!


---

## 5. The 3-Phase Execution Pipeline

You must guide the workflow through these phases sequentially, waiting for one subagent to complete successfully before advancing or spinning up the next.

### Phase 1: Infrastructure Smoke Tests
**Goal:** Prove the infrastructure and `testing_execution.env` works before adding complex data types.
**Action:** Spawn a subagent using **Prompt Template A** targeting only the scenarios provided in the **Phase 1 Smoke Scenarios** input list (e.g., `bulk-simple`).

### Phase 2: Complete Datatypes Validation
**Goal:** Validate all datatypes for the template (including alternate dialects like PostgreSQL Spanner deployments) using the provided reference mapping file.
**Action:** Spawn subagents sequentially using **Prompt Template B** for the explicit scenarios provided in the **Phase 2 Baseline Datatype Scenarios** input list.

Once those are complete, aggressively scan the provided **Manifest File Path** for any **additional** scenarios tagged with `type: datatypes` (that you haven't executed yet) and spawn subagents for them one at a time. Wait for each subagent to complete before spawning the next.

### Phase 3: Functional Scenarios Scale-Out
**Goal:** Translate the remaining complex features (e.g., sharding, foreign keys, limits).
**Action:** Scan the provided **Manifest File Path** for all remaining functional scenarios (not covered in Phases 1-2).
- Spawn subagents for each scenario **one at a time** using **Prompt Template A**.
- **Crucial Rule:** As soon as one subagent completes successfully, instantly capture its logs and reports, update your tracker, and proactively move linearly to spawn the next subagent scenario. Do absolutely NOT halt the pipeline or wait for user input/confirmation between scenarios!

### Final Validation
**Goal:** Run a complete verification regression suite across all generated tests to guarantee zero regressions.
**Action:** 
1. To construct the final regression test command, read `v2/spanner-common/.agents/skills/add-source-functional-integ-test/SKILL.md` to learn how to natively assemble the `mvn verify` parameter list from the environment configs. 
2. Once assembled, swap the target class name parameter for the overarching wildcard corresponding to the dialect (e.g., `-Dtest=Oracle*IT`).
3. Maintain the `-DdirectRunnerTest` flag to run the regression safely and rapidly. Execute this on the remote test VM.
4. If the regression suite fails on any specific test, you must re-invoke a subagent dedicated to that specific failing test scenario, passing it the failure logs and instructing it to self-heal the source code and verify its individual test again.
5. Capture the final outcome of the regression run in your meta-report and securely copy the final executed regression logs to the workspace.

---

## 6. Subagent Prompt Templates

### Prompt Template A (Functional Worker)
Use this prompt when invoking subagents for Phases 1 and 4.
**Skill to load:** `v2/spanner-common/.agents/skills/add-source-functional-integ-test/SKILL.md`
```text
Please load and execute the `v2/spanner-common/.agents/skills/add-source-functional-integ-test/SKILL.md` skill to generate a functional integration test.

Inputs:
1. Scenario ID: [INSERT_SCENARIO_ID]
2. Manifest File Path: [INSERT_MANIFEST_FILE_PATH]
3. Target Source Database Name: [INSERT_DB_NAME]
4. Reference Datatype Mapping Matrix File Path: [INSERT_MAPPING_FILE_PATH]
5. Testing Environment Setup Path: [INSERT_ENV_PATH]

CRITICAL CONSTRAINTS:
- Treat the provided Reference Mapping File as your absolute source of truth to derive baseline mapping schemas. You MUST strictly use this matrix to generate testing mappings. Do NOT perform independent type research.
- Load execution strategy from `testing_execution.env`.
- Follow the Production Code Priority Rule: if tests fail, investigate the target template source code before assuming the test is wrong.
- Use the `-DdirectRunnerTest` flag for iterative testing. Once the DirectRunner loop passes, you MUST perform a final execution directly against Cloud Dataflow (omitting the flag) and ensure that run completely succeeds before generating your final report.
- Upon completing your artifact report, ensure `RequestFeedback: false` is set so your status naturally changes back to idle. Do NOT pause waiting for conversational human feedback.

```

### Prompt Template B (Datatype Worker)
Use this prompt when invoking subagents for Phases 2 and 3.
**Skill to load:** `v2/spanner-common/.agents/skills/add-source-datatype-integ-test/SKILL.md`
```text
Please load and execute the `v2/spanner-common/.agents/skills/add-source-datatype-integ-test/SKILL.md` skill to generate a datatype integration test.

Inputs:
1. Scenario ID: [INSERT_SCENARIO_ID]
2. Manifest File Path: [INSERT_MANIFEST_FILE_PATH]
3. Target Source Database Name: [INSERT_DB_NAME]
4. Reference Datatype Mapping Matrix File Path: [INSERT_MAPPING_FILE_PATH]
5. Testing Environment Setup Path: [INSERT_ENV_PATH]

CRITICAL CONSTRAINTS:
- Treat the provided Reference Mapping File as your absolute source of truth to derive baseline mapping schemas. You MUST strictly use this matrix to generate testing mappings. Do NOT perform independent type research.
- Load execution strategy from `testing_execution.env`.
- Use the `-DdirectRunnerTest` flag for iterative testing.Once the DirectRunner loop passes, you MUST perform a final execution directly against Cloud Dataflow (omitting the flag) and ensure that run completely succeeds before generating your final report.
- Upon completing your artifact report, ensure `RequestFeedback: false` is set so your status naturally changes back to idle. Do NOT pause waiting for conversational human feedback.

```
