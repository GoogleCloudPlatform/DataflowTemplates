---
name: add-source-datatype-integ-test
description: >
  Skill for adding data type integ tests for new source by provided datatype_mapping_matrix.
---

# New Source DataTypes Integration Test Generation Skill

This skill instructs an AI Coding Agent to design and synthesize an exhaustive, dialect-wide integration test suite for Dataflow Templates for a new database source dialect from first principles, without being biased by a reference database test.

---

## 1. Goal
Given a Scenario ID, parse its configuration from `manifest.yaml`, gather structural class/helper context from a reference test (without copying its columns/assertions), read provided datatype mapping matrix and compile a comprehensive list of all native datatypes supported by the target database dialect (including recommended and alternative Spanner mappings), generate native source SQL schemas containing CREATE and INSERT statements with type-specific formats, write a complete Java integration test from scratch with dynamic assertions, compile-verify, and self-heal.

## 1.1 Initialization Check
Before you begin parsing or executing any core steps, you MUST verify the following inputs and dependencies exist:
1. **Scenario ID**
2. **Manifest File Path**
3. **Target Source Database Name**
4. **Reference Datatype Mapping Matrix File Path**
5. **Testing Environment Setup Path**: You MUST proactively check the workspace root directory for a `.env` file named `testing_execution.env`.
6. **Mapping Matrix Schema Validation**: 
   You must dynamically validate the `.csv` mapping file against the canonical schema.
   1. Use the `read_url_content` tool to fetch the canonical reference: `https://raw.githubusercontent.com/GoogleCloudPlatform/spanner-migration-tool/master/.agents/skills/source_research_helper/sampleOutput/mysql_datatype_mapping_matrix.csv`
   2. Parse the headers (first line) of both the fetched sample and the local file at `Reference Datatype Mapping Matrix File Path`.
   3. Verify that *every* column header present in the fetched sample is also present in the local provided matrix (a subset match; the local matrix may contain extra columns).

If ANY of the prompt inputs are missing, or if the `testing_execution.env` file does not exist, or if the provided mapping matrix is missing any canonical headers, you **MUST HALT EXECUTION IMMEDIATELY**. Do not attempt to guess, hallucinate paths, or proceed. Output a direct question asking for the missing inputs, missing file, or reporting the explicitly missing columns.

---

## 1.2 Prerequisites: Execution Configuration
The agent must look for a `testing_execution.env` file in the workspace root with the following schema:
```env
REMOTE_WORKSPACE_PATH=
REMOTE_EXEC_TEMPLATE=
REMOTE_SYNC_TEMPLATE=
TEST_PROJECT=
TEST_REGION=
TEST_SPANNER_INSTANCE=
TEST_STAGE_BUCKET=
TEST_ARTIFACT_BUCKET=
TEST_HOST_IP=
TEST_PRIVATE_CONNECTIVITY=
TEST_DB_USER=
TEST_DB_PASSWORD=
```

---

## 1.3 Directory Segregation Rule
To prevent cluttering the root templates package, you MUST place all generated files into a strictly nested directory structure matching the lowercased name of the target database.
- **Java Class Path:** `src/test/java/com/google/cloud/teleport/v2/templates/{target_db_name_lowercase}/{TestClassName}.java`
- **Java Package:** `package com.google.cloud.teleport.v2.templates.{target_db_name_lowercase};`
- **SQL Resources Path:** `src/test/resources/{target_db_name_lowercase}/{TestClassName}/`

---

## 1.4 Global Constraint: Production Code Priority Rule

> [!CRITICAL]
> **Production Code Priority Rule (Source Over Test)**:
> This is a global constraint that applies to all phases of this skill (context discovery, type mapping, code generation, compilation, execution, and debugging).
> If an integration test fails during compilation or execution, you MUST first assume that the bug lies in the **production template source code** (e.g., dialect adapters, DML generators, schema mappers, or exception classifiers) rather than the test harness or DDL.
> *   **Systematic Investigation**: You must thoroughly analyze the template's core Java engine to identify if it is failing to support the target database's dialect behavior correctly.
> *   **No Test Hacking**: You are strictly forbidden from "hacking", disabling, or loosening a test assertion or DDL constraint to bypass a failure, unless the production template is proven to be behaving 100% correctly and the mismatch is purely due to database-specific padding/formatting differences.
> *   **Documentation**: Every change to the production template must be documented in your final report with a root-cause explanation, and any test adaptation must be explicitly justified in the code with JavaDocs.

---

## 2. Step 1: Input Parsing & Scenario Scouting
1. The user MUST provide the path to the `manifest.yaml` scenario registry file in their prompt. Open that file.
2. Locate the scenario matching the input `Scenario ID`.
3. Extract:
   - `context`: Fully qualified name of the reference test class (e.g., `com.google.cloud.teleport.v2.templates.<ReferenceSourceDialect>DataTypesIT`).
   - `template`: Path to the template module (e.g., `v2/<template_path>`).
   - `spanner_dialect`: Target Spanner database dialect (`GOOGLE_STANDARD_SQL` or `POSTGRESQL`). If not explicitly defined, default to `GOOGLE_STANDARD_SQL`.
   - Check if a baseline data type mapping file has been supplied in the prompt/context (e.g., `mapping.json` or `mapping.yaml`).
4. Determine the **Migration Direction**:
   - Inspect the `template` path and the `context` class inheritance chain (scouted in Step 2):
     - If the reference class extends a forward migration base class (e.g., `<SourceDbToSpannerITBaseClass>`) or the `template` path contains a forward template name (e.g., `sourcedb-to-spanner` or `datastream-to-spanner`): **Forward Migration** (Source Database -> Spanner).
     - If the reference class extends a reverse migration base class (e.g., `<SpannerToSourceDbITBaseClass>`) or the `template` path contains a reverse template name (e.g., `spanner-to-sourcedb`): **Reverse Migration** (Spanner -> Target Database).

---

## 3. Step 2: Context Scouting (Structural Analysis Only)
To build a compilable integration test, you must analyze the structural patterns of the template test suite.

> [!IMPORTANT]
> **Strict Scouting Constraint**:
> Open the reference class, its parent classes, and configuration base classes *strictly* to discover:
> - The class inheritance chain (e.g., extends `<BaseITClass>`).
> - Setup helper APIs (e.g., `setUp<SourceDialect>ResourceManager()`, `setUpSpannerResourceManager()`).
> - The pipeline launching structure (e.g., `launchDataflowJob(...)`, `pipelineOperator()`).
>
> You **MUST NOT** copy the list of tables, columns, data types, test values, or assertions defined in the reference class. The new test suite must be built independently from first principles.

1. **Locate Reference Class File**:
   - Perform a glob/grep search to find the file matching the context class name on disk.
2. **Scan Inheritance Chain**:
   - Open the reference class file and inspect its `extends <SuperClass>` signature.
   - Recursively search for and open all parent/base classes up the inheritance chain to read all available setup and helper APIs.

---

## 4. Step 3: Ingesting the Ground Truth Mapping
Instead of researching the database dialect from scratch, this skill assumes a complete, human-approved data type mapping matrix has been provided in CSV format (the "Ground Truth"). Your task is to ingest this mapping and use it to strictly drive all subsequent schema and code generation.

> [!IMPORTANT]
> **Mandatory Input Rule**:
> You MUST receive the exact path to the initial mapping `.csv` file from the user or Orchestrator in your prompt.
> You MUST NOT perform independent web research, nor should you invent types that are not listed in this CSV file. Treat this file as the absolute, exhaustive dictionary of what must be tested.

1. **Ingest the Mapping File**:
   - Read the provided mapping `.csv` file. 
   - Parse the matrix into memory.
   - Use the `"Source Database Type / Alias"` column to identify the native source database type.
2. **Identify Dialect Validations**:
   - Check the `spanner_dialect` determined in Step 1 (`GOOGLE_STANDARD_SQL` vs `POSTGRESQL`). 
   - Fetch the mapping strictly from the corresponding Spanner Dialect column in the CSV (e.g., `"Spanner GoogleSQL Default Datatype"`, `"Spanner GoogleSQL Alternative Datatypes"`, `"Spanner PostgreSQL Default Datatype"`, `"Spanner PostgreSQL Alternative Datatypes"`).
3. **Identify Validation Targets**:
   - Extract every row that is intended for Default or Alternative mapping (Scenarios A and B).
   - **Primary Keys (Scenario C):** You MUST explicitly read the `"Is Source Datatype Supported as PK?"` column in the matrix. If supported (`Yes` or similar), you MUST apply the specific mapped type provided in the `"Spanner GoogleSQL Default Datatype If Column is PK"` or `"Spanner PostgreSQL Default Datatype If Column is PK"` column depending on the target dialect.
   - **Unsupported/Complex Types (Scenario D):** Extract every row marked as Unsupported or Complex. To identify rows for Scenario D, filter for any rows where `"Datastream Support Status"` is marked as unsupported (or equivalent phrasing like 'No' or 'Unsupported').
4. **No User Approval Required**:
   - Because this mapping file was vetted in a prior phase, do **NOT** ask the user for approval. Proceed immediately to Step 4.

---

## 5. Step 4: DDL Schema Synthesis (Tested Scenarios)

For every discovered datatype **and every identified type alias**, you must generate schemas covering the following scenarios:
- **Scenario A: Default Type Migration**: Regular column using recommended/default Spanner mapping.
  - *Naming Pattern*: Table Name: `{type_or_alias}_table`, Target Column: `{type_or_alias}_col`.
- **Scenario B: Alternative Type Migration**: Regular column using an alternative Spanner mapping.
  - *Naming Pattern*: Table Name: `{type_or_alias}_to_{clean_target_type}_table`, Target Column: `{type_or_alias}_to_{clean_target_type}_col`.
- **Scenario C: Primary Key Mapping**: Testing the data type or alias as part of the primary key constraint.
  - *Naming Pattern*: Table Name: `{type_or_alias}_pk_table`, Target Column: `{type_or_alias}_pk_col` as the primary key.
  - *Feasibility & Column Override*: You MUST only generate Scenario C if the matrix explicitly marks `Is the datatype supported as a PK in the source?` as `Yes`. Check the `If Column is a PK` column for the target dialect, as you might need to use a different Spanner type (e.g., `BYTES` or restricted `STRING`) when the key is indexed.
  - **SMT Range Splitter Constraint**: You MUST exclude all floating-point and decimal/numeric types (e.g. `FLOAT`, `DOUBLE`, `REAL`, `NUMERIC`, `DECIMAL` with fractional scales) from Scenario C (`is_pk_feasible = False`). During range queries, SMT's range splitter casts partition boundaries to `Long` integers, truncating the fractional bounds and causing extreme boundary float rows to be silently dropped during migration.
- **Scenario D: Unsupported/Complex Types**: Tables containing unsupported spatial, collection, or complex types to verify graceful migration and row-count consistency without pipeline failures.
- **Scenario E: Array/Collection Types**: Tables explicitly testing arrays, lists, or collections of all supported scalar base types.

> [!IMPORTANT]
> **Case Preservation & Identifier Quoting**:
> To ensure generated tests and schemas run successfully, you must align and preserve the exact character casing of all table names, column names, keys, and constraints between the target source database and the target Spanner database.
> 1. **Check Case Preservation Requirement**:
>    - First check if the reference schema, target Spanner schema, or target test uses any uppercase or mixed-case identifiers.
>    - Check if the target database folds unquoted identifiers to a different case by default (e.g., some database folds unquoted names to lowercase, or another database folds to uppercase).
>    - If the target database folds unquoted identifiers, AND there are mixed-case/uppercase names, case preservation is **REQUIRED**.
>    - If all identifiers are already completely lowercase, case preservation is **NOT required** (and you should NOT wrap them in redundant quotes).
> 2. **Apply Quoting if Case Preservation is Required**:
>    - Wrap all identifiers (table names, column names, keys, etc.) in dialect-appropriate quotes in the generated DDL statements.
>    - **Do Not Alter Original Intent**: You MUST NOT alter the original casing of the reference schema identifiers (e.g., converting mixed-case names to all-caps or all-lowercase) just to trivially bypass the database's folding rules. You MUST apply quotes to preserve the original intended mixed-casing exactly as it was provided in the reference.
> 3. **Casing Parity Between Target Source and Spanner**:
>    - In some cases (such as data type ITs), target schema table names might differ from the reference test schema.
>    - However, whatever names are used for the target source schema, their exact character case **MUST be identical** in the target Spanner database schema DDL.

### 5.1 FORWARD MIGRATION SCHEMA GUIDELINES
If the migration direction is **Forward**:
1. **Target Source Schema DDL (`{target_source_dialect}-schema.sql`)**:
   - Generate `CREATE TABLE` statements for Scenarios A, B, C, D, and E.
   - **Multi-Row Insertion Mandate (CRITICAL):**
     1. **CSV Edge Cases First**: For EVERY table (Scenarios A, B, and E), you MUST first extract the literal array of values from the `Edge Cases for Smoke Test` column in the CSV mapping for this datatype. Generate an `INSERT` statement for every single parsed value.
     2. **Supplement with Core Boundaries**: After inserting the CSV edge cases, you MUST forcefully supplement the table with your own generated values for the following explicit boundary scenarios:
        - **Typical/Normal Value.**
        - **Absolute Maximum Boundary Value** (e.g., Max Integer, Max Precision Decimal, max string length).
        - **Absolute Minimum Boundary Value / Empty** (e.g., Min Integer, empty string, empty array).
        - **NULL Value** (test null handling for the column).
        - **Special/Edge Cases** (e.g., Emojis, Leap Year dates, multi-byte Unicode).
     - *Type-Specific Formatting*: Ensure these `INSERT` statements use any special formats required by the source type (e.g., hex strings/literals for binary, escaped strings, date/time string formats, array literals, JSON strings).

2. **Target Spanner Schema DDL (`{target_source_dialect}-{spanner_dialect}-spanner-schema.sql`)**:
   - Translate target source tables to target Spanner DDL statements matching Scenarios A, B, C, D, and E structures.
   - Do NOT include any `INSERT` or population statements in the Spanner schema DDL.
   - For Spanner `GOOGLE_STANDARD_SQL` dialect, do NOT use double quotes (`"`) or single quotes (`'`) to quote identifiers. Only use backticks (`` ` ``) if escaping is required. Use double quotes if targeting `POSTGRESQL` Spanner dialect.

### 5.2 REVERSE MIGRATION SCHEMA GUIDELINES
If the migration direction is **Reverse**:
1. **Target Spanner Schema DDL (`{target_source_dialect}-{spanner_dialect}-spanner-schema.sql`)**:
   - Synthesize Spanner `CREATE TABLE` DDL statements matching Scenarios A, B, C, D, and E structures.
   - Do NOT include any `INSERT` or population statements in this file. (Data population will be handled in Java using Spanner mutations).
   - For Spanner `GOOGLE_STANDARD_SQL` dialect, do NOT use double quotes (`"`) or single quotes (`'`) to quote identifiers. Only use backticks (`` ` ``) if escaping is required. Use double quotes if targeting `POSTGRESQL` Spanner dialect.
2. **Target Source Schema DDL (`{target_source_dialect}-schema.sql`)**:
   - Generate native target database `CREATE TABLE` statements for Scenarios A, B, C, D, and E.
   - Do NOT write any `INSERT INTO` statements in this file.

### 5.3 Schema Completeness Self-Audit
Before proceeding to Step 5, the agent MUST perform a completeness self-audit:
1.  **Cross-Reference Table Mappings**: Compare the generated `{target_source_dialect}-schema.sql` tables against the approved Logical Type Mapping Table from Step 3.
2.  **Verify Scenario Coverage**: Ensure that for *every* data type and type alias listed in the mapping table, dedicated test tables are defined for:
    *   **Scenario A** (Default recommended Spanner mapping)
    *   **Scenario B** (Alternative Spanner mappings)
    *   **Scenario C** (Primary Key mapping - if indexable and feasible)
    *   **Scenario E** (Array/Collection mapping - if array types exist)
3.  **Verify Scenario D (Unsupported Types)**: Ensure that *every* type identified as unsupported or complex in the mapping table (e.g. geometric, spatial, system/internal types) is represented in a Scenario D table to verify that the pipeline migrates them gracefully (maintaining row count consistency) without failure.
4.  **Enforce Resolution**: If any type, alias, or unsupported type is missing from the synthesized DDL schemas, you must generate the missing tables and insert statements before proceeding.

---

## 6. Step 5: Infrastructure Lifecycle & Resource Manager Setup

### 6.1 Source Database ResourceManager lookup and check
To provision infrastructure resources for integration tests, you MUST inherit the `ResourceManager` architectural pattern used by the `reference_test_class`. If the reference test delegates setup to a method in a base class (e.g., an `ITBase.java` class), you MUST declare a matching method for your target database in that same base class if it doesn't already exist.

1. **Inherit Reference Pattern**: Inspect the `ResourceManager` imported by the reference test's setup method. 
   - If the reference test logic uses a simple testcontainers-based Resource Manager from the Apache Beam IT SDK (`org.apache.beam.it`), you MUST find and use the equivalent simple testcontainers Resource Manager from the Beam IT SDK for your target source.
   - If the reference test instead imports a custom cloud-based Resource Manager from the local `it/` codebase, you MUST search the local `it/` codebase for the equivalent cloud Resource Manager for your target source.
2. **Base Class Injection**: Once you determine the correct `ResourceManager` class using the pattern above, inject the corresponding setup method directly into the Integration Test Base Class, and be sure to update any of its helper methods (such as dialect or driver locators) to support it.
3. **Synthesize Missing Custom ResourceManager**: If neither the local codebase nor the Apache Beam IT SDK has support for the target source, you MUST synthesize a new custom `ResourceManager` class from scratch:
   - For JDBC sources: Use `AbstractJDBCResourceManager.java` or an existing dialect's `ResourceManager` from the `it/` folder as a blueprint, extending `AbstractJDBCResourceManager`. Write the class to the appropriate test utility directory.
   - For non-JDBC/NoSQL/messaging sources: Use an existing robust implementation like `SpannerResourceManager.java` from the `it/` folder as your architectural blueprint for establishing connection URIs, teardown lifecycles, and test containers.

---

## 7. Step 6: Java Test Class Synthesis
Write the concrete Java integration test class from scratch under the target package/directory:

### Coding Constraints
1. **Header**: Start the file with the exact header comment: `/* GENERATED BY: Test Automation Skill */`.
2. **No Inline SQL/DDL**: Load SQL schemas strictly by referencing the resource path constant.
3. **Dynamic Source Resource Manager Setup**:
   - Inspect the discovered base class files to locate the correct target `ResourceManager` class and its setup method.
   - Call the corresponding setup method defined on the base class (e.g., `setUp<SourceDialect>ResourceManager()` for the target source dialect).
4. **Dynamic Spanner Resource Manager Setup**:
   - Inspect the discovered base/parent class files to locate the correct method to initialize the Spanner ResourceManager matching the target `spanner_dialect` (e.g. search for `setUpPGDialectSpannerResourceManager` if Spanner dialect is `POSTGRESQL`, or `setUpSpannerResourceManager` if `GOOGLE_STANDARD_SQL`).
5. **Case Preservation in Java SQL Queries**:
   - If the target source database folds unquoted identifiers by default, and the reference test/schema uses uppercase or mixed-case identifiers, you MUST wrap table and column names in dialect-appropriate quotes within the generated Java test class SQL queries.
6. **Proprietary Database Drivers (`jdbcDriverJars`)**:
   - Some databases are natively bundled in the flex template container by default while proprietary ones are often excluded via test scopes. If your target source database relies on a proprietary JDBC driver that is excluded from the main template deployment, the test will crash on Cloud Dataflow unless you explicitly pass the `--jdbcDriverJars` parameter. 
   - You MUST ensure your generated Java test class retrieves the dynamically staged driver GCS URL from the base IT class environment (e.g. checking the parent class for a GCS driver path method matching your dialect) and maps it into the `launchTemplate(...)` parameters block.
7. **Unified Launch Method (`launchDataflowJob`)**:
   - Always run the template using the base helper method:
     `jobInfo = launchDataflowJob(getClass().getSimpleName(), ...);`
   - Do NOT construct manual inline runner option maps or direct pipeline executions in the test class. The `TemplateTestBase` framework automatically routes execution to local `DirectRunner` or Cloud `Dataflow` depending on build system parameters.

---

## 8. Step 7: Maven POM Dependency Injection
1. Open the `pom.xml` file for the target template module (e.g., `<template_path>/pom.xml`).
2. Identify the required dependencies for communicating with the target database/source.
3. If the identified dependency is not already declared in the `pom.xml`, add it inside the `<dependencies>` section with `<scope>test</scope>`.

---

## 9. Step 8: Compilation, Execution, and Runtime Cleanup

Load the variables from `testing_execution.env`. You MUST route all synchronization and execution through the provided template strings to support the vendor's specific environment.

### 9.1 Workspace Synchronization
1. Sync the local workspace to the execution environment (Note: If the vendor configured local execution, this may just be a harmless `echo` command):
   - Read `REMOTE_SYNC_TEMPLATE`. Replace `<LOCAL_PATH>` with `./` and `<REMOTE_PATH>` with the value of `REMOTE_WORKSPACE_PATH`.
   - Execute the resulting command in your terminal.
2. **Pre-build local dependencies**: If the test dynamically loads jars from other modules (e.g., `spanner-custom-shard` target directory) which are excluded from sync, build those modules on the target environment:
   - Read `REMOTE_EXEC_TEMPLATE`. Replace `<COMMAND>` with `cd ${REMOTE_WORKSPACE_PATH} && mvn package -pl v2/spanner-custom-shard -am -DskipTests`.
   - Execute the rendered command.

### 9.2 Compilation Verification
Verify compilation:
1. Replace `<COMMAND>` in `REMOTE_EXEC_TEMPLATE` with `cd ${REMOTE_WORKSPACE_PATH} && mvn test-compile -pl <template_path> -am -Dcheckstyle.skip=true`.
2. Execute it.
If compilation fails:
- Extract the compiler error block matching your target test class name.
- Analyze the compiler logs, fix the Java code locally, re-sync (Step 8.1), and compile-verify again until it succeeds.

### 9.3 DirectRunner Testing Loop (DirectRunner-First Strategy)
To minimize development turnaround time, always attempt to execute your tests using local pipeline first:
1. Run the test by appending `-DdirectRunnerTest` to your maven execution command.
   - Replace `<COMMAND>` in `REMOTE_EXEC_TEMPLATE` with the following command (substitute the `<variables>` appropriately):
   `cd ${REMOTE_WORKSPACE_PATH} && rm -f live_logs && mvn verify -f pom.xml -U -PtemplatesIntegrationTests,splunkDeps -pl <template_path> -am -Dtest=<TestClassName> -e -DdirectRunnerTest -Dmdep.analyze.skip -Dcheckstyle.skip -Dspotless.check.skip=true -Djib.skip -DskipShade -DartifactBucket="${TEST_ARTIFACT_BUCKET}" -DstageBucket="${TEST_STAGE_BUCKET}" -Dproject="${TEST_PROJECT}" -Dregion=${TEST_REGION} -DspannerInstanceId="${TEST_SPANNER_INSTANCE}" -Dsurefire.useFile=false -DitParallelismType=none -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -DhostIp=${TEST_HOST_IP} -DprivateConnectivity=${TEST_PRIVATE_CONNECTIVITY} -DcloudProxyHost=${TEST_HOST_IP} -DcloudProxyPassword="${TEST_DB_PASSWORD}" -DcloudProxyUsername="${TEST_DB_USER}" > live_logs 2>&1`
   - Run the rendered template command in the background (append `&`).
2. The testing framework will bypass cloud staging and run the template inside the local JVM in-process.
3. **Thread Cancellation Fix**: Ensure `DirectRunnerClient.java` is patched to cancel running jobs using `thread.interrupt()` instead of the deprecated `thread.stop()`, preventing test hangs during resource cleanup.

### 9.4 Functional Test Execution (Cloud Dataflow)
1. Run the test without the `-DdirectRunnerTest` property. 
   - Replace `<COMMAND>` in `REMOTE_EXEC_TEMPLATE` with the same Maven command from 8.3, but omitting `-DdirectRunnerTest`.
   - Run the rendered template command in the background (append `&`).
2. Monitor execution progress by tailing the logs (e.g., `tail -n 100 live_logs`).

### 9.5 Resource Teardown & Cleanup Verification
1. **Do Not Ignore Cleanup Failures**: If the test execution hangs or fails during the teardown phase (e.g., failure to drop databases or replication slots), the test must be considered **FAILED**. Do not manually terminate and call it a pass.
2. **Investigate Cleanup Order**: A common cause of cleanup failure is incorrect resource manager shutdown order. Ensure that consumer resources (e.g., Datastream streams, Dataflow jobs) are fully stopped and deleted *before* attempting to clean up source resources (e.g., database instances, replication slots).
3. **Leaked Resource Check**: Always verify that all provisioned resources (Spanner databases, Cloud SQL databases, replication slots, GCS files) are deleted after the run. If leakages are detected, update the test teardown logic to handle them.

### 9.6 Investigation & Debugging of Test Failures
If the integration test fails (assertion failure, runtime exception, or timeout):
1. **Analyze Test Logs**: Inspect the test output or surefire reports (`live_logs` if running remotely) to identify the failing assertion or exception stack trace.
2. **Inspect Dataflow Job Status**: Retrieve the Dataflow job ID from the logs and check its status and error messages:
   ```bash
   gcloud dataflow jobs list --project=${TEST_PROJECT} --region=${TEST_REGION}
   gcloud logging read "resource.type=\"dataflow_step\" AND resource.labels.job_id=\"<YOUR_JOB_ID>\" AND severity>=WARNING" --project=${TEST_PROJECT} --limit=100
   ```
3. **Identify the Root Cause**: Determine if the failure is due to:
   - Schema mismatch (casing, type conversion).
   - Network connectivity issues.
   - Permission issues on GCP resources.
   - Logical bug in the template code.
4. **Apply Fixes & Re-test**: Apply fixes to the test code or template code, re-sync changes, and re-run.

### 9.7 Artifact Archiving
1. Upon a completely successful test run (compilation, execution, and zero DLQ errors), you MUST create a dedicated artifact folder relative to the workspace root: `src/test/resources/<target_db_name_lowercase>/reports/datatype_testing/<Scenario_ID>_<Timestamp>/`.
2. Copy the following artifacts into this folder:
   - The successful `live_logs` output.
   - The provided initial mapping file.
---

## 10. Step 9: Post-Validation Migration Report
1. You MUST generate a markdown report file named `test_automation_migration_report.md` at the exact path relative to the workspace root: `<template_path>/src/test/resources/<target_db_name_lowercase>/reports/datatype_testing/<Scenario_ID>_<Timestamp>/test_automation_migration_report.md`. Placed anywhere else, the user will not see it.
2. The report MUST rigidly follow this structure:
   
   **1. Test Suite Details:**
   - Scenario ID, Target Source Dialect, and Target Spanner Dialect.
   - Summarize what the test suite is validating (e.g., "Validating 42 Postgres types and 3 alternative mappings using DirectRunner").

   **2. Execution Result & Final Command:**
   - Final Status (SUCCESS/FAILURE).
   - The exact Maven command used for the final successful execution.

   **3. Source Code Bugs & Fixes:**
   - Detail any bugs discovered in the *production template code* (not the test code).
   - Show the exact code changes applied to fix the production code.

   **4. Self-Healing & Retry History:**
   - A chronological list of all failed attempts during compilation or execution.
   - Format: `Retry # | Issue/Exception Encountered | Fix Applied`.

   **5. Exhaustive Type Testing Matrix:**
   - A single comprehensive table detailing every tested type.
   - Columns: `Type Category`, `Source Type`, `Spanner Type`, `Edge Cases Tested` (List the specific boundary/null/special values tested), `Pass/Fail`.

   **6. Coverage vs. Initial Baseline:**
   - A single comprehensive table (Single type per table) to compare the final tested types against the initial user-provided baseline mapping.
   - Explicitly list which new types or alternative overrides were discovered by the agent's independent research.
   - **Must include a comprehensive Baseline Mapping Table** with the following columns: 
     - `Source Type`
     - `Target Spanner Type` (include alternative mapping overrides)
     - `Covered?` (Yes/No/Skipped)
     - `Table Name` (table name which test this type)
     - `Verification` (Pass/Fail)
     - `Values Tested` (list of exact values tested and what was value verified on spanner)
     - `Notes` (Any specific notes to be added, like reason for skip, reason for failure, etc)
---

## 11. Strict Code Editing Constraints
CRITICAL RULE: Do NOT write secondary Python or Bash scripts to execute find-and-replace string manipulations on the template `.java` or `pom.xml` source code. You must use your native AI file-editing tools explicitly to modify code block-by-block.

---

## 12. Human Escalation & Clarification Guidelines
As an autonomous agent executing this skill, you must stop and ask the user for confirmation or input in the following scenarios:
1. **Unresolved Compilation Errors**: If after **3 self-healing attempts** the test class still fails to compile, stop, present the compiler error block and your current code, and ask the user for guidance.
2. **Missing ResourceManager blue-prints**: If you determine that a new ResourceManager needs to be synthesized from scratch and you cannot find any suitable blueprint reference, ask the user to specify/confirm the ResourceManager setup structure.
3. **Data Type Ambiguity**: If mapping a data type from the source schema results in multiple potential target database/Spanner dialect mappings with different behaviors, ask the user to confirm the preferred mapping choice.


