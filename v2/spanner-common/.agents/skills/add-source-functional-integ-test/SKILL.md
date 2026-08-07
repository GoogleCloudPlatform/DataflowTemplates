---
name: add-source-functional-integ-test
description: >
  Skill for adding functional integ test for new source by taking reference from existing MySQL integ tests.
---

# New Source Functional Integration Test Generation Skill

This skill instructs an AI Coding Agent to generate and verify integration tests for a new database source scenario (e.g. migrating from a reference source to a target source) for Dataflow Templates.

---

## 1. Goal
Given a Scenario ID, read its configuration from `manifest.yaml`, locate all reference resources and code, discover logical data type mappings, translate schemas/DDLs and the concrete Java test class, verify via compilation, and self-heal any compilation failures.

## 1.1 Initialization Check
Before you begin parsing or executing any core steps, you MUST verify the following inputs and dependencies exist:
1. **Scenario ID**
2. **Manifest File Path**
3. **Target Source Database Name**
4. **Reference Datatype Mapping Matrix File Path**
5. **Testing Environment Setup Path**: You MUST proactively check the workspace root directory for a `.env` file named `testing_execution.env`.

If ANY of the prompt inputs are missing, or if the `testing_execution.env` file does not exist in the root directory, you **MUST HALT EXECUTION IMMEDIATELY**. Do not attempt to guess, hallucinate paths, or proceed without the environment variables. Output a direct question asking the user to provide the missing inputs or create the file.

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
   - `context`: Fully qualified name of the reference test class (e.g. `com.google.cloud.teleport.v2.templates.MySQLDataTypesIT`).
   - `template`: Path to the template module (e.g. `v2/sourcedb-to-spanner`).
   - `spanner_dialect`: Target Spanner database dialect (`GOOGLE_STANDARD_SQL` or `POSTGRESQL`). If not explicitly defined in the manifest for the scenario, default to `GOOGLE_STANDARD_SQL`.
   - `type` (optional): Set to `datatypes` for exhaustive type-mapping integration tests.
   - `type_mappings` (optional): A pre-defined list of mappings. Each mapping specifies:
     - `source_type`: The data type in the reference source database (e.g., `VARCHAR(21000)`).
     - `spanner_type`: The desired Spanner data type (e.g., `STRING(MAX)`).

---

## 3. Step 2: Context Discovery (Scouting)
To avoid hallucinating helper methods or base configurations, you must gather all relevant context:

> [!IMPORTANT]
> **Strict Context Constraint**: Anchor all context discovery strictly starting from the reference class file extracted in Step 1. Do NOT search for or inspect other unrelated integration tests (e.g., other PostgreSQL or MySQL tests) in the codebase unless they are direct ancestors or referenced resources of the reference class. Inspecting unrelated test files can lead to hallucinating mismatched setup methods, parent classes, or helper signatures.

1. **Locate Reference Class File**:
   - Perform a glob/grep search to find the file matching the context class name on disk.
2. **Scan Inheritance Chain**:
   - Open the reference class file and inspect its `extends <SuperClass>` signature.
   - Recursively search for and open all parent/base classes up the inheritance chain to read all available setup and helper APIs.
3. **Scan Referenced Resource Files**:
   - In the reference class file and parent classes, look for string constants pointing to *any* resource files (e.g., SQL schemas, Avro files, JSON mappings, CSVs).
   - Locate and read all of these referenced resource files from the repository.

---

## 4. Step 3: Logical Type Mapping Discovery
Because you are generating functional tests (e.g., sharding, foreign keys) and not exhaustively generating boundary data, you MUST translate the DDL schemas using the exact data types defined in the provided CSV Ground Truth mapping.

1. **Ingest the CSV Mapping File**:
   - Read the `.csv` file provided in your prompt.
   - Parse the matrix to understand how default types are mapped for the target Spanner Dialect (`GOOGLE_STANDARD_SQL` vs `POSTGRESQL`).
2. **Translate Types**:
   - As you scan the reference DDL schema from Step 2, identify the column data types.
   - Use the ingested CSV matrix as a strict dictionary to translate those column types into the Target Source Type and the matching Target Spanner Type.
3. **No User Approval Required**:
   - Because this mapping file was vetted in a prior phase (or curated by the user), it is the absolute source of truth.
   - Do **NOT** construct a markdown mapping table to ask the user for approval. 
   - Proceed immediately to Step 4 (DDL Schema Translation).

---

## 5. Step 4: DDL Schema Translation
Identify whether case sensitivity is required to match Spanner schemas, and perform quoting adjustments:

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

### 5.1 Target Source Schema DDL
1. Translate the reference source DDL schema file(s) into native SQL statements compatible with the target source database dialect.
2. The column data types in the generated SQL schema MUST use the standard equivalent target source types identified in the logical type mapping table.
3. Apply the **Case Preservation & Identifier Quoting** check and rule to prevent the target database from folding identifier cases.
4. Save the generated DDL file to the target template resources directory using the pattern: `{target_source_dialect}-schema.sql`.

### 5.2 Target Spanner Schema DDL
1. Translate the reference Spanner DDL SQL file to the target Spanner database dialect (`GOOGLE_STANDARD_SQL` or `POSTGRESQL`).
2. Align column names, table structures, and primary keys with the target source schema, ensuring the exact character case of all table and column names matches the target source schema exactly. Apply the **Case Preservation & Identifier Quoting** rule if targeting Spanner PostgreSQL dialect.
   - **Important**: For Spanner `GOOGLE_STANDARD_SQL` dialect, do NOT use double quotes (`"`) or single quotes (`'`) to quote identifiers (tables, columns, etc.) as they are not supported. Only use backticks (`` ` ``) if escaping is required; otherwise, leave identifiers unquoted.
3. Save the translated Spanner DDL SQL file to the target template resources directory using the pattern: `{target_source_dialect}-{spanner_dialect}-spanner-schema.sql`.

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

## 7. Step 6: Java Test Class Translation
Translate the reference Java class to the target dialect class under the target package/directory:

### Name Mapping Rules
- Target class name: Replace the reference source dialect name in the class name with the target source dialect name (e.g. `<SourceDialect>DataTypesIT` -> `<TargetDialect>DataTypesIT`).

### Strict Coding Constraints
1. **Header**: Start the file with the exact header comment: `/* GENERATED BY: Test Automation Skill */`.
2. **No Inline SQL/DDL**: Load SQL schemas strictly by referencing the resource path constant.
3. **Dynamic Source Resource Manager Setup**:
   - Inspect the discovered base class files to locate the correct target `ResourceManager` class and its setup method.
   - Call the corresponding setup method defined on the base class (e.g., `setUp<TargetDialect>ResourceManager()` for the target dialect).
4. **Dynamic Spanner Resource Manager Setup**:
   - Inspect the discovered base/parent class files to locate the correct method to initialize the Spanner ResourceManager matching the target `spanner_dialect` (e.g. search for `setUpPGDialectSpannerResourceManager` if Spanner dialect is `POSTGRESQL`, or `setUpSpannerResourceManager` if `GOOGLE_STANDARD_SQL`).
5. **Case Preservation in Java SQL Queries**:
   - If the target source database folds unquoted identifiers by default, and the reference test/schema uses uppercase or mixed-case identifiers, you MUST wrap these table and column names in dialect-appropriate quotes within the generated Java test class SQL queries, inserts, or validations.
6. **Proprietary Database Drivers (`jdbcDriverJars`)**:
   - Some databases are natively bundled in the flex template container by default while proprietary ones are often excluded via test scopes. If your target source database relies on a proprietary JDBC driver that is excluded from the main template deployment, the test will crash on Cloud Dataflow unless you explicitly pass the `--jdbcDriverJars` parameter. 
   - You MUST ensure your generated Java test class retrieves the dynamically staged driver GCS URL from the base IT class environment (e.g. checking the parent class for a GCS driver path method matching your dialect) and maps it into the `launchTemplate(...)` parameters block.
7. **Replication/CDC Setup**:
   - If the template processes CDC updates, ensure the source database resource manager initializes logical replication or binlogs, and pass the resulting replication configuration (e.g., slot names, publication names, or GTID/binlog positions) to the source connector configuration.

---

## 8. Step 7: Maven POM Dependency Injection
1. Open the `pom.xml` file for the target template module (e.g., `<template_path>/pom.xml`).
2. Identify the required dependencies for communicating with the target database/source. These can include:
   - For JDBC sources: The JDBC driver dependency (e.g., `<jdbc_driver_artifact_id>` for `<TargetSourceDBDialect>`).
   - For non-JDBC/NoSQL/messaging/etc. sources: The target database's official Java client library SDK dependency (e.g., Cassandra or MongoDB driver) or the Apache Beam IO connector library dependency (e.g., `beam-sdks-java-io-cassandra`).
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
1. Upon a completely successful test run (compilation, execution, and zero DLQ errors), you MUST create a dedicated artifact folder relative to the workspace root: `src/test/resources/<target_db_name_lowercase>/reports/functional_testing/<Scenario_ID>_<Timestamp>/`.
2. Copy the following artifacts into this folder:
   - The successful `live_logs` output.

---

## 10. Step 9: Post-Validation Migration Report
1. You MUST generate a markdown report file named `test_automation_migration_report.md` at the exact path relative to the workspace root: `<template_path>/src/test/resources/<target_db_name_lowercase>/reports/functional_testing/<Scenario_ID>_<Timestamp>/test_automation_migration_report.md`. Placed anywhere else, the user will not see it.
2. The report must contain:
   
   **1. Test Suite Details:**
   - Scenario ID, Target Source Dialect, and Target Spanner Dialect.
   - Summarize what the test suite is validating/testing with what varification has been done.

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
   - A single comprehensive table (Single-Row-Per-Table) detailing every tested type.
   - Columns: `Type Category`, `Source Type`, `Spanner Type`, `Edge Cases Tested`, `Pass/Fail`.

---

## 11. Strict Code Editing Constraints
CRITICAL RULE: Do NOT write secondary Python or Bash scripts to execute find-and-replace string manipulations on the template `.java` or `pom.xml` source code. You must use your native AI file-editing tools explicitly to modify code block-by-block.

---

## 12. Human Escalation & Clarification Guidelines
As an autonomous agent executing this skill, you must stop and ask the user for confirmation or input in the following scenarios:
1. **Unresolved Compilation Errors**: If after **3 self-healing attempts** the test class still fails to compile, stop, present the compiler error block and your current code, and ask the user for guidance.
2. **Missing ResourceManager blue-prints**: If you determine that a new ResourceManager needs to be synthesized from scratch (i.e. no existing ResourceManager exists in the templates module for the target dialect) and you cannot find any suitable blueprint reference, ask the user to specify/confirm the ResourceManager setup structure.
3. **Data Type Ambiguity**: If mapping a data type from the source schema results in multiple potential target database/Spanner dialect mappings with different behaviors, ask the user to confirm the preferred mapping choice.
