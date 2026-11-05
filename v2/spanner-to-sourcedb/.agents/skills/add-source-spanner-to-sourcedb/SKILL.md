---
name: add-source-spanner-to-sourcedb
description: >-
  Guide for implementing a database source connector in the v2/spanner-to-sourcedb
  reverse migration Dataflow template. Details scope boundaries, prerequisites, connector implementations,
  registry registrations, unit testing, and smoke testing guidelines.
---

# Skill: Implement Source in Spanner to SourceDb Template

## Overview
This skill provides a step-by-step procedure for adding support for a new database source connector to the `v2/spanner-to-sourcedb` reverse migration template. It details prerequisites, scope boundaries, type mapping requirements, connector implementation, registry registration, unit testing, and smoke testing procedures.

---

## Prerequisites

> [!CRITICAL]
> **MANDATORY PREREQUISITE GATE**:
> Before running code searches, inspecting files, or executing unit tests, you MUST verify if the user provided the inputs below.
> If any required input is missing, **STOP IMMEDIATELY** and ask the user for clarification before proceeding further.

1. **Datatype Mapping File**: Ask the user to provide or point to the Datatype Mapping Matrix for the new target database, defining the mappings between Spanner GoogleSQL and PostgreSQL dialects and the destination database datatypes.
2. **Test Setup Details**: Ask the user for the test environment details required for live smoke testing, including:
   * Target Database Instance details (host/instance name, port, database name, and credentials/connection method).
   * Source Spanner Instance & Database details (with Change Stream configured).
   * Shard configuration file path or details (if multi-shard / sharded migration).

---

## Architectural Boundaries & Code Scope
All implementation for a new source connector MUST be strictly confined to:
1. **Source Connector Package**: `v2/spanner-to-sourcedb/src/main/java/com/google/cloud/teleport/v2/templates/source/<source_type>/`
   * Implementation of `ISpToSrcSourceConnector` (`<Source>SpToSrcSourceConnector.java`)
   * Implementation of `IDMLGenerator` (`<Source>DMLGenerator.java`)
   * Source-specific schema, connection, and DAO classes in package `<source_type>` (e.g., `<Source>Dao.java`, `<Source>ConnectionHelper.java`, `<Source>TypeHandler.java`).
2. **Connector Registry / Factory**: `SourceProcessorFactory.java` (`v2/spanner-to-sourcedb/src/main/java/com/google/cloud/teleport/v2/templates/dbutils/processor/SourceProcessorFactory.java`)
   * Dynamic connector registration via `sourceMap.put(Constants.SOURCE_<SOURCE>, new <Source>SpToSrcSourceConnector())`.
3. **Constants**: `Constants.java` (`v2/spanner-to-sourcedb/src/main/java/com/google/cloud/teleport/v2/templates/constants/Constants.java`)
   * Definition of source identifier constant (e.g., `public static final String SOURCE_<SOURCE> = "<source_type>";`).
4. **Shared Core Registries & Configs**: `v2/spanner-common`
   * Registries and configuration files in `spanner-common` (updated similar to existing sources).

---

## Datatype Mapping Matrix Requirements
Consult the Datatype Mapping Matrix provided in the prerequisites to verify correct datatype conversion between Spanner GoogleSQL/PostgreSQL dialects and the target database. Ensure proper alignment for character, numeric, temporal, binary, boolean, JSON, and any other datatypes as specified in the mapping file.

---

## Step-by-Step Implementation Workflow

### Step 0: Mandatory Prerequisite Gate
1. **Verify Inputs**: Inspect the request for the required inputs. If the Datatype Mapping Matrix and Test Setup Details are not provided in the user request, **DO NOT run any inspection or execution tools**. Stop and ask the user for the missing details first.

### Step 1: Implement `<Source>SpToSrcSourceConnector`
Implement `ISpToSrcSourceConnector` in `com.google.cloud.teleport.v2.templates.source.<source_type>`:
1. Implement `getDmlGenerator()`, `getConnectionHelper()`, `getDao(Shard shard)`, `initConnectionHelper(...)`, `parseShardConfig(...)`, `validate(...)`, `getInformationSchema(...)`, `supportsSharding()`, `shouldUpdateReadValuesToSpannerRecord()`, and `classifyException(...)`.
2. Configure connection parameters and dialect-specific setup to work for GoogleSQL and PostgreSQL Spanner dialects similar to other supported sources.

### Step 2: Implement `<Source>DMLGenerator` and Source Processing Classes
1. Implement `IDMLGenerator` (`<Source>DMLGenerator.java`) in `com.google.cloud.teleport.v2.templates.source.<source_type>` to generate dialect-specific DML statements (`INSERT`, `UPDATE`, `DELETE`) for the target database.
2. Implement remaining source-specific change event context, schema scanning, type mapping, and DAO classes in package `com.google.cloud.teleport.v2.templates.source.<source_type>` similar to other existing source connectors.

### Step 3: Register Source in Constants & Processor Factory
1. Add the source constant in `Constants.java`:
   ```java
   public static final String SOURCE_<SOURCE> = "<source_type>";
   ```
2. Register connector in `SourceProcessorFactory.java`:
   ```java
   sourceMap.put(Constants.SOURCE_<SOURCE>, new <Source>SpToSrcSourceConnector());
   ```
3. Update corresponding registries and converter files in `v2/spanner-common` similar to existing sources.

### Step 4: Unit Testing & Verification
Execute Maven unit tests for the template:
```bash
mvn test -pl v2/spanner-to-sourcedb \
  -Dtest=<Source>SpToSrcSourceConnectorTest,SourceProcessorFactoryTest
```
All unit tests must pass with `BUILD SUCCESS`.

### Step 5: Mandatory Live Smoke Testing & Verification

> [!CRITICAL]
> **MANDATORY EXECUTION REQUIREMENT**:
> Immediately after unit tests pass, you **MUST AUTOMATICALLY PROCEED** to execute live end-to-end smoke testing using the test environment details provided in the prerequisites.
> Do NOT stop, pause, or declare completion after unit testing without running the live smoke tests.

> [!IMPORTANT]
> **WORKER MACHINE TYPE DIRECTIVE**:
> When submitting the Dataflow job for live smoke testing, you **MUST** explicitly specify a worker machine type of the correct size with at least 4 vCPUs (e.g., `--worker-machine-type=n2-standard-4`). Omitting this parameter will cause Dataflow job launch validation to fail with a machine specification policy violation.

1. **Environment Setup**:
   * Connect to the source Spanner database instance (with change stream enabled) and target database instance configured in the test setup.
2. **Perform CRUD Operations**:
   * **INSERT**: Execute `INSERT` statements covering supported datatypes in the source Spanner database.
   * **UPDATE**: Update existing rows in the source Spanner database.
   * **DELETE**: Delete test rows from the source Spanner database.
3. **Verify Replication Flow**:
   * Verify that Spanner Change Stream events flow through Dataflow to the target database.
   * Query the target database tables to confirm that `INSERT`, `UPDATE`, and `DELETE` operations are accurately reflected in the destination database.
4. **Retry Loop on Failure**:
   * If any CRUD operation fails to replicate or produces data discrepancies in the target database, inspect error logs, modify the connector, DML generator, or DAO code, rebuild, and re-test until all operations pass cleanly.
