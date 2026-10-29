---
name: add-source-cdc-data-generator
description: >-
  Guide for implementing a database source connector in the v2/cdc-data-generator
  Dataflow template. Details scope boundaries, prerequisites, connector implementations,
  factory registrations, unit testing, and smoke testing guidelines.
---

# Skill: Implement Source in CDC Data Generator Template

## Overview
This skill provides a step-by-step procedure for adding support for a new database source connector to the `v2/cdc-data-generator` template. It details prerequisites, scope boundaries, type mapping requirements, connector implementation, factory registration, unit testing, and smoke testing procedures.

---

## Prerequisites

> [!CRITICAL]
> **MANDATORY PREREQUISITE GATE**:
> Before running code searches, inspecting files, or executing unit tests, you MUST verify if the user provided the inputs below.
> If any required input is missing, **STOP IMMEDIATELY** and ask the user for clarification before proceeding further.

1. **Datatype Mapping File**: Ask the user to provide or point to the Datatype Mapping Matrix for the new source database, defining the supported source database datatypes and their corresponding target mappings.
2. **Test Setup Details**: Ask the user for the test environment details required for smoke testing, including:
   * Source Database Instance details (host/instance name, port, database/service name, credentials/connection method, and schema/namespace if applicable).
   * Shard configuration / sink options details and GCP project / staging GCS bucket information.

---

## Architectural Boundaries & Code Scope
All implementation for a new source connector MUST be strictly confined to:
1. **Source Connector Package**: `v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/source/<source_type>/`
   * Implementation of `ISourceConnector` (`<Source>SourceConnector.java`)
   * Source-specific config, schema fetcher, type mapper, and data writer classes in package `<source_type>` (inspect and implement similar to existing source packages such as `v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/source/mysql/`).
2. **Connector Factory**: `SourceConnectorFactory.java` (`v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/source/SourceConnectorFactory.java`)
   * Dynamic connector lookup in `getSourceConnector(SinkType)` and `getSourceConnectorBySourceType(String)`.
3. **Pipeline Options & Constants**:
   * `CdcDataGeneratorOptions.java` (`v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/templates/CdcDataGeneratorOptions.java`) for `SinkType` enum and `@TemplateParameter.Enum` registration.
   * `Constants.java` (`v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/templates/utils/Constants.java`) and `v2/cdc-data-generator/pom.xml` (if a driver dependency is required).
4. **Shared Core Schema Scanners & Utilities**: `v2/spanner-common`
   * Schema scanner and shared utilities in `v2/spanner-common` (updated similar to existing sources).

---

## Datatype Mapping Matrix Requirements
Consult the Datatype Mapping Matrix provided in the prerequisites to verify correct datatype conversion between the source database datatypes and `LogicalType`. Ensure proper alignment for character, numeric, temporal, binary, boolean, JSON, and any other dialect-specific datatypes as specified in the mapping file.

---

## Step-by-Step Implementation Workflow

### Step 0: Mandatory Prerequisite Gate
1. **Verify Inputs**: Inspect the request for the required inputs. If the Datatype Mapping Matrix and Test Setup Details are not provided in the user request, **DO NOT run any inspection or execution tools**. Stop and ask the user for the missing details first.

### Step 1: Implement `<Source>SourceConnector`
Implement `ISourceConnector` (`<Source>SourceConnector.java`) in `com.google.cloud.teleport.v2.source.<source_type>` by inspecting the files in existing source packages (e.g., `com.google.cloud.teleport.v2.source.mysql`) and implementing all methods of `ISourceConnector` similarly for the new source.

### Step 2: Implement Source Support Classes
Inspect all files in the existing source connector packages (such as `v2/cdc-data-generator/src/main/java/com/google/cloud/teleport/v2/source/mysql/`) and implement the corresponding sink config, schema fetcher, type mapper, and data writer classes in `com.google.cloud.teleport.v2.source.<source_type>` for the new database source. Ensure all supported types from the Datatype Mapping Matrix are covered.

### Step 3: Register Source in Options, Constants & Factory
1. Add the new source to `SinkType` and `@TemplateParameter.Enum` in `CdcDataGeneratorOptions.java`:
   ```java
   @TemplateParameter.TemplateEnumOption("<SOURCE>")
   ```
2. Add the source-specific constants in `Constants.java` (`v2/cdc-data-generator`), update `v2/spanner-common` if needed, and add any required driver dependency to `v2/cdc-data-generator/pom.xml`.
3. Register the connector in `SourceConnectorFactory.java`:
   * Update `getSourceConnector(SinkType)`
   * Update `getSourceConnectorBySourceType(String)`

### Step 4: Unit Testing & Verification
Inspect the existing unit tests in `v2/cdc-data-generator/src/test/java/com/google/cloud/teleport/v2/source/`, write the corresponding unit tests for the new source, and execute Maven unit tests for the template:
```bash
mvn test -pl v2/cdc-data-generator \
  -Dtest=<Source>*Test,SourceConnectorFactoryTest
```
All unit tests must pass with `BUILD SUCCESS`.

### Step 5: Mandatory Live Smoke Testing & Verification

> [!CRITICAL]
> **MANDATORY EXECUTION REQUIREMENT**:
> Immediately after unit tests pass, you **MUST AUTOMATICALLY PROCEED** to execute live end-to-end smoke testing using the test environment details provided in the prerequisites.
> Do NOT stop, pause, or declare completion after unit testing without running the live smoke tests.

1. **Environment & Schema Setup**:
   * Connect to the database instance configured in the test setup and create test tables covering all supported datatypes from the Datatype Mapping Matrix.
   * Upload the sink options configuration file to GCS.
2. **Perform Live Data Generation (INSERT, UPDATE, DELETE)**:
   * Launch the `cdc-data-generator` Dataflow pipeline with `sinkType=<SOURCE>` and non-zero `insertQps`, `updateQps`, and `deleteQps`.
3. **Verify Generation Flow**:
   * Monitor the pipeline execution and DLQ directory to ensure zero `writeFailures` or `generationFailures`.
   * Query the target database tables directly to confirm that `INSERT`, `UPDATE`, and `DELETE` operations are accurately executed across all supported datatypes.
4. **Retry Loop on Failure**:
   * If any CRUD operation fails to write or produces DLQ records or data discrepancies in the database, inspect error logs and DLQ payloads, modify the source connector classes, rebuild, and re-test until all operations pass cleanly.
