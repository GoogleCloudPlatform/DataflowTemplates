---
name: add-source-sourcedb-to-spanner
description: >-
  Guide for implementing a database source connector in the v2/sourcedb-to-spanner
  forward migration Dataflow template. Details scope boundaries, prerequisites, connector implementations,
  registry registrations, unit testing, and smoke testing guidelines.
---

# Skill: Implement Source in SourceDb to Spanner Template

## Overview
This skill provides a step-by-step procedure for adding support for a new database source connector to the `v2/sourcedb-to-spanner` forward migration template. It details prerequisites, scope boundaries, type mapping requirements, connector implementation, factory registration, unit testing, and smoke testing procedures.

---

## Prerequisites

> [!CRITICAL]
> **MANDATORY PREREQUISITE GATE**:
> Before running code searches, inspecting files, or executing unit tests, you MUST verify if the user provided the inputs below.
> If any required input is missing, **STOP IMMEDIATELY** and ask the user for clarification before proceeding further.

1. **Datatype Mapping File**: Ask the user to provide or point to the Datatype Mapping Matrix for the new source database, defining the mappings between source database datatypes and Spanner (GoogleSQL and PostgreSQL dialects) datatypes or `UnifiedMappingProvider.Type`.
2. **Test Setup Details**: Ask the user for the test environment details required for smoke testing, including:
   * Source Database Instance details (host/instance name, port, database name, credentials/connection method, and schema/namespace if applicable).
   * Target Spanner Instance & Database details.

---

## Architectural Boundaries & Code Scope
All implementation for a new source connector MUST be strictly confined to:
1. **Source Connector Package**: `v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/source/<source_type>/`
   * Implementation of `ISrcToSpSourceConnector` (`<Source>SrcToSpSourceConnector.java`) or subclassing `AbstractJdbcSrcToSpSourceConnector` (for JDBC sources).
   * Dialect adapter, value mapping provider, and config defaults classes in package `<source_type>` (e.g., `<Source>ConfigDefaults.java`).
2. **Connector Factory**: `SourceConnectorFactory.java` (`v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/source/SourceConnectorFactory.java`)
   * Dynamic connector lookup in `getSourceConnectorByDialect()`, `getSourceConnectorBySourceType()`, and `getSourceJdbcConnectorByDialect()`.
3. **Pipeline Options**: `SourceDbToSpannerOptions.java` (`v2/sourcedb-to-spanner/src/main/java/com/google/cloud/teleport/v2/options/SourceDbToSpannerOptions.java`)
   * Source dialect constant definition (e.g., `String <SOURCE>_SOURCE_DIALECT = "<SOURCE_DIALECT_NAME>";`) and enum registration in `@TemplateParameter.Enum`.
4. **Shared Core Registries & Configs**: `v2/spanner-common`
   * Registries and constant files in `spanner-common` (e.g. `Constants.java` / `SourceConstants.java` for `public static final String <SOURCE>_SOURCE_TYPE = "<source_type>";`).

---

## Datatype Mapping Matrix Requirements
Consult the Datatype Mapping Matrix provided in the prerequisites to verify correct datatype conversion between the source database datatypes and `UnifiedMappingProvider.Type` / Spanner target datatypes. Ensure proper alignment for character, numeric, temporal, binary, boolean, JSON, and any other dialect-specific datatypes as specified in the mapping file.

---

## Step-by-Step Implementation Workflow

### Step 0: Mandatory Prerequisite Gate
1. **Verify Inputs**: Inspect the request for the required inputs. If the Datatype Mapping Matrix and Test Setup Details are not provided in the user request, **DO NOT run any inspection or execution tools**. Stop and ask the user for the missing details first.

### Step 1: Implement `<Source>SrcToSpSourceConnector`
Implement `ISrcToSpSourceConnector` (or extend `AbstractJdbcSrcToSpSourceConnector` for JDBC sources) in `com.google.cloud.teleport.v2.source.<source_type>`:
1. Define type mappings between source datatypes and `UnifiedMappingProvider.Type` in `getTypeMapping()`. Ensure all the types mentioned in the datatype mapping are covered.
2. For sources which have a beam IO library, use that library.For JDBC supported sources, implement the relevant JDBC connector methods similar to existing JDBC sources.
3. Configure connection parameters and dialect-specific setup to work with Spanner target databases (GoogleSQL and PostgreSQL dialects).
4. For the types marked as primary key supported in the data type mapping ensure an implementation for the splitter is provided.

### Step 2: Implement Source IO Wrapper Config Defaults and Schema Discovery
1. Create source configuration defaults (e.g. `<Source>ConfigDefaults.java`), dialect adapter, and value mappings provider in package `com.google.cloud.teleport.v2.source.<source_type>`.
2. Configure schema discovery, fetch size, connection pooling, and uniform partitioning parameters appropriate for the source database.

### Step 3: Register Source in Options, Constants & Factory
1. Add the source dialect constant in `SourceDbToSpannerOptions.java` and update `@TemplateParameter.Enum`:
   ```java
   String <SOURCE>_SOURCE_DIALECT = "<SOURCE_DIALECT_NAME>";
   ```
2. Add the source type constant in `Constants.java` / `SourceConstants.java` in `v2/spanner-common`:
   ```java
   public static final String <SOURCE>_SOURCE_TYPE = "<source_type>";
   ```
3. Register the connector in `SourceConnectorFactory.java`:
   * Update `getSourceConnectorByDialect(...)`
   * Update `getSourceConnectorBySourceType(...)`
   * Update `getSourceJdbcConnectorByDialect(...)` (for JDBC sources)

### Step 4: Unit Testing & Verification
Execute Maven unit tests for the template:
```bash
mvn test -pl v2/sourcedb-to-spanner \
  -Dtest=<Source>SrcToSpSourceConnectorTest,SourceConnectorFactoryTest
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
   * Connect to the source database instance and target Spanner database instance configured in the test setup.
2. **Populate Source Data**:
   * Insert test rows covering all supported datatypes into the source database.
3. **Verify Replication / Migration Flow**:
   * Launch the `sourcedb-to-spanner` Dataflow pipeline.
   * Query the target Spanner database tables to confirm that source rows are accurately migrated to destination Spanner tables.
4. **Retry Loop on Failure**:
   * If any record fails to migrate or produces data discrepancies in Spanner, inspect error logs, modify connector, IO wrapper, or mapping code, rebuild, and re-test until all operations pass cleanly.
