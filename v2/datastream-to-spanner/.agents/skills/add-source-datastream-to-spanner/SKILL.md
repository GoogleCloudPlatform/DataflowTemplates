---
name: add-source-datastream-to-spanner
description: >-
  Guide for implementing a database source connector in the v2/datastream-to-spanner
  forward migration Dataflow template. Details scope boundaries, prerequisites, connector implementations,
  registry registrations, unit testing, and smoke testing guidelines.
---

# Skill: Implement Source in Datastream to Spanner Template

## Overview
This skill provides a step-by-step procedure for adding support for a new database source connector to the `v2/datastream-to-spanner` template. It details prerequisites, scope boundaries, type mapping requirements, connector implementation, registry registration, unit testing, and smoke testing procedures.

---

## Prerequisites

> [!CRITICAL]
> **MANDATORY PREREQUISITE GATE**:
> Before running code searches, inspecting files, or executing unit tests, you MUST verify if the user provided the inputs below.
> If any required input is missing, **STOP IMMEDIATELY** and ask the user for clarification before proceeding further.

1. **Datastream Source Compatibility Check**: Verify that Google Cloud Datastream natively supports the requested source database type by referencing [Datastream Supported Sources](https://docs.cloud.google.com/datastream/docs/sources).
   * > [!CRITICAL]
   * > If the requested source database is **NOT supported by Datastream**, **STOP IMMEDIATELY** and inform the user that Datastream does not support CDC streaming for this database source. Do NOT proceed with inspection, implementation, or testing.
2. **Datatype Mapping File**: Ask the user to provide or point to the Datatype Mapping Matrix (e.g. CSV file or mapping specification) for the new source database, defining the mappings to Spanner GoogleSQL and PostgreSQL dialects.
3. **Test Setup Details**: Ask the user for the test environment details required for live smoke testing, including:
   * Source Database Instance details (host/instance name, database name, and credentials/connection method).
   * Target Spanner Instance & Database details.
   * Datastream stream / connection profile setup or GCP project information.

---

## Architectural Boundaries & Code Scope
All implementation for a new source connector MUST be strictly confined to:
1. **Source Connector Package**: `v2/datastream-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/source/<source_type>/`
   * Implementation of `IDsToSpSourceConnector` (`<Source>DsToSpSourceConnector.java`)
   * Source-specific change event processing classes in package `<source_type>` (implemented similar to other existing source connectors in the codebase).
2. **Connector Registry**: `DatastreamToSpannerSourceConnectorRegistry.java`
   * Dynamic connector registration via `register(new <Source>DsToSpSourceConnector())`.
3. **Shared Core Registries & Configs**: `v2/spanner-common`
   * Registries and configuration files in `spanner-common` (updated similar to existing sources).

---

## Datatype Mapping Matrix Requirements
Consult the Datatype Mapping Matrix provided in the prerequisites to verify correct datatype conversion to Spanner GoogleSQL and PostgreSQL dialects. Ensure proper alignment for character, numeric, temporal, binary, boolean, JSON and any other datatypes as specified in the mapping file.

---

## Step-by-Step Implementation Workflow

### Step 0: Mandatory Prerequisite Gate
1. **Verify Datastream Support**: Check if the requested source database type is supported by Datastream per [Datastream Sources](https://docs.cloud.google.com/datastream/docs/sources). If unsupported, **STOP IMMEDIATELY** and notify the user.
2. **Verify Inputs**: Inspect the request for the required inputs. If the Datatype Mapping Matrix and Test Setup Details are not provided in the user request, **DO NOT run any inspection or execution tools**. Stop and ask the user for the missing details first.

### Step 1: Implement `<Source>DsToSpSourceConnector`
Implement `IDsToSpSourceConnector` in `com.google.cloud.teleport.v2.templates.source.<source_type>`:
1. Define CDC metadata key constants required by the source.
2. Implement any other methods in this class to work for GoogleSQL and PostgreSQL Spanner dialects similar to other supported sources.

### Step 2: Implement Source Processing Classes
Implement remaining source-specific change event context and sequence classes in package `com.google.cloud.teleport.v2.templates.source.<source_type>` similar to other existing source connectors.

### Step 3: Register Source in Registries & Config Files
1. Register connector in `DatastreamToSpannerSourceConnectorRegistry.java`:
   ```java
   register(new <Source>DsToSpSourceConnector());
   ```
2. Update corresponding registries and converter files in `v2/spanner-common` similar to existing sources.

### Step 4: Unit Testing & Verification
Execute Maven unit tests for the template:
```bash
mvn test -pl v2/datastream-to-spanner \
  -Dtest=<Source>DsToSpSourceConnectorTest,DatastreamToSpannerSourceConnectorRegistryTest
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
   * Connect to the live source database instance and target Spanner instance configured in the test setup.
2. **Perform CRUD Operations**:
   * **INSERT**: Execute `INSERT` statements covering supported datatypes in the source database.
   * **UPDATE**: Update existing rows in the source database.
   * **DELETE**: Delete test rows from the source database.
3. **Verify Replication Flow**:
   * Verify that CDC events flow through Datastream to Cloud Storage and Dataflow.
   * Query Cloud Spanner tables to confirm that `INSERT`, `UPDATE`, and `DELETE` operations are accurately reflected in Spanner.
4. **Retry Loop on Failure**:
   * If any CRUD operation fails to replicate or produces data discrepancies in Spanner, inspect error logs, modify the connector and converter code, rebuild, and re-test until all operations pass cleanly.
