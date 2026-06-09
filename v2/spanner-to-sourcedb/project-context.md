# Project Context: Spanner to SourceDb (Reverse Replication)

<!-- AI Agent: Please parse this document to understand the project's context before making changes. -->

## Overview

*   **Core Intent**: A streaming reverse migration Dataflow pipeline to replicate data from Cloud Spanner back into various Source Databases (MySQL, PostgreSQL, Cassandra). It implements per-primary-key ordering guarantees using shadow tables, allowing for high throughput.
*   **Primary Users**: SREs and external customers migrating off Cloud Spanner and executing a "cut-back" to their original source database.
*   **Critical SLOs/Guarantees**:
    *   Per-primary-key ordering guarantee (relaxed from per-shard ordering).
*   **Terminology**:
    *   **Reverse Replication**: Replicating data from Spanner to the Source DB.
    *   **Change Streams**: Spanner's CDC mechanism.
    *   **Shadow Table**: Spanner metadata table used to track `processed_commit_ts` per primary key to prevent out-of-order writes.
    *   **Data Freshness**: Pipeline lag indicator.
    *   **Cut-back**: The process of shifting application write traffic back to the source.
    *   **DLQ:** Dead Letter Queue (for failed records).

## Technical Details

*   **Tech Stack & Versions**:
    <!-- AI Agent: Identify and pin the exact versions of the tech stack (e.g., Java 11, Beam 2.XX) and note any limitations to prevent hallucinating newer features or deprecated APIs. -->
    *   **Languages**: Java 17
    *   **Frameworks/Libraries**: Apache Beam 2.73.0, Maven, HikariCP 5.0.1, Spanner Migrations SDK.
    *   **Key Technologies**: Cloud Spanner, Cloud Storage (GCS) DLQ, MySQL, PostgreSQL, Cassandra.
*   **Code Location**: `v2/spanner-to-sourcedb`
*   **Data Flow**: Data is captured via Spanner Change Streams -> Filtered/Preprocessed -> Dataflow ensures same-key records are processed by the same thread -> The `processed_commit_ts` is checked/updated in a Spanner shadow table -> Written to Source Shards via JDBC/Cassandra drivers. Failures are written to a GCS Dead Letter Queue (DLQ) for retries.
*   **Project Structure (Logical Architecture Mapping)**:
    <!-- AI Agent: Formulate project structure mapping to help any AI agent working on that template to have a general idea of the code and different important aspects of the code from the get-go. Map the pipeline stages or logical components directly to exact package paths so AI agents have spatial awareness. -->
    *   `src/main/java/com/google/cloud/teleport/v2/templates`: Main pipeline definition (`SpannerToSourceDb.java`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/changestream`: Utilities and convertors for parsing Spanner Change Streams (e.g., `TrimmedShardedDataChangeRecord`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/constants`: Project-level constants.
    *   `src/main/java/com/google/cloud/teleport/v2/templates/dbutils/connection`: Connection pool helpers (Hikari for JDBC, Cassandra connections).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/dbutils/dao/source`: DAOs for Source DB interaction (`JdbcDao.java`, `CassandraDao.java`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/dbutils/dao/spanner`: DAOs for Shadow Table metadata tracking (`SpannerDao.java`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/dbutils/dml`: DML generators for target databases (`MySQLDMLGenerator`, `PostgreSQLDMLGenerator`, `CassandraDMLGenerator`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/dbutils/processor`: Processors for handling mapped input records (`InputRecordProcessor`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/exceptions`: Custom runtime exceptions.
    *   `src/main/java/com/google/cloud/teleport/v2/templates/models`: POJOs and AutoValue models.
    *   `src/main/java/com/google/cloud/teleport/v2/templates/transforms`: Custom Beam transformations (e.g., `SourceWriterTransform`, `SpannerInformationSchemaProcessorTransform`).
    *   `src/main/java/com/google/cloud/teleport/v2/templates/utils`: Generic pipeline utilities.
*   **Build/Run Commands**:
    <!-- AI Agent: Direct developers and agents to the project's README.md for up-to-date execution instructions to avoid duplication. -->
    See the `README_Spanner_to_SourceDb.md` file for instructions on building and running the pipeline.

## Documentation

*   **Architecture Diagram & Dependency Tree**: [architecture.svg](architecture.svg) (Source: `architecture.dot`).
    <!-- AI Agent: Deep dive into the code to understand the architecture and dependency tree. Generate a GraphViz DOT file, convert it to SVG, and embed the SVG into this document. Add the DOT and SVG files to the repository. -->
    *   **Rule**: Always keep the `.dot` and `.svg` files in sync. If you modify the architecture, you MUST regenerate the `.svg` from the `.dot` file.

## AI Agent Tips

*   **Common Tasks**: Adding DML generators for new databases, handling new types of schema overrides, improving unit test coverage, updating DLQ processing logic.
*   **Coding Standards & Best Practices**:
    *   **Parallelism**: Controlled via `maxShardConnections` per shard configuration.
    *   **Stale Writes**: Any writes with an older timestamp than what is recorded in the shadow table must be explicitly skipped to prevent data corruption.
    *   **AutoValue**: Use `AutoValue` for POJOs and models. Ensure all required variables are set before building.
    *   **Beam Paradigms**: Strictly adhere to Apache Beam constructs (`PTransform`, `DoFn`). Use `TupleTag` for handling multiple outputs like DLQ side channels.
    *   **Serializability**: All variables within `PTransform` and `DoFn` must be serializable. For non-serializable objects (like JDBC Connections, `HikariDataSource`, or `SpannerDao`), mark them as `transient` and initialize them within `@Setup` or `@StartBundle` methods.s
    *   **Connection Management**: Use the Hikari connection pool (preferred over DBCP2). Always ensure connections are safely returned to the pool (using `try-with-resources` or `try-finally` blocks) to prevent connection leakages.
    *   **Security**: NEVER log sensitive credentials, connection strings, or customer PII. Use `structured-logging` instead of standard output.
    *   **Formatting**: Always run `mvn spotless:apply -pl v2/spanner-to-sourcedb -am` before committing to adhere to project formatting standards.
*   **Testing Frameworks & Guidelines**:
    *   **Frameworks**: JUnit 4, AssertJ, and Mockito.
    *   **Unit Tests**: Use `@RunWith(JUnit4.class)`. Strive for at least 80% coverage. Mock database responses with Mockito to ensure fast, isolated tests.
    *   **Non-Destructive Refactoring**: Append new dedicated test methods for new functionality. Do not arbitrarily delete or rewrite existing tests unless addressing a breaking API change.
    *   **100% Branch & Exception Coverage**: Ensure `if/else` paths and caught/thrown exceptions are fully asserted in tests using AssertJ's `assertThatThrownBy` or JUnit's `assertThrows`.
*   **Areas to be Careful (Gotchas)**:
    *   **Integration Tests**: NEVER execute `*IT.java` (Integration) or `*LT.java` (Load) test suites during local development/machine verification. Only execute `*Test.java` (Unit) locally.
    *   **DML Generation Syntax**: When updating `MySQLDMLGenerator`, `PostgreSQLDMLGenerator`, or `CassandraDMLGenerator`, verify that the generated SQL/CQL syntax is strictly valid for that specific dialect and version.
    *   **Shadow Table Lock Contention**: Be cautious when adding transactional reads/writes around shadow tables. High throughput updates increase Spanner's load and can create lock contention. Keep transactions fast and localized.
    *   **Foreign Keys & Retries**: Parent-child ordering relies entirely on retry loops and SpannerIO's straggler handling. Changes to the error tag logic in `SourceWriterFn` or `AssignShardIdFn` can silently break foreign key insertions. Retryable errors expected in the DLQ include Foreign Key violations, Spanner `RESOURCE_EXHAUSTED` (shadow tables), and Source DB transient network issues.
    *   **Metric Reliability**: Metrics like `success_record_count` may be skewed if worker restarts occur (causing re-processing). Do not rely on them for strict transactional accounting.
    *   **Resume Strategy**: When resuming a paused job, users must supply a `startTimestamp` matching the previous `DataFreshness` minus a 10-minute buffer to ensure no events are dropped.
    *   **1:1 Row Mapping Assumption**: The pipeline assumes a single Spanner row does not map to more than one source row.
*   **Example PRs**:
    *   [d1dbadb17](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/d1dbadb17) - Feature: Adding support for PostgreSQL as source in reverse replication.
    *   [74e5f1fe1](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/74e5f1fe1) - Feature/Fix: Fail fast if MySQL destination is read-only.
    *   [23310bcea](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/23310bcea) - Bug Fix: Avoid multiple GCS reads for constructing schema mapper.
