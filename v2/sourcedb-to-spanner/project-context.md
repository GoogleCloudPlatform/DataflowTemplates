# Project Context: SourceDb to Spanner

<!-- 
AI SYSTEM DIRECTIVES (CRITICAL):
1. Role: Act as a Senior Software Engineer for this project.
2. Read: You must parse this document before starting any task.
3. Write/Override: You are explicitly authorized to overwrite, modify, or delete existing entries in this file when they become outdated or are superseded by new decisions.
4. Maintain: Actively prune the "AI Agent Tips" section to prevent context window bloat.
5. Plan: When creating or modifying an Implementation Plan artifact, you MUST explicitly include a step to review this project-context.md file to ensure your plan aligns with established architectural decisions and gotchas.
-->

## Overview

*   **Core Intent:** A bulk migration Dataflow pipeline to migrate data from various Source Databases (MySQL, PostgreSQL, Cassandra) into Cloud Spanner. It handles sharded and non-sharded databases. Writes eagerly to Spanner (no intermediate buffers) and works in tandem with a CDC pipeline (like Datastream) to reach eventual consistency.
*   **Primary Users:** SREs, external customers migrating to Cloud Spanner, and users of Spanner Migration Tool.
*   **Critical SLOs/Guarantees:** Must effectively handle bulk data extraction and mapping to Cloud Spanner mutations while maintaining data integrity. Features a Dead Letter Queue (DLQ) for failed mutations.
*   **Terminology:**
    *   **Pipeline Controller:** Central component managing the lifecycle, configuration parsing, and dependency ordering.
    *   **DLQ:** Dead Letter Queue (for failed records).
    *   **SourceRow:** Intermediate representation of a row read from the source database, typically wrapping an Avro `GenericRecord` based on Datastream's unified type system.
    *   **Mutation:** Spanner mutation to be applied.
    *   **RWUPT:** Read With Uniform Partitions.
    *   **UniformSourcePartitioner:** Custom partitioner used because standard Beam partitioners only split integer/datetime columns.

## Technical Details

*   **Tech Stack & Versions:**
    *   **Languages:** Java 17
    *   **Frameworks/Libraries:** Apache Beam 2.73.0, Maven
    *   **Key Google Technologies:** Cloud Spanner, Cloud Storage (GCS), Dataflow
*   **Code Location:** `v2/sourcedb-to-spanner`
*   **Data Flow:** Data is read from Source Databases (MySQL/PostgreSQL/Cassandra) using JDBC or Datastax driver -> Mapped into SourceRows -> Transformed to Spanner Mutations -> Written to Cloud Spanner. Failed mutations are logged to a GCS DLQ.
*   **Project Structure (Logical Architecture Mapping):**
    *   `src/main/java/com/google/cloud/teleport/v2/source/reader`: Source Readers (IoWrappers for Cassandra, JDBC, etc., RowMappers)
    *   `src/main/java/com/google/cloud/teleport/v2/transformer`: Transformers (e.g., `SourceRowToMutationDoFn`)
    *   `src/main/java/com/google/cloud/teleport/v2/writer`: Writers and error handling (`SpannerWriter`, `DeadLetterQueue`)
    *   `src/main/java/com/google/cloud/teleport/v2/templates`: Main pipeline definition (`SourceDbToSpanner`)
*   **Build/Run Commands:**
    See the `README_Sourcedb_to_Spanner_Flex.md` file for instructions on building and running the pipeline.

## Documentation
*   **Architecture Diagram:** [architecture.svg](architecture.svg) (Source: `architecture.dot`).
    *   **Rule:** Always keep the `.dot` and `.svg` files in sync. If you modify the architecture, you MUST regenerate the `.svg` from the `.dot` file.

## AI Agent Tips

*   **Common Tasks:** Adding new JDBC dialects, fixing parsing errors, implementing new transformations or schema overrides, adding new source reader capabilities.
*   **Coding Standards & Best Practices:**
    *   Use `AutoValue` for POJOs. Do not bypass or omit variables required by the AutoValue builder.
    *   Strict adherence to Apache Beam paradigms (PTransforms, DoFns). Use `TupleTag` for side outputs like the DLQ.
    *   **Serializability:** All elements that are members of `PTransforms` and `PCollections` MUST be serializable. Use `Serializable` interface or register an appropriate `Coder`. Mark non-serializable IO channels or active connection clients `transient` and instantiate them strictly within `@Setup` or `@StartBundle`.
    *   **Security:** NEVER log sensitive credentials or customer PII.
    *   **Type Handling:** Time-based fields MUST be normalized to UTC and encoded as ISO-8601 with nanosecond precision. String fields must correctly map source charsets to Java UTF-16.
    *   **Separation of Concerns:** The Reader must encode the highest precision possible without data loss. Scaling/rounding to fit Spanner's limits is the strict responsibility of the Transformer.
    *   Use structured logging (`com.google.cloud.teleport.structured-logging`).
    *   **Formatting:** Always run `mvn spotless:apply -pl v2/sourcedb-to-spanner -am` before committing to adhere to project formatting standards.
*   **Testing Frameworks & Guidelines:**
    *   **Frameworks:** JUnit 4, Google Truth for assertions, Mockito for mocking.
    *   **Rules:** Ensure tests use `@RunWith(JUnit4.class)`. Use embedded databases for testing when possible (e.g. `derby` or `embedded-cassandra`). Maintain a minimum Unit test code coverage of 80%.
    *   **Non-Destructive Refactoring:** When enhancing production classes, do not refactor or rewrite existing test methods. Minimalistically resolve breaking changes and append new, dedicated test methods for new functionality.
    *   **100% Branch & Exception Coverage:**
        *   **Conditionals:** For every touched conditional (e.g., `if/else`, ternary operators), write tests covering both `true` and `false` paths.
        *   **Exceptions:** Assert all thrown checked and runtime exceptions explicitly via `assertThrows()` or Truth's `ThrowableSubject`.
*   **Core Architectural Decisions:**
    *   **Inconsistent Data Snapshots:** The reader intentionally does NOT read from a consistent snapshot. The companion CDC stream is trusted to replay updates and resolve mid-flight inconsistencies. Do not attempt to "fix" or lock tables for consistency.
*   **Known Issues & Quirks:**
    *   **Foreign Keys:** The pipeline processes parent tables before child tables, but cyclic (self-referencing) foreign keys will cause startup failures and are unsupported.
    *   **Pipeline Logic:** Cross-shard querying logic, causal ordering around the DLQ, and schema mappings parsing are highly complex areas.
    *   **Integration/Load Tests:** NEVER execute `*IT.java` (Integration) or `*LT.java` (Load) test suites during local coding/machine verification. These require remote environments. Only execute `*Test.java` (Unit) locally.
*   **Lessons Learned & Ah-ha Moments:**
    *   **OOM Prevention (MySQL Cursor Fetch):** Always configure `fetchSize` to prevent Out Of Memory errors.
*   **Example PRs:**
    *   [39a8ae5e0](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/39a8ae5e0) - Fix GCS Avro Export flow
    *   [90964dca6](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/90964dca6) - Add Support for UUID-based Partitioning
