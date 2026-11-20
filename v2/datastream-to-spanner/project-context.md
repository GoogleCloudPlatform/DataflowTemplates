# Project Context: Datastream to Spanner

<!-- 
AI SYSTEM DIRECTIVES (CRITICAL):
1. Role: Act as a Senior Software Engineer for this project.
2. Read: You must parse this document before starting any task.
3. Write/Override: You are explicitly authorized to overwrite, modify, or delete existing entries in this file when they become outdated or are superseded by new decisions.
4. Maintain: Actively prune the "AI Agent Tips" section to prevent context window bloat.
5. Plan: When creating or modifying an Implementation Plan artifact, you MUST explicitly include a step to review this project-context.md file to ensure your plan aligns with established architectural decisions and gotchas.
-->

## Overview

*   **Core Intent:** The Dataflow template is a streaming CDC (Change Data Capture) migration pipeline that applies real-time database changes to Cloud Spanner. **Important distinction:** This template does *not* read directly from the source database or pipe data to Datastream. Instead, Datastream independently reads from the source DB and writes the change events as Avro (or JSON) files to a GCS bucket. This Dataflow template then consumes those files from GCS, converts the Avro records into internal JSON representations, and applies the changes to Spanner. It's meant to reduce application downtime by applying in-flight changes while bulk data loads happen.
*   **Primary Users:** Database administrators, migration engineers, and customers moving databases to Cloud Spanner.
*   **Critical SLOs/Guarantees:** Must ensure eventual consistency with the source database by never allowing older events to overwrite newer ones (preserving commit order).
*   **Terminology:**
    *   **Change Event (CE):** A DML change (insert/update/delete) that contains the full row data.
    *   **Shadow Table:** A companion table created alongside each Spanner destination table to keep track of versioning metadata (like an Oracle SCN) for each primary key.
    *   **DLQ:** Dead Letter Queue (for failed records).
    *   **Oracle SCN:** System Change Number. A version number ensuring events are committed in the right order.
    *   **Schema Override:** A mechanism to override the schema of the destination database.

## Technical Details

*   **Tech Stack & Versions:**
    *   **Languages:** Java 17
    *   **Frameworks/Libraries:** Apache Beam, GCP Spanner SDK, Datastream API Client.
    *   **Key Technologies:** Cloud Spanner, Cloud Dataflow, Datastream, Google Cloud Storage (GCS)
*   **Code Location:** `v2/datastream-to-spanner`
*   **Data Flow:** Source Database -> Datastream -> Google Cloud Storage (Avro/JSON) -> Cloud Dataflow Pipeline (consumes Avro/JSON from GCS, converts Avro to JSON, transforms, schema validates) -> Cloud Spanner. Failed events are sent to a Dead Letter Queue (DLQ) in GCS for recycling/retrying.
*   **Project Structure (Logical Architecture Mapping):**
    *   `v2/datastream-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates`: Core dataflow pipeline logic (`DataStreamToSpanner.java`) and DoFns.
    *   `v2/datastream-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/datastream`: Datastream mapping, JSON/Avro parsing, and dialect-specific parsing contexts.
    *   `v2/datastream-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/spanner`: Spanner-specific logic including `SpannerTransactionWriter` and Schema overrides parser.
    *   `v2/datastream-to-spanner/src/main/java/com/google/cloud/teleport/v2/templates/transform`: Custom transformation and processing logic for change events.
    *   `v2/datastream-to-spanner/src/test/java/com/google/cloud/teleport/v2/templates`: Unit and Integration tests.
*   **Build/Run Commands:**
    See the `README.md` file for instructions on building and running the pipeline.

## Documentation

*   **Architecture Diagram & Dependency Tree:** [architecture.svg](architecture.svg) (Source: `architecture.dot`).
    *   **Rule:** Always keep the `.dot` and `.svg` files in sync. If you modify the architecture, you MUST regenerate the `.svg` from the `.dot` file.
*   **User Guide & Advanced Config:** See [README_Cloud_Datastream_to_Spanner.md](README_Cloud_Datastream_to_Spanner.md) for detailed operational guidelines, troubleshooting, and advanced parameters (like `shardingContextFilePath`, `sessionFilePath`, `shadowTableSpannerInstanceId`, and custom Java transformations).

## AI Agent Tips

*   **Common Tasks:** Adding new dialect support for Datastream, improving retry logic for the DLQ, adding transformations or metrics for the Dataflow pipeline.
*   **Coding Standards & Best Practices:**
    *   Individual CEs are processed separately for parallel scaling, rather than grouping them into the original source transactions. Consistency is managed using lateness checks on the Shadow Tables.
    *   **Avoid Serial Processing:** Do not attempt to group events by transaction or serially order them. The approach relies on parallel workers, taking advantage of Cloud Dataflow's scale.
    *   **Avoid GroupBy:** Do not use `GroupByKey` or internal worker state to filter stale events before writing. It doesn't scale well and complicates state recovery. Always use Shadow Tables for the lateness check.
    *   Because CE writes are idempotent and protected by version checks, the template relies heavily on automatic retries for failed events, reducing the complexity of referential integrity (e.g., when a child arrives before a parent).
    *   **Referential Integrity:** For foreign keys and interleaved tables, rely purely on the retry mechanisms. A child event arriving before its parent will fail, but will eventually be written when it is retried after the parent succeeds.
    *   Schema migration must be done *prior* to starting this pipeline; it does not process or replicate DDL events.
*   **Testing Frameworks & Guidelines:**
    *   **Frameworks:** JUnit 4, Mockito for mocking dependencies.
    *   **Rules:** Ensure adequate UT coverage for new logic. Integration tests should be placed in the respective `*IT.java` classes with robust wait conditions.
*   **Core Architectural Decisions:**
    *   Lateness checks on Shadow Tables are critical; bugs here can lead to data inconsistency.
    *   DLQ retry logic (both `retryDLQ` and `retryAllDLQ` modes) handles data integrity on errors. Modifying it must be done carefully to prevent infinite loops or skipped events.
*   **Known Issues & Quirks:**
    *   **Version Overflow:** Be mindful of edge cases in version ordering (e.g. if the Oracle SCN exceeds limits and restarts at zero). Ensure comparisons in `ChangeEventSequence` remain robust against edge case overflows.
*   **Lessons Learned & Ah-ha Moments:**
    *   **Fatal Errors:** Unexpected/fatal errors (like type conversion failures) should not be endlessly retried. Ensure any new exceptions are properly routed to the severe DLQ bucket.
*   **Example PRs:**
    *   [PR #3035](https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/3035) - [datastream-to-spanner] Unable to convert field timestamp to long
    *   [PR #2867](https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/2867) - changed mysql event ordering in datastream to spanner
