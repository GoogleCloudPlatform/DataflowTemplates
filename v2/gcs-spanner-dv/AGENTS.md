# AGENTS.md: GCS Spanner Data Validator

> [!IMPORTANT]
> **For AI agents:** If this document conflicts with the code, the code wins — fix this document. When your change affects options, pipeline stages, BigQuery schemas, or a documented bug, update the matching section here (including the Mermaid diagram) in the same PR.

## Commands

```bash
# Compile:
mvn test-compile -pl v2/gcs-spanner-dv

# Unit tests:
mvn test -pl v2/gcs-spanner-dv -Dspotless.check.skip=true -Dcheckstyle.skip=true -Djacoco.skip=true

# Formatting / style:
mvn spotless:check -pl v2/gcs-spanner-dv

# Integration test (DataflowRunner; stages the template from local source into -DstageBucket):
mvn clean test -pl v2/gcs-spanner-dv -am -Dtest=<it_test_name> -Dproject=<project_id> -Dregion=<region> -DartifactBucket=<bucket_name> -DstageBucket=<bucket_name> -DspannerInstanceId=<spanner_instance_id> -Dspotless.check.skip=true -Dcheckstyle.skip=true -Djacoco.skip=true

# Build & stage the Flex Template:
mvn clean package -PtemplatesStage -DskipTests -DprojectId=<project_id> -DbucketName=<bucket_name> -DstagePrefix=<stage_prefix> -DtemplateName="Avro_to_Spanner_Data_Validator" -pl v2/gcs-spanner-dv -am
```

*   **DirectRunner ITs:** add `-DdirectRunnerTest` (and drop `-DstageBucket`). Faster and less resource intensive.
*   **Do not pass `-DspecPath=gs://dataflow-templates-<region>/latest/...` to validate a change** — it runs the *released* template, not your code. Use `-DspecPath` only to test an already-staged template.
*   **Deployment:** Terraform module in `terraform/Avro_to_Spanner_Data_Validator`, samples in `terraform/samples`.

---

## Overview

*   **Core Intent:** A batch Dataflow pipeline (class `GCSSpannerDV`, template `Avro_to_Spanner_Data_Validator`) that validates migration correctness by reading records from a source system (via GCS AVRO files) and destination Cloud Spanner, hashing the records, comparing them, and writing validation statistics and mismatch reports to BigQuery. The `gcs-spanner-dv` template is called AFTER the `sourcedb-to-spanner` dataflow template, which writes source rows in AVRO format to `<gcsOutputDirectory>/<sourceTableName>/<shardId>/*.avro` (the `<shardId>` directory segment is omitted for non-sharded runs) if `gcsOutputDirectory` is passed. The GCS Avro envelope schema (`SourceRowWithMetadata`) is defined in `SourceRow.gcsSchema()` (`SourceRow.java`), and payload column type mappings are defined in `UnifiedTypeMapper.java`.
*   **Supported Features & Configurations:**
    *   **Data Transformations:** Supported using custom transformations implementing `ISpannerMigrationTransformer` (`v2/spanner-migrations-sdk`), applied to source Avro records via `GenericRecordTypeConvertor.java` (`v2/spanner-common`; see sample implementations in `v2/spanner-custom-shard`, e.g., `CustomTransformationForDVIT.java`). Configured via `transformationJarPath`, `transformationClassName`, and `transformationCustomParameters`.
    *   **Schema Transformations:** Supported using overrides or session files (primarily for renaming tables/columns or dropping columns). Configured via `schemaOverridesFilePath`, `tableOverrides`, or `columnOverrides`, or `sessionFilePath` if using a session file.
    *   **Table Filtering:** Users can restrict validation to a subset of tables via `--tables` (comma-separated source table names) or `--tableConfigurationFilePath` (GCS JSON file `{"tableNames": ["t1", "t2"]}`, parsed into `TableConfiguration.java`). Both flags are mutually exclusive and accept **source** table names.
    *   **Sharded Sources:** The pipeline accounts for sharded database topologies. Users generally use one of two configurations:
        *   They may choose to have a `shardIdColumn` (default name: `migration_shard_id`) in their Spanner schema that stores the logical shard ID of the row's origin. This is currently supported by passing a session file so the pipeline is aware of the `shardIdColumn`. *(Note: Supporting this via the overrides file is planned as a follow-up.)*
        *   They may choose not to store this shard information directly in Spanner.

---

## Architecture & Data Flow

The following Mermaid diagram is the **Single Source of Truth (SOT)** for the pipeline architecture. External `.dot` and `.svg` files are not required.

```mermaid
flowchart TD
    subgraph Storage ["Storage & Databases"]
        GCS[("Google Cloud Storage<br/>(Avro Data)")]
        Spanner[("Cloud Spanner<br/>(Destination DB)")]
    end

    subgraph Config ["Configuration & Mapping"]
        TableConfig["TableConfiguration<br/>(--tables / --tableConfigurationFilePath)"]
        SchemaMapper["SchemaMapperProviderFn<br/>(Session / Overrides)"]
        CustomTransform["CustomTransformation<br/>(Optional JAR)"]
    end

    subgraph InfoSchema ["SpannerInformationSchemaProcessorTransform"]
        ProcessInfoSchema["ProcessInformationSchemaFn<br/>(Fetch Spanner DDL)"]
    end

    subgraph SourceReader ["SourceReaderTransform"]
        AvroRead["AvroIO.readAllGenericRecords"]
        SourceHash["SourceHashFn<br/>(ComparisonRecordMapper +<br/>UnifiedHasherVisitor)"]
        AvroRead --> SourceHash
    end

    subgraph SpannerReader ["SpannerReaderTransform"]
        CreateReadOps["CreateSpannerReadOpsFn"]
        BatchSpannerRead["SpannerIO.readAll<br/>(BatchSpannerRead, 15s staleness)"]
        SpannerHash["SpannerHashFn<br/>(ComparisonRecordMapper +<br/>UnifiedHasherVisitor)"]
        CreateReadOps --> BatchSpannerRead --> SpannerHash
    end

    subgraph MatchRecords ["MatchRecordsTransform"]
        CoGroup["CoGroupByKey<br/>(Key: Murmur3_128 Hash)"]
        Funnel["FunnelComparedRecordsFn<br/>(MATCHED_TAG /<br/>MISSING_IN_SPANNER_TAG /<br/>MISSING_IN_SOURCE_TAG)"]
        CoGroup --> Funnel
    end

    subgraph ReportResults ["ReportResultsTransform"]
        FormatMismatches["transformMismatchedRecords<br/>(MISSING_IN_DESTINATION /<br/>MISSING_IN_SOURCE)"]
        ComputeStats["ComputeTableStatsFn"]
        CombineSummary["ValidationSummaryCombineFn"]
        ComputeStats --> CombineSummary
    end

    subgraph BigQuery ["BigQuery (Validation Results)"]
        BQMismatches[("MismatchedRecords<br/>(FILE_LOADS)")]
        BQTableStats[("TableValidationStats<br/>(FILE_LOADS)")]
        BQSummary[("ValidationSummary<br/>(STREAMING_INSERTS)")]
    end

    Spanner --> ProcessInfoSchema
    ProcessInfoSchema -.->|"PCollectionView&lt;Ddl&gt;"| SourceHash
    ProcessInfoSchema -.->|"PCollectionView&lt;Ddl&gt;"| CreateReadOps
    ProcessInfoSchema -.->|"PCollectionView&lt;Ddl&gt;"| SpannerHash

    TableConfig -.-> AvroRead
    TableConfig -.-> CreateReadOps
    SchemaMapper -.-> AvroRead
    SchemaMapper -.-> SourceHash
    SchemaMapper -.-> CreateReadOps
    SchemaMapper -.-> SpannerHash
    CustomTransform -.-> SourceHash

    GCS --> AvroRead
    SourceHash -->|"Source ComparisonRecords (SOURCE_TAG)"| CoGroup

    Spanner --> BatchSpannerRead
    SpannerHash -->|"Spanner ComparisonRecords (SPANNER_TAG)"| CoGroup

    Funnel -->|"Mismatched tags"| FormatMismatches
    Funnel -->|"All tags"| ComputeStats

    FormatMismatches --> BQMismatches
    ComputeStats --> BQTableStats
    CombineSummary --> BQSummary
```

### Detailed Data Flow Steps
1. Source records are read from GCS and mapped to their Spanner representations using `ComparisonRecordMapper.java` inside `SourceHashFn.java`. (If a `CustomTransformation` is configured, source records are transformed before hashing.)
2. Spanner records are read directly across allowed tables in a single `BatchSpannerRead` stage and converted in `SpannerHashFn.java`.
3. Both sides are converted into a standard `ComparisonRecord.java` DTO and hashed in `ComparisonRecordMapper.buildRecord`: a Murmur3 128-bit hasher over alphabetically ordered column names and values (values are fed type-aware by `UnifiedHasherVisitor.java`), followed by the table name. `ComparisonRecord.java` carries only `(tableName, schemaName, primaryKeyColumns, hash, shardId)` — no row payload, so shuffle scales with row count rather than row width.
   * **Shard ID Asymmetry:** Source `ComparisonRecord`s populate `shardId` from the Avro envelope, whereas Spanner `ComparisonRecord`s always have `shardId = null` (so `MISSING_IN_SOURCE` rows in BigQuery always have `shard_id = NULL`).
4. `MatchRecordsTransform.java` performs a `CoGroupByKey` on this hash.
5. `FunnelComparedRecordsFn.java` routes records to TupleTags `MATCHED_TAG`, `MISSING_IN_SPANNER_TAG`, and `MISSING_IN_SOURCE_TAG` (mapped downstream in `ReportResultsTransform.java` to BigQuery mismatch types `MISSING_IN_DESTINATION` and `MISSING_IN_SOURCE`).
   * **Note on Mismatched Column Values:** Because grouping is done on the hash of the *entire* record, if a row exists in both source and destination with the same primary key but different column values, their hashes will differ. The pipeline logs one as `MISSING_IN_SOURCE` (for the Spanner version) and one as `MISSING_IN_DESTINATION` (for the source version), so each value-mismatched row counts twice in `mismatch_row_count`.
6. Finally, `ReportResultsTransform.java` writes `MismatchedRecords`, `TableValidationStats`, and `ValidationSummary` to BigQuery (`WRITE_APPEND`, keyed by `run_id`). Tables with 0 rows on both sides emit no `TableValidationStats` row and are not counted in `total_tables_validated`.

---

## Expected Scale
The entire pipeline (reading, hashing, matching, and reporting) must be designed to stream and scale to unbounded rows without in-memory accumulation.

*   **ValidationSummary:** Expected cardinality is 1 row per pipeline run.
*   **TableValidationStats:** Expected cardinality is 1 row per table (bounded by Cloud Spanner's limit of up to 5,000 tables per database).
*   **MismatchedRecords:** Must scale to an unbounded number of rows and data volume.

---

## Code Layout (`src/main/java/com/google/cloud/teleport/v2/`)

*   `templates`: pipeline entry point. `options`: pipeline options (`GCSSpannerDVOptions.java`). `config`: table filtering.
*   `transforms`: top-level PTransforms (one per stage in the diagram). `dofn`: DoFns for reading, hashing, matching, and stats. `fn`: schema mapper provider, summary combiner, and helpers.
*   `dto`: `ComparisonRecord`, BigQuery row DTOs, and `BigQuerySchemas.java`. `mapper`: `ComparisonRecordMapper` (Avro/Spanner → `ComparisonRecord`). `visitor`: type-aware hashing and PK string formatting.

Design doc: [go/gcs-spanner-dv-dd](http://go/gcs-spanner-dv-dd)

---

## AI Agent Tips

*   **Feature Completeness:** When implementing new features or making significant code changes, you MUST refer to the **Supported Features & Configurations** section (Data Transformations, Schema Transformations, Table Filtering, Sharded Sources) to ensure your changes gracefully support all configurations.
*   **Modifying BigQuery DTOs:**
    *   The BigQuery writes use `WRITE_APPEND` with no `schemaUpdateOptions`, so adding a column to `MismatchedRecord.java`, `TableValidationStats.java`, or `ValidationSummary.java` breaks appends into datasets created by an older template version. Account for this (e.g., schema update options or a migration note) whenever you change a BigQuery schema.
    *   Evaluate if the new field is fundamental to verifying the correctness of the pipeline's core logic. If so, update the corresponding test-assertion DTOs in `GCSSpannerDVTestAsserts.java`. Transient or dynamic execution metadata (such as `run_id` or timestamps) should be excluded from test DTOs.
*   **Data Type Mappings & Conversions:** `sourcedb-to-spanner` writes source records to Avro using Datastream unified types (see [Datastream Unified Types documentation](https://docs.cloud.google.com/datastream/docs/unified-types)). The DV pipeline casts these Avro values to Spanner `Value`s based on the target column datatype read directly from the Spanner DDL (there is no fixed Avro-to-Spanner mapping).
*   **Hashing Stability:** Do not change the hashing algorithm (`ComparisonRecordMapper.buildRecord` and `UnifiedHasherVisitor.java`) without cross-validating both the Avro and Spanner code paths to avoid breaking existing pipeline correctness.
*   **Unit Testing Guidelines:** Use JUnit 4 (`@RunWith(JUnit4.class)`), Apache Beam `TestPipeline` (`@Rule`) + `PAssert`, Google Truth, and Mockito (`mockito-inline`).
*   **Integration Tests**
    *   **Base Classes & Reference:** Standard ITs extend `GCSSpannerDVITBase.java` (reference: `GCSSpannerDVCoreMatchingIT.java`); end-to-end tests (`SourceDbToSpanner` -> `GCSSpannerDV`) extend `EndToEndTestingITBase.java` (reference: `BulkMigrationAndValidationE2EIT.java`).
    *   **Runner Selection:** Annotate `DirectRunner` tests with `@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})` and `DataflowRunner` tests with `@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})`. **Mandatory:** Any test using custom transformations MUST use `DataflowRunner` (`SkipDirectRunnerTest.class`) because `CustomTransformationImplFetcher` (`v2/spanner-common`, `.../spanner/migrations/utils/`) caches the transformer in a `static` field and leaks across concurrent `DirectRunner` tests in the same JVM. Reuse `CustomTransformationForDVIT.java` (in `v2/spanner-custom-shard`) when possible.
    *   **Test Data & Avro Envelopes:** Generate records programmatically at runtime using `GCSSpannerDVAvroSetupHelper.java` (`RecordBuilder` for standard `Users`/`AccountRoles` schemas) or `GenericRecordBuilder` (never check in binary `.avro` files). Place custom `.avsc`, session, and SQL files in an isolated `src/test/resources/<TestName>/` directory. Custom `.avsc` files must follow `src/test/resources/GCSSpannerDVAvroSetupHelper/users.avsc`: root record `SourceRowWithMetadata` with `tableName` (`string`), `shardId` (`["string", "null"]` with **no default**, so `.set("shardId", null)` is required when building), `primaryKeys` (`array` of `string`), and `payload` (where every column uses `["null", "<type>"]` with `"default": null`).
    *   **Spanner Staleness (`DirectRunner` only):** `SpannerReaderTransform.java` reads with a 15-second exact staleness bound. If a `DirectRunner` test writes rows directly into Spanner during setup, call `Thread.sleep(20000);` after writes before launching the pipeline (not needed for `DataflowRunner`).
    *   **Sharded ITs:** Tests using a `shardIdColumn` must load a session file (`sessionFilePath`) and place `shardIdColumn` as the first primary key column in the Spanner DDL (see `GCSSpannerDVShardedIT.java`).
    *   **Spanner Limits (`GCSSpannerDVWideRowMax*IT.java`):** These ITs cover Spanner limits (max columns, cell size, key size, key columns, name lengths, string size). Changes to type handling, hashing, PK formatting, or read/shuffle paths must keep them passing.
    *   **New Source Dialects:** When adding support for a new source database that needs an all-datatypes E2E IT (like `BulkMigrationAndValidationMySQLAllDataTypesE2EIT.java`), use the `add-integ-tests-gcs-spanner-dv` skill (`v2/gcs-spanner-dv/.agents/skills/`). It delegates to `add-source-datatype-integ-test` (`v2/spanner-common/.agents/skills/`) and requires a datatype mapping matrix file.
    *   **Assertions & Style:** Write distinct scenarios as separate `@Test` methods with descriptive names (used to prefix GCP resource names). Assert exact BigQuery output rows via `GCSSpannerDVTestAsserts.java` (`assertValidationSummary`, `assertTableValidationStats`, `assertMismatchedRecords`) — never assert only row counts.
*   **Load Tests** (`src/test/java/com/google/cloud/teleport/v2/templates/loadtesting/`):
    *   All load tests extend `GCSSpannerDVLTBase.java`. Call `setUpResourceManagers()` for an ephemeral Spanner database, or `setUpResourceManagers(spannerProjectId, spannerInstanceId, spannerDatabaseId)` (which uses `SpannerResourceManager.Builder.useStaticDatabase()`) to attach to a pre-existing static Spanner database without creating or dropping it during cleanup.
    *   **Recommendation:** For large load tests, always use pre-populated static resources — **especially for the Avro GCS input directory** — rather than generating data in-test, to avoid slow setup and test flakiness.
*   **Known Bugs, Issues & Quirks:**
    *   **Duplicate source Avro records cause false `MATCH` (`b/543222130`):** In `FunnelComparedRecordsFn.java`, when both `sourceGroup` and `spannerGroup` are non-empty for a hash, all records in `sourceGroup` are emitted as `MATCHED` (and `ComputeTableStatsFn.java` sets `destinationRowCount = matched + onlyInSpanner`). Duplicate source Avro records for an existing Spanner row are therefore counted as matched rather than flagged as mismatches.
    *   **Non-canonical JSON hashing (`b/546487364`):** `UnifiedHasherVisitor.visitJson` hashes raw JSON text without canonicalization, so key-ordering or formatting differences between source Avro and Spanner `JSON`/`PG_JSONB` produce false mismatches (MySQL/PostgreSQL all-datatypes E2E ITs currently skip non-trivial JSON for this reason).
    *   **Silent zero-row pass on filter typos:** Because `SourceReaderTransform.java` uses `EmptyMatchTreatment.ALLOW` and `ComputeTableStatsFn.java` marks `mismatch == 0` as `MATCH`, a typo in `--tables` or `gcsInputDirectory` that reads 0 files (and 0 Spanner tables) silently produces no errors.
    *   **No DLQ / Fail-Fast on Mapper Errors:** Any unchecked exception in `ComparisonRecordMapper.java` (e.g. `Table not found in DDL`, custom transformation throw) fails the entire batch job after work-item retries.
    *   **Reruns with the same `runId` mix results:** All three BigQuery writes use `WRITE_APPEND`, so rerunning with an existing `runId` appends a second set of rows under the same key.
    *   **BigQuery sinks are independent:** `MismatchedRecords`, `TableValidationStats`, and `ValidationSummary` are separate writes, and the summary is computed from table stats rather than gated on the other loads. A `ValidationSummary` row does not prove `MismatchedRecords` loaded successfully.
    *   **Primary Key Requirement:** `gcs-spanner-dv` is *only* supported for tables with deterministic primary keys. Tables without primary keys (which rely on SMT-injected synthetic UUID PKs) or sharded tables with cross-shard PK collisions (without `migration_shard_id` in the Spanner PK) will produce mismatches.
