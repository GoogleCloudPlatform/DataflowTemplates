# GCSSpannerDVCustomTransformationIT Test Plan

Write the integration test file `GCSSpannerDVCustomTransformationIT.java` in `v2/gcs-spanner-dv/src/test/java/com/google/cloud/teleport/v2/templates/GCSSpannerDVCustomTransformationIT.java`.

These are the cases to test:
1. Basic Data Transformation of non-pk columns (inherently tested by 2 and 3)
2. Explicit type handling (e.g., String to BYTES). (Users table column full_name)
3. Complex Avro datatype column transformation (Users table column created_at)
4. Row filtering (isEventFiltered()). (Users table column age == 99 should be filtered)
5. Passing transformationCustomParameters correctly. (customParam appended to full_name)
6. Sharded Topology Transformations (preserving migration_shard_id)

You MUST use the existing `CustomTransformationForDVIT` located at `v2/spanner-custom-shard/src/main/java/com/custom/CustomTransformationForDVIT.java`.
You MUST use `GCSSpannerDVAvroSetupHelper.TableDef.USERS` for creating test Avro records.

**Instructions for `main-writer`:**
1. Create a Spanner DDL script (e.g., `GCSSpannerDVCustomTransformationIT/spanner-schema.sql`) for the `Users` table. Keep in mind that `full_name` will be written as `BYTES(MAX)` and `created_at` as `STRING(MAX)` due to the transformation. Add `migration_shard_id STRING(MAX)` to test sharding.
2. In the Java test file:
    - Extend `GCSSpannerDVITBase`.
    - Use `createAndUploadJarToGcs` to build and upload the transformation JAR.
    - Write a single test method `testCustomTransformations()` that:
        - Creates test records using `GCSSpannerDVAvroSetupHelper.RecordBuilder`. Include a record with `age = 99` (to be filtered out), and records with regular data.
        - Writes these Avro records to GCS using `writeAvroRecords`.
        - Writes the expected corresponding rows to Spanner using `SpannerResourceManager` (or Spanner's Mutation API), so that Data Validator can match them. For the dropped record (age=99), do NOT write to Spanner. Note: For the Spanner records, `full_name` needs to include the custom param and be converted to ByteArray/Base64/bytes depending on how Spanner API expects it.
        - Launches the `gcs-spanner-dv` Dataflow job with custom transformation options (e.g. `transformationJarPath`, `transformationClassName`, `transformationCustomParameters`).
        - Asserts the Data Validator results to ensure 100% match. (E.g. checking Spanner output table for validation results if `EndToEndTestingITBase` / `GCSSpannerDVITBase` provides it, or `jobInfo.isDone()`).

Review `GCSSpannerDVCoreMatchingIT.java` for how `gcs-spanner-dv` tests are usually structured.

Good luck!
