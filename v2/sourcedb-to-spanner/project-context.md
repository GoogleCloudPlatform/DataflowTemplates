# Project Context: SourceDb to Spanner

<!-- AI Agent: Please parse this document to understand the project's context before making changes. -->

## Overview

*   **Core Intent:** A bulk migration Dataflow pipeline to migrate data from various Source Databases (MySQL, PostgreSQL, Cassandra) into Cloud Spanner. It handles sharded and non-sharded databases.
*   **Primary Users:** Internal SREs, external customers migrating to Cloud Spanner, and users of Spanner Migration Tool.
*   **Critical SLOs/Guarantees:** Must effectively handle bulk data extraction and mapping to Cloud Spanner mutations while maintaining data integrity. Features a Dead Letter Queue (DLQ) for failed mutations.
*   **Terminology:** 
    *   **DLQ:** Dead Letter Queue (for failed records).
    *   **SourceRow:** Intermediate representation of a row read from the source database.
    *   **Mutation:** Spanner mutation to be applied.

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
    ```bash
    # To build the flex template
    export PROJECT_ID="<your-project-id>"
    export BUCKET_NAME="<your-bucket-name>"
    mvn clean package -PtemplatesStage -DskipTests -DprojectId="$PROJECT_ID" -DbucketName="$BUCKET_NAME" -DstagePrefix="templates-<replace-with-your-prefix>" -DtemplateName="Sourcedb_to_Spanner_Flex" -pl v2/sourcedb-to-spanner -am

    # To run tests
    mvn clean test -pl v2/sourcedb-to-spanner -am

    # To run pipeline
    export JOB_NAME="bulk-migrate-to-spanner-$(date +%Y%m%d-%H%M%S)"
    export OUTPUT_DIR="gs://${BUCKET_NAME}/bulk-migration"
    gcloud dataflow flex-template run $JOB_NAME \
      --project=$PROJECT_ID \
      --region=$REGION \
      --template-file-gcs-location="gs://dataflow-templates-${REGION}/latest/flex/Sourcedb_to_Spanner_Flex" \
      --max-workers=2 \
      --num-workers=1 \
      --worker-machine-type=n2-highmem-8 \
      --parameters sourceConfigURL=$GCS_SHARDING_PATH,instanceId=$SPANNER_INSTANCE_NAME,databaseId=$SPANNER_DATABASE_NAME,projectId=$PROJECT_ID,outputDirectory=$OUTPUT_DIR,username=datastream_user,password=complex_password_123,schemaOverridesFilePath=$GCS_OVERRIDES_PATH,transformationJarPath=$CUSTOM_JAR_PATH,transformationClassName=com.custom.CustomTransformationFetcher
    ```

## Project Management

*   **Buganizer Component:** [Infrastructure > Spanner > Cloud > Migrations](https://b.corp.google.com/issues?q=componentid:1008064) - (Cloud Spanner migrations component)
*   **Key Contacts:**
    *   **Recent Contributors:** darshan-sj, aasthabharill, shreyakhajanchi, sm745052

## Documentation

*   **Key Design Docs:**
    *   [Bulk Migration to Spanner Design](http://go/bulk-migration-to-spanner-design) - Overall pipeline design.
    *   [CS Reader for Bulk Migration](http://go/cs-reader-for-bulk-migration-to-spanner) - Reader design.
    *   [Spanner Bulk Migration User Guide](http://go/spanner-bulk-migration-user-guide) - Usage instructions.
*   **Architecture Diagram:** [architecture.svg](architecture.svg)

## AI Agent Tips

*   **Common Tasks:** Adding new JDBC dialects, fixing parsing errors, implementing new transformations or schema overrides, adding new source reader capabilities.
*   **Coding Standards & Best Practices:**
    *   Use `AutoValue` for POJOs.
    *   Strict adherence to Apache Beam paradigms (PTransforms, DoFns). Use `TupleTag` for side outputs like the DLQ.
    *   Use structured logging (`com.google.cloud.teleport.structured-logging`).
*   **Testing Frameworks & Guidelines:**
    *   **Frameworks:** JUnit 4, Google Truth for assertions, Mockito for mocking.
    *   **Rules:** Ensure tests use `@RunWith(JUnit4.class)`. Use embedded databases for testing when possible (e.g. `derby` or `embedded-cassandra`).
*   **Areas to be Careful:** Cross-shard querying logic, causal ordering around the DLQ, and schema mappings parsing.
*   **Example CLs:**
    *   [39a8ae5e0](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/39a8ae5e0) - Fix GCS Avro Export flow
    *   [90964dca6](https://github.com/GoogleCloudPlatform/DataflowTemplates/commit/90964dca6) - Add Support for UUID-based Partitioning
