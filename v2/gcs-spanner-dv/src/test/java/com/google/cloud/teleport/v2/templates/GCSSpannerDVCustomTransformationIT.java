package com.google.cloud.teleport.v2.templates;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.DirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.TableValidationStatsDto;
import com.google.cloud.teleport.v2.templates.GCSSpannerDVTestAsserts.ValidationSummaryDto;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests verifying the custom transformation logic of the GCSSpannerDV pipeline.
 *
 * <p>Ensures the pipeline can apply custom user transformations for dropping rows,
 * explicitly type casting columns, handling complex avro datatypes, and adding dynamically computed columns.
 */
@Category({TemplateIntegrationTest.class, DirectRunnerTest.class})
@RunWith(JUnit4.class)
@TemplateIntegrationTest(GCSSpannerDV.class)
public class GCSSpannerDVCustomTransformationIT extends GCSSpannerDVITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(GCSSpannerDVCustomTransformationIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "GCSSpannerDVCustomTransformationIT/spanner-schema.sql";

  @Before
  public void setUp() throws IOException, InterruptedException {
    LOG.info("Setting up Spanner and BigQuery resources");
    spannerResourceManager = setUpSpannerResourceManager();
    bigQueryResourceManager = setUpBigQueryResourceManager();
    bigQueryResourceManager.createDataset(REGION);
    LOG.info("BigQuery dataset created");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    LOG.info("Spanner instance created");

    // Create and upload jar for custom transformations
    createAndUploadJarToGcs("custom");
  }

  @Test
  public void testCustomTransformations() throws Exception {
    LOG.info("Generating and Uploading Avro Records to GCS");

    Instant t1 = Instant.parse("2024-01-01T10:00:00Z");
    Instant t2 = Instant.parse("2024-01-02T10:00:00Z");

    // 1 matched record, 1 record to be filtered out (age = 99)
    List<GenericRecord> usersRecords =
        Arrays.asList(
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 1L)
                .set("event_id", "E1")
                .set("full_name", "Alice")
                .set("age", 30)
                .set("created_at", t1)
                .build(), // Matched record
            new GCSSpannerDVAvroSetupHelper.RecordBuilder(
                    GCSSpannerDVAvroSetupHelper.TableDef.USERS, "shard1")
                .set("user_id", 2L)
                .set("event_id", "E2")
                .set("full_name", "Bob")
                .set("age", 99) // Will be filtered out
                .set("created_at", t2)
                .build()); // Dropped record

    String gcsInputDirectory = getGcsPath("input");
    uploadAvroFileToGcs(
        "input/users.avro", GCSSpannerDVAvroSetupHelper.TableDef.USERS.schema, usersRecords);

    // 2. Inject Spanner Records (Destination)
    LOG.info("Injecting Spanner records");

    spannerResourceManager.write(
        Arrays.asList(
            Mutation.newInsertOrUpdateBuilder("Users")
                .set("user_id")
                .to(1L)
                .set("event_id")
                .to("E1")
                .set("full_name")
                .to(ByteArray.copyFrom("AliceInWonderland"))
                .set("age")
                .to(30L)
                .set("created_at")
                .to("Time_1704106800000000") // "2024-01-01T10:00:00Z" + 1 hour in microseconds
                .set("migration_shard_id")
                .to("shard1")
                .build())); // Dropped record (user_id=2) is NOT written to Spanner.

    // Wait for Spanner's 20-second exact staleness read bound in SpannerReaderTransform
    Thread.sleep(20000);

    // 3. Launch Pipeline
    LOG.info("Launching Dataflow validation job");
    LaunchConfig.Builder options = LaunchConfig.builder(testName, specPath);
    
    CustomTransformation customTransformation =
        CustomTransformation.builder("custom/customTransformation.jar", "com.custom.CustomTransformationForDVIT")
            .setCustomParameters("InWonderland")
            .build();

    LaunchInfo jobInfo =
        launchDataflowJob(
            options,
            testName,
            PROJECT,
            spannerResourceManager,
            bigQueryResourceManager.getDatasetId(),
            gcsInputDirectory,
            "GCSSpannerDVCustomTransformationIT/session.json",
            null,
            null,
            null,
            customTransformation,
            null);

    pipelineOperator().waitUntilDone(createConfig(jobInfo));

    // 4. Assert BigQuery Validation Results
    GCSSpannerDVTestAsserts.assertValidationSummary(
        bigQueryResourceManager,
        Arrays.asList(
            new ValidationSummaryDto(
                /* status= */ "MATCH",
                /* totalTablesValidated= */ 1L,
                /* totalRowsMatched= */ 1L,
                /* totalRowsMismatched= */ 0L,
                /* tablesWithMismatches= */ "")));

    GCSSpannerDVTestAsserts.assertTableValidationStats(
        bigQueryResourceManager,
        Arrays.asList(
            new TableValidationStatsDto(
                /* schemaName= */ null,
                /* tableName= */ "Users",
                /* status= */ "MATCH",
                /* sourceRowCount= */ 1L, // Record 2 is filtered out
                /* destinationRowCount= */ 1L,
                /* matchedRowCount= */ 1L,
                /* mismatchRowCount= */ 0L)));

    GCSSpannerDVTestAsserts.assertMismatchedRecords(
        bigQueryResourceManager, Arrays.asList());
  }
}
