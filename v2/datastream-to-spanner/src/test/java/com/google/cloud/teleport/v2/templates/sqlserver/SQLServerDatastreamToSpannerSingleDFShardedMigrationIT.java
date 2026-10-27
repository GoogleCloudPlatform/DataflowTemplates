/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.sqlserver;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.cloudsql.CloudSqlServerResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat;
import org.apache.beam.it.gcp.datastream.SqlServerSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for a sharded SQL Server migration using a single Dataflow job for the whole
 * migration with the {@link DataStreamToSpanner} Flex template, driven by REAL Datastream streams.
 *
 * <p>Two "physical shards" are simulated with two Datastream streams reading from the same SQL
 * Server database; each stream covers two SQL Server schemas ({@code S1L1}/{@code S1L2} and {@code
 * S2L1}/{@code S2L2}) which play the role of the logical shards. Datastream sets {@code
 * _metadata_schema} to the SQL Server schema name, which the template combines with {@code
 * _metadata_stream} and the sharding context ({@code sourceConfigURL}) to derive the {@code
 * migration_shard_id} written to Spanner. Both streams share the same GCS destination path, so a
 * single Pub/Sub notification set and a single Dataflow job serve the whole migration.
 *
 * <p>Deviations from the MySQL reference ({@code DatastreamToSpannerSingleDFShardedMigrationIT}):
 *
 * <ul>
 *   <li>Logical shards are SQL Server schemas inside one database (not separate databases), since
 *       Datastream for SQL Server reports the schema, not the database, in {@code
 *       _metadata_schema}.
 *   <li>The Datastream resource manager prefixes stream ids with the test id and a random suffix,
 *       so the sharding config cannot be a static file. {@code sharding-config.conf} is a template
 *       whose {@code STREAM_A_ID}/{@code STREAM_B_ID} placeholders are replaced with the ids of the
 *       streams created at runtime and uploaded with {@code createArtifact}; {@code
 *       sourceConfigURL} is passed explicitly as a job parameter.
 *   <li>Because the streams are created by the test and no {@code SqlServerSource} is handed to the
 *       base launcher, {@code datastreamSourceType=sqlserver} and {@code streamName} are set
 *       explicitly in the job parameters.
 *   <li>The avro backfill / CDC uploads are replaced by equivalent DML on the source: backfill rows
 *       are inserted before launch and the CDC wave (update / delete / insert) is applied to shard
 *       {@code L1} once the backfill has landed in Spanner.
 * </ul>
 *
 * <p>TODO: {@code ChangeEventSessionConvertor#populateShardId} currently only populates the shard
 * id column for {@code mysql} and {@code oracle} sources; until it also handles {@code sqlserver},
 * {@code migration_shard_id} will be written as an empty string and the final assertion of this
 * test fails. The test is {@link Ignore}d until that pipeline fix lands.
 */
@Ignore(
    "migration_shard_id is not populated for sqlserver sources yet (ChangeEventSessionConvertor)")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SQLServerDatastreamToSpannerSingleDFShardedMigrationIT
    extends DataStreamToSpannerITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SQLServerDatastreamToSpannerSingleDFShardedMigrationIT.class);

  private static final String RESOURCE_DIR =
      "sqlserver/SQLServerDatastreamToSpannerSingleDFShardedMigrationIT/";
  private static final String SQLSERVER_DDL_RESOURCE = RESOURCE_DIR + "sqlserver-schema.sql";
  private static final String SPANNER_DDL_RESOURCE = RESOURCE_DIR + "spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = RESOURCE_DIR + "sqlserver-session.json";
  private static final String SHARDING_CONTEXT_RESOURCE = RESOURCE_DIR + "sharding-config.conf";

  private static final String GCS_PATH_PREFIX = "sqlserver-single-df-sharded";
  private static final String TABLE = "Users";

  // Logical shards: SQL Server schemas. Stream A covers physical shard 1, stream B shard 2.
  private static final String SCHEMA_S1L1 = "S1L1";
  private static final String SCHEMA_S1L2 = "S1L2";
  private static final String SCHEMA_S2L1 = "S2L1";
  private static final String SCHEMA_S2L2 = "S2L2";
  private static final String STREAM_A_PLACEHOLDER = "STREAM_A_ID";
  private static final String STREAM_B_PLACEHOLDER = "STREAM_B_ID";

  private static final Set<SQLServerDatastreamToSpannerSingleDFShardedMigrationIT> testInstances =
      ConcurrentHashMap.newKeySet();
  private static boolean initialized = false;
  private static PipelineLauncher.LaunchInfo jobInfo;

  private static CloudSqlServerResourceManager msSqlResourceManager;
  private static SpannerResourceManager spannerResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static DatastreamResourceManager datastreamResourceManager;

  /**
   * Set up the resource managers, the source schemas and data, the two Datastream streams and
   * launch the single Dataflow job once for this test class.
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (SQLServerDatastreamToSpannerSingleDFShardedMigrationIT.class) {
      testInstances.add(this);
      if (!initialized) {
        LOG.info("Setting up SQL Server test container resource manager...");
        msSqlResourceManager = setUpSqlServerResourceManager();
        LOG.info("SQL Server resource manager created with URI: {}", msSqlResourceManager.getUri());
        LOG.info("Setting up Spanner resource manager...");
        spannerResourceManager = setUpSpannerResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        LOG.info(
            "Spanner resource manager created with instance ID: {}",
            spannerResourceManager.getInstanceId());
        LOG.info("Setting up Pub/Sub resource manager...");
        pubsubResourceManager = setUpPubSubResourceManager();
        LOG.info("Setting up GCS resource manager...");
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        LOG.info("GCS resource manager created with bucket: {}", gcsResourceManager.getBucket());
        LOG.info("Setting up Datastream resource manager...");
        datastreamResourceManager = setUpDatastreamResourceManager();

        LOG.info("Executing SQL Server DDL script...");
        executeSqlScript(msSqlResourceManager, SQLSERVER_DDL_RESOURCE);
        msSqlResourceManager.enableCdcOnAllTables();

        LOG.info("Inserting backfill rows into all four logical shards...");
        insertBackfillRows();

        // Both streams write into the same GCS prefix that the Dataflow job watches
        // (mirrors createDataStream in DataStreamToSpannerITBase).
        String gcsPrefix =
            getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), "");
        LOG.info("Creating Datastream stream A for schemas {} and {}...", SCHEMA_S1L1, SCHEMA_S1L2);
        Stream streamA = createShardStream("shard-a", gcsPrefix, List.of(SCHEMA_S1L1, SCHEMA_S1L2));
        LOG.info("Creating Datastream stream B for schemas {} and {}...", SCHEMA_S2L1, SCHEMA_S2L2);
        Stream streamB = createShardStream("shard-b", gcsPrefix, List.of(SCHEMA_S2L1, SCHEMA_S2L2));

        String streamAId = streamId(streamA);
        String streamBId = streamId(streamB);
        LOG.info("Created streams with ids {} and {}", streamAId, streamBId);

        // The sharding config must reference the runtime stream ids, so it is rendered from the
        // resource template and uploaded explicitly instead of via shardingContextFileResourceName.
        String shardingConfig =
            Resources.toString(
                    Resources.getResource(SHARDING_CONTEXT_RESOURCE), StandardCharsets.UTF_8)
                .replace(STREAM_A_PLACEHOLDER, streamAId)
                .replace(STREAM_B_PLACEHOLDER, streamBId);
        String shardingConfigArtifact = GCS_PATH_PREFIX + "/shardingConfig.conf";
        gcsResourceManager.createArtifact(shardingConfigArtifact, shardingConfig);
        LOG.info("Uploaded sharding config: {}", shardingConfig);

        String sessionFileContent =
            generateSessionFile(
                1,
                SCHEMA_S1L1,
                spannerResourceManager.getDatabaseId(),
                List.of(TABLE),
                SESSION_FILE_RESOURCE);

        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put("streamName", streamA.getName());
        jobParameters.put("datastreamSourceType", "sqlserver");
        jobParameters.put(
            "sourceConfigURL", getGcsPath(shardingConfigArtifact, gcsResourceManager));

        LOG.info("Launching Dataflow job...");
        jobInfo =
            launchDataflowJob(
                "sqlserver-single-df-sharded",
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                jobParameters,
                null,
                null,
                gcsResourceManager,
                null,
                sessionFileContent,
                null);
        initialized = true;
      }
    }
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @AfterClass
  public static void cleanUp() throws IOException {
    LOG.info("Cleaning up resources...");
    // A failing teardown of one instance must not prevent the other instances' Dataflow jobs and
    // the shared resource managers below from being cleaned up.
    for (SQLServerDatastreamToSpannerSingleDFShardedMigrationIT instance : testInstances) {
      try {
        instance.tearDownBase();
      } catch (Exception e) {
        LOG.error("Failed to tear down test instance {}", instance.testName, e);
      }
    }

    // Datastream streams must be deleted before the CDC-enabled source database is dropped.
    ResourceManagerUtils.cleanResources(datastreamResourceManager);

    ResourceManagerUtils.cleanResources(
        msSqlResourceManager, spannerResourceManager, gcsResourceManager, pubsubResourceManager);
  }

  @Test
  public void multiShardMigration() {
    // A single Dataflow job serves two physical shards (two Datastream streams), each containing
    // two logical shards (SQL Server schemas). Migrates the Users table from the 4 logical shards
    // and verifies that data from all shards is correctly migrated to Spanner. Additionally, checks
    // whether the migration shard ID column is populated accurately based on the sharding context.
    assertThatPipeline(jobInfo).isRunning();

    // Backfill: 3 rows per logical shard.
    ConditionCheck backfillCheck =
        SpannerRowsCheck.builder(spannerResourceManager, TABLE)
            .setMinRows(12)
            .setMaxRows(12)
            .build();
    LOG.info("Waiting for backfill rows to arrive in Spanner...");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(JOB_START_PROCESSING_WAIT_MINUTES)),
                backfillCheck);
    assertThatResult(result).meetsConditions();

    // CDC wave on logical shard L1 (schema S1L1): update, delete and insert.
    LOG.info("Applying CDC wave on schema {}...", SCHEMA_S1L1);
    msSqlResourceManager.runSQLUpdate(
        String.format("UPDATE %s.%s SET age = 20 WHERE id = 1", SCHEMA_S1L1, TABLE));
    msSqlResourceManager.runSQLUpdate(
        String.format("DELETE FROM %s.%s WHERE id = 2", SCHEMA_S1L1, TABLE));
    msSqlResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (13, 'Tester13', 113)", SCHEMA_S1L1, TABLE));

    // The row count stays at 12 after the CDC wave (one delete, one insert), so wait for the
    // inserted row to be visible instead of relying on the count alone.
    ConditionCheck cdcCheck =
        SpannerRowsCheck.builder(spannerResourceManager, TABLE)
            .setMinRows(12)
            .setMaxRows(12)
            .build()
            .and(rowExistsCheck(13, "L1"));
    LOG.info("Waiting for CDC rows to arrive in Spanner...");
    result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(10)), cdcCheck);
    assertThatResult(result).meetsConditions();

    // Sleep for cutover time to wait till all CDCs propagate.
    // A real world customer also has a small cut over time to reach consistency.
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }
    // Assert specific rows
    assertUsersTableContents();
  }

  private void insertBackfillRows() {
    insertUser(SCHEMA_S1L1, 1, "Tester1", 101);
    insertUser(SCHEMA_S1L1, 2, "Tester2", 102);
    insertUser(SCHEMA_S1L1, 3, "Tester3", 103);
    insertUser(SCHEMA_S1L2, 4, "Tester4", 104);
    insertUser(SCHEMA_S1L2, 5, "Tester5", 105);
    insertUser(SCHEMA_S1L2, 6, "Tester6", 106);
    insertUser(SCHEMA_S2L1, 7, "Tester7", 107);
    insertUser(SCHEMA_S2L1, 8, "Tester8", 108);
    insertUser(SCHEMA_S2L1, 9, "Tester9", 109);
    insertUser(SCHEMA_S2L2, 10, "Tester10", 110);
    insertUser(SCHEMA_S2L2, 11, "Tester11", 111);
    insertUser(SCHEMA_S2L2, 12, "Tester12", 112);
  }

  private void insertUser(String schema, int id, String name, int age) {
    msSqlResourceManager.runSQLUpdate(
        String.format(
            "INSERT INTO %s.%s (id, name, age) VALUES (%d, '%s', %d)",
            schema, TABLE, id, name, age));
  }

  /**
   * Creates and starts a Datastream stream for the {@code Users} table of the given SQL Server
   * schemas, writing avro files to the shared {@code gcsPrefix}.
   */
  private Stream createShardStream(String streamIdSuffix, String gcsPrefix, List<String> schemas) {
    Map<String, List<String>> allowedTables = new HashMap<>();
    for (String schema : schemas) {
      allowedTables.put(schema, List.of(TABLE));
    }
    SqlServerSource source =
        SqlServerSource.builder(
                msSqlResourceManager.getHost(),
                msSqlResourceManager.getUsername(),
                msSqlResourceManager.getPassword(),
                msSqlResourceManager.getPort(),
                msSqlResourceManager.getDatabaseName())
            .setAllowedTables(allowedTables)
            .build();
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile-" + streamIdSuffix, source);
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile-" + streamIdSuffix,
            gcsResourceManager.getBucket(),
            gcsPrefix,
            DestinationOutputFormat.AVRO_FILE_FORMAT);
    Stream stream =
        datastreamResourceManager.createStream(streamIdSuffix, sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);
    return stream;
  }

  /**
   * Returns the stream id (last path segment of the fully qualified stream name), which is what the
   * template matches against {@code streamId} in the sharding context.
   */
  private static String streamId(Stream stream) {
    String name = stream.getName();
    return name.substring(name.lastIndexOf('/') + 1);
  }

  private ConditionCheck rowExistsCheck(long id, String shardId) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return String.format(
            "Spanner row with id=%d and migration_shard_id=%s exists in %s", id, shardId, TABLE);
      }

      @Override
      protected CheckResult check() {
        List<Struct> rows =
            spannerResourceManager.runQuery(
                String.format(
                    "SELECT id FROM %s WHERE id = %d AND migration_shard_id = '%s'",
                    TABLE, id, shardId));
        boolean found = !rows.isEmpty();
        return new CheckResult(
            found,
            found
                ? String.format("Row id=%d found with shard %s", id, shardId)
                : String.format("Row id=%d with shard %s not found yet", id, shardId));
      }
    };
  }

  private void assertUsersTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    events.add(expectedRow(1, "Tester1", 20, "L1"));
    events.add(expectedRow(3, "Tester3", 103, "L1"));
    events.add(expectedRow(13, "Tester13", 113, "L1"));

    events.add(expectedRow(4, "Tester4", 104, "L2"));
    events.add(expectedRow(5, "Tester5", 105, "L2"));
    events.add(expectedRow(6, "Tester6", 106, "L2"));

    events.add(expectedRow(7, "Tester7", 107, "L3"));
    events.add(expectedRow(8, "Tester8", 108, "L3"));
    events.add(expectedRow(9, "Tester9", 109, "L3"));

    events.add(expectedRow(10, "Tester10", 110, "L4"));
    events.add(expectedRow(11, "Tester11", 111, "L4"));
    events.add(expectedRow(12, "Tester12", 112, "L4"));

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }

  private static Map<String, Object> expectedRow(
      int id, String name, int ageSpanner, String migrationShardId) {
    Map<String, Object> row = new HashMap<>();
    row.put("id", id);
    row.put("name", name);
    row.put("age_spanner", ageSpanner);
    row.put("migration_shard_id", migrationShardId);
    return row;
  }
}
