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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.cloud.teleport.v2.templates.DataStreamToSpannerITBase;
import com.google.common.io.Resources;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for sharded data migration using a single Dataflow job for Oracle Source
 * targeting Spanner.
 *
 * <p>Like the MySQL version, it migrates the Users table from 2 physical shards with 2 logical
 * shards each. In Oracle, a logical shard is a schema (the dbName of a shard config is the schema
 * name) and a physical shard is a Datastream stream: each of the 2 streams replicates the Users
 * tables of 2 isolated Oracle users (schemas) of the shared Oracle container.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDatastreamToSpannerSingleDFShardedMigrationIT extends DataStreamToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleDatastreamToSpannerSingleDFShardedMigrationIT.class);

  private static final String TABLE = "Users";

  private static final String SESSION_FILE_RESOURCE =
      "oracle/OracleDatastreamToSpannerSingleDFShardedMigrationIT/oracle-session.json";

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDatastreamToSpannerSingleDFShardedMigrationIT/oracle-google_standard_sql-spanner-schema.sql";

  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/OracleDatastreamToSpannerSingleDFShardedMigrationIT/oracle-schema.sql";

  /** The logical shard IDs; logical shards L1, L2 are in stream 1 and L3, L4 in stream 2. */
  private static final List<String> LOGICAL_SHARDS = List.of("L1", "L2", "L3", "L4");

  private static HashSet<OracleDatastreamToSpannerSingleDFShardedMigrationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static final String GCS_PATH_PREFIX = "oracle-datastream-to-spanner-sharded-migration";

  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;

  /** Isolated Oracle user (schema) of each logical shard. */
  private static final Map<String, String> oracleUsers = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (OracleDatastreamToSpannerSingleDFShardedMigrationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        try {
          setUpSharedResources();
        } catch (Exception e) {
          LOG.error("Setup failed, cleaning up the resources created so far.", e);
          try {
            cleanUpSharedResources(List.of(this));
          } catch (RuntimeException cleanupError) {
            e.addSuppressed(cleanupError);
          }
          throw e;
        }
      }
    }
  }

  /**
   * Creates the shared resources, the Oracle schemas (logical shards) and the Datastream streams
   * (physical shards), and launches the Dataflow job (sets {@link #jobInfo}).
   */
  private void setUpSharedResources() throws Exception {
    spannerResourceManager = setUpSpannerResourceManager();
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    pubsubResourceManager = setUpPubSubResourceManager();
    gcsResourceManager = setUpSpannerITGcsResourceManager();

    for (String logicalShard : LOGICAL_SHARDS) {
      String oracleUser = SharedOracleLiveITContainer.setupOracleIsolatedUser();
      oracleUsers.put(logicalShard, oracleUser);
      SharedOracleLiveITContainer.executeSqlScript(oracleUser, ORACLE_SCHEMA_FILE_RESOURCE);
    }

    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-connect-2")
            .build();

    String gcsPrefix =
        getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket() + "/", "");

    String stream1 = createStream("shard1", "L1", "L2", gcsPrefix);
    String stream2 = createStream("shard2", "L3", "L4", gcsPrefix);

    // For Oracle, Datastream populates _metadata_schema with the schema (owner) of the table,
    // i.e. the isolated Oracle user, not the database/service name.
    String shardConfig =
        generateSourceConfig(Map.of("L1", stream1, "L2", stream1, "L3", stream2, "L4", stream2));

    gcsResourceManager.createArtifact(
        "input/shardingConfig.conf", shardConfig.getBytes(StandardCharsets.UTF_8));

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("inputFileFormat", "avro");
    jobParams.put("inputFilePattern", getGcsPath(GCS_PATH_PREFIX + "/cdc/", gcsResourceManager));
    jobParams.put("datastreamSourceType", "oracle");
    jobParams.put("workerMachineType", "n1-standard-4");
    jobParams.put("sourceConfigURL", getGcsPath("input/shardingConfig.conf", gcsResourceManager));

    String sessionFileContent =
        Resources.toString(Resources.getResource(SESSION_FILE_RESOURCE), StandardCharsets.UTF_8)
            .replace("it_test", oracleUsers.get("L1"));

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName() + "shard1",
            null,
            null,
            GCS_PATH_PREFIX,
            spannerResourceManager,
            null,
            jobParams,
            null,
            null,
            gcsResourceManager,
            datastreamResourceManager,
            sessionFileContent,
            // Streams are already created above. Passing a JDBC source here would create another
            // stream and set the "streamName" pipeline option, which overrides the stream name of
            // every record and breaks the sharding config lookup.
            null);
  }

  /**
   * Creates and starts a Datastream stream (physical shard) replicating the Users tables of the
   * schemas of the given 2 logical shards, connecting as the Oracle user of the first one. The
   * isolated users have the DBA role, so they can read the tables of the other schema too.
   *
   * @return the ID of the stream.
   */
  private String createStream(
      String physicalShard, String logicalShard1, String logicalShard2, String gcsPrefix) {
    OracleSource oracleSource =
        SharedOracleLiveITContainer.oracleSourceBuilder(oracleUsers.get(logicalShard1))
            .setAllowedTables(
                Map.of(
                    oracleUsers.get(logicalShard1), List.of(TABLE),
                    oracleUsers.get(logicalShard2), List.of(TABLE)))
            .build();
    SourceConfig sourceConfig =
        buildJdbcSourceConfig(
            datastreamResourceManager, "jdbc-profile-" + physicalShard, oracleSource);
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile-" + physicalShard,
            gcsResourceManager.getBucket(),
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);
    Stream stream =
        datastreamResourceManager.createStream(
            "stream-" + physicalShard, sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);
    return stream.getName().substring(stream.getName().lastIndexOf('/') + 1);
  }

  @AfterClass
  public static void cleanUp() {
    cleanUpSharedResources(testInstances);
  }

  /**
   * Cleans up the Dataflow jobs of the given test instances, the shared resource managers and the
   * isolated Oracle users, then resets them so that a later setup (after a failed one) starts
   * clean.
   */
  private static void cleanUpSharedResources(
      Collection<OracleDatastreamToSpannerSingleDFShardedMigrationIT> instances) {
    try {
      SharedOracleLiveITContainer.cleanUpTestResources(
          instances,
          new ArrayList<>(oracleUsers.values()),
          spannerResourceManager,
          pubsubResourceManager,
          gcsResourceManager,
          datastreamResourceManager);
    } finally {
      spannerResourceManager = null;
      pubsubResourceManager = null;
      gcsResourceManager = null;
      datastreamResourceManager = null;
      oracleUsers.clear();
    }
  }

  @Test
  public void multiShardMigration() throws Exception {
    // A single Dataflow job runs for two physical shards (streams), each containing two logical
    // shards (schemas). Migrates the Users table from the 4 logical shards, with inserts, updates
    // and deletes, and verifies that the data from all the shards is correctly migrated to Spanner
    // and that the migration shard ID column is populated according to the sharding config.

    // Check if pipeline is running
    assertThatPipeline(jobInfo).isRunning();

    insertDataInOracle();

    // Wait for the final contents, including the update and the delete in logical shard L1.
    ConditionCheck usersCheck =
        new ConditionCheck() {
          @Override
          protected String getDescription() {
            return "Check Users table contents";
          }

          @Override
          protected CheckResult check() {
            try {
              assertUsersTableContents();
              return new CheckResult(true, "Users table contains the expected rows");
            } catch (AssertionError e) {
              return new CheckResult(false, "Users table does not contain the expected rows yet");
            }
          }
        };

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(15)), usersCheck);

    assertThatResult(result).meetsConditions();

    // Sleep for cutover time to wait till all CDCs propagate.
    try {
      Thread.sleep(CUTOVER_MILLIS);
    } catch (InterruptedException e) {
    }

    // Assert specific rows
    assertUsersTableContents();
  }

  private void insertDataInOracle() throws Exception {
    LOG.info("Inserting rows into the Users tables of the logical shards in Oracle");
    // Logical shard L1, with an update and a delete.
    insertUsers("L1", 1, 2, 3, 13);
    SharedOracleLiveITContainer.executeSql(
        oracleUsers.get("L1"),
        "UPDATE \"Users\" SET \"age\" = 20 WHERE \"id\" = 1;"
            + " DELETE FROM \"Users\" WHERE \"id\" = 2");
    insertUsers("L2", 4, 5, 6);
    insertUsers("L3", 7, 8, 9);
    insertUsers("L4", 10, 11, 12);
    SharedOracleLiveITContainer.flushRedoLogs();
  }

  /** Inserts the users with the given IDs (named TesterID, aged 100 + ID) in a logical shard. */
  private void insertUsers(String logicalShard, int... ids) throws Exception {
    List<String> inserts = new ArrayList<>();
    for (int id : ids) {
      inserts.add(
          String.format(
              "INSERT INTO \"Users\" (\"id\", \"name\", \"age\") VALUES (%d, 'Tester%d', %d)",
              id, id, 100 + id));
    }
    SharedOracleLiveITContainer.executeSql(
        oracleUsers.get(logicalShard), String.join(";", inserts));
  }

  private void assertUsersTableContents() {
    Map<String, List<Integer>> idsPerShard =
        Map.of(
            "L1", List.of(1, 3, 13),
            "L2", List.of(4, 5, 6),
            "L3", List.of(7, 8, 9),
            "L4", List.of(10, 11, 12));
    List<Map<String, Object>> events = new ArrayList<>();
    for (Map.Entry<String, List<Integer>> shard : idsPerShard.entrySet()) {
      for (int id : shard.getValue()) {
        Map<String, Object> row = new HashMap<>();
        row.put("id", id);
        row.put("name", "Tester" + id);
        // The age of user 1 was updated.
        row.put("age", id == 1 ? 20 : 100 + id);
        row.put("migration_shard_id", shard.getKey());
        events.add(row);
      }
    }

    SpannerAsserts.assertThatStructs(spannerResourceManager.runQuery("select * from Users"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
    // The row of the deleted user 2 must be gone too.
    assertEquals(events.size(), (long) spannerResourceManager.getRowCount(TABLE));
  }

  /**
   * Generates the sharding config mapping each logical shard (i.e. the schema of its Oracle user)
   * to its stream.
   *
   * @param streamPerLogicalShard the ID of the stream of each logical shard.
   */
  private String generateSourceConfig(Map<String, String> streamPerLogicalShard) {
    List<String> shardConfigs = new ArrayList<>();
    for (String logicalShard : LOGICAL_SHARDS) {
      shardConfigs.add(
          String.format(
              "    {\n"
                  + "      \"logicalShardId\": \"%s\",\n"
                  + "      \"dbName\": \"%s\",\n"
                  + "      \"streamId\": \"%s\"\n"
                  + "    }",
              logicalShard,
              oracleUsers.get(logicalShard),
              streamPerLogicalShard.get(logicalShard)));
    }
    return "{\n  \"shardConfigs\": [\n" + String.join(",\n", shardConfigs) + "\n  ]\n}";
  }
}
