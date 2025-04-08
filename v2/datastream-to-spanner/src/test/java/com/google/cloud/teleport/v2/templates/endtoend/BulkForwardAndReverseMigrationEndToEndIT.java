/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates.endtoend;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.DataStreamToSpanner;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/**
 * Integration test for end-to-end Testing of all Spanner migration Flex templates for basic run
 * including new spanner tables and column rename use-case.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class BulkForwardAndReverseMigrationEndToEndIT extends EndToEndTestingITBase {
  private static final String SPANNER_DDL_RESOURCE =
      "EndToEndTesting/BulkForwardAndReverseSharded/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE = "SpannerToSourceDbCustomShardIT/session.json";
  private static final String MYSQL_SCHEMA_FILE_RESOURCE =
      "SpannerToSourceDbCustomShardIT/mysql-schema.sql";

  private static PipelineLauncher.LaunchInfo bulkJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static MySQLResourceManager jdbcResourceManagerShardA;
  private static MySQLResourceManager jdbcResourceManagerShardB;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName bulkSubscriptionName;

  private static final String TABLE = "Authors";
  private static final HashMap<String, String> AUTHOR_TABLE_COLUMNS =
      new HashMap<>() {
        {
          put("id", "INT" + " NOT NULL");
          put("name", "VARCHAR(200)");
        }
      };
  private static final HashSet<BulkForwardAndReverseMigrationEndToEndIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo rrJobInfo;
  private static PipelineLauncher.LaunchInfo fwdJobInfo;
  protected SecretManagerResourceManager secretClient;

  private static CloudSqlResourceManager cloudSqlResourceManager;

  private static final Map<String, Object> COLUMNS =
      new HashMap<>() {
        {
          put("name", RandomStringUtils.randomAlphabetic(10));
        }
      };

  private static final Integer NUM_EVENTS = 2;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (BulkForwardAndReverseMigrationEndToEndIT.class) {
      testInstances.add(this);
      if (bulkJobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(BulkForwardAndReverseMigrationEndToEndIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();
        jdbcResourceManagerShardA = MySQLResourceManager.builder(testName + "shardA").build();

        createMySQLSchema(
            jdbcResourceManagerShardA, BulkForwardAndReverseMigrationEndToEndIT.MYSQL_SCHEMA_FILE_RESOURCE);

        jdbcResourceManagerShardB = MySQLResourceManager.builder(testName + "shardB").build();

        createMySQLSchema(
            jdbcResourceManagerShardB, BulkForwardAndReverseMigrationEndToEndIT.MYSQL_SCHEMA_FILE_RESOURCE);

        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadJarToGcs(gcsResourceManager);

        createAndUploadShardConfigToGcs();
        gcsResourceManager.uploadArtifact(
            "input/session.json",
            Resources.getResource(BulkForwardAndReverseMigrationEndToEndIT.SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        bulkJobInfo =
            launchBulkDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                pubsubResourceManager,
                getClass().getSimpleName(),
                "input/customShard.jar",
                "com.custom.CustomShardIdFetcherForIT",
                null,
                null,
                MYSQL_SOURCE_TYPE);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (BulkForwardAndReverseMigrationEndToEndIT instance : testInstances) {
      instance.tearDownBase();
    }
  }

  @Test
  public void spannerToSourceDbBasic() {}
}
