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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.CASSANDRA_SOURCE_TYPE;
import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link SpannerToSourceDb} Flex template with max number of columns. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceDbMaxColumnsIT extends SpannerToSourceDbITBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToCassandraSourceDbMaxColumnsIT.class);

  private static final int NUM_COLS = 1024;
  private static final int NUM_NON_KEY_COLS = 1023;
  private static final String PRIMARY_KEY = "id";
  private static final String SECONDARY_KEY_PREFIX = "col_";
  private static final String CASSANDRA_CONFIG_FILE_RESOURCE =
      "SpannerToSourceDbWideRowIT/cassandra-config-template.conf";

  private static final String TEST_TABLE = "testtable";
  private static final String COLUMN_SIZE = "100";
  private static final HashSet<SpannerToCassandraSourceDbMaxColumnsIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static CassandraResourceManager cassandraResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;
  private final List<Throwable> assertionErrors = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    skipBaseCleanup = true;
    synchronized (SpannerToCassandraSourceDbMaxColumnsIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDBAndTableWithNColumns(TEST_TABLE, NUM_NON_KEY_COLS, COLUMN_SIZE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadCassandraConfigToGcs(
            gcsResourceManager, cassandraResourceManager, CASSANDRA_CONFIG_FILE_RESOURCE);
        createCassandraTableWithNColumns(cassandraResourceManager, TEST_TABLE, NUM_NON_KEY_COLS);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
                gcsResourceManager);
        ADDITIONAL_JOB_PARAMS.putAll(
            new HashMap<>() {
              {
                put("network", VPC_NAME);
                put("subnetwork", SUBNET_NAME);
                put("workerRegion", VPC_REGION);
              }
            });
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null,
                CASSANDRA_SOURCE_TYPE);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToCassandraSourceDbMaxColumnsIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        cassandraResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  /**
   * Retrieves the total row count of a specified table in Cassandra.
   *
   * <p>This method executes a `SELECT COUNT(*)` query on the given table and returns the number of
   * rows present in it.
   *
   * @param tableName the name of the table whose row count is to be retrieved.
   * @return the total number of rows in the specified table.
   * @throws RuntimeException if the query does not return a result.
   */
  private long getRowCount(String tableName) {
    String query = String.format("SELECT COUNT(*) FROM %s", tableName);
    ResultSet resultSet = cassandraResourceManager.executeStatement(query);
    Row row = resultSet.one();
    if (row != null) {
      return row.getLong(0);
    } else {
      throw new RuntimeException("Query did not return a result for table: " + tableName);
    }
  }

  /** Writes a row with 1,024 columns in Spanner and verifies replication to Cassandra. */
  @Test
  public void testSpannerToCassandraWithMaxColumns() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeRowWithMaxColumnsInSpanner();
    assertRowWithMaxColumnsInCassandra();
  }

  private void writeRowWithMaxColumnsInSpanner() {
    List<Mutation> mutations = new ArrayList<>();
    Mutation.WriteBuilder mutationBuilder =
        Mutation.newInsertOrUpdateBuilder(TEST_TABLE).set(PRIMARY_KEY).to("SampleTest");

    for (int i = 1; i < NUM_COLS; i++) {
      mutationBuilder.set(SECONDARY_KEY_PREFIX + i).to("TestValue_" + i);
    }

    mutations.add(mutationBuilder.build());
    spannerResourceManager.write(mutations);
    LOG.info("Inserted row with 1,024 columns into Spanner using Mutations");
  }

  private void assertRowWithMaxColumnsInCassandra() {

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(15)), () -> getRowCount(TEST_TABLE) == 1);
    assertThatResult(result).meetsConditions();

    Iterable<Row> rows;
    try {
      rows = cassandraResourceManager.readTable(TEST_TABLE);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + TEST_TABLE, e);
    }

    assertThat(rows).hasSize(1);
    for (Row row : rows) {
      LOG.info("Cassandra Row to Assert for All Data Types: {}", row.getFormattedContents());
      String primaryKeyColumn = row.getString(PRIMARY_KEY);
      assertEquals("SampleTest", primaryKeyColumn);
      for (int i = 1; i < NUM_COLS; i++) {
        assertEquals("TestValue_" + i, row.getString(SECONDARY_KEY_PREFIX + i));
      }
    }
    LOG.info("Successfully validated 1,024 columns in Cassandra");
  }
}
