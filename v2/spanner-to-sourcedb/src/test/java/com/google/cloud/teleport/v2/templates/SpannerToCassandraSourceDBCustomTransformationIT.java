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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceDBCustomTransformationIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToCassandraSourceDBCustomTransformationIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToCassandraSourceIT/spanner-schema.sql";
  private static final String CASSANDRA_SCHEMA_FILE_RESOURCE =
      "SpannerToCassandraSourceIT/cassandra-schema.sql";
  private static final String CASSANDRA_CONFIG_FILE_RESOURCE =
      "SpannerToCassandraSourceIT/cassandra-config-template.conf";

  private static final String USER_TABLE = "Users";
  private static final HashSet<SpannerToCassandraSourceDBCustomTransformationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static CassandraResourceManager cassandraResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;
  private final List<Throwable> assertionErrors = new ArrayList<>();

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToCassandraSourceDBCustomTransformationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadCassandraConfigToGcs(
            gcsResourceManager, cassandraResourceManager, CASSANDRA_CONFIG_FILE_RESOURCE);
        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""));
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

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToCassandraSourceDBCustomTransformationIT instance : testInstances) {
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
   * Tests the data flow from Spanner to Cassandra.
   *
   * <p>This test ensures that a basic row is successfully written to Spanner and subsequently
   * appears in Cassandra, validating end-to-end data consistency.
   *
   * @throws InterruptedException if the thread is interrupted during execution.
   * @throws IOException if an I/O error occurs during the test execution.
   */
  @Test
  public void spannerToCasandraSourceDbBasic() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeBasicRowInSpanner();
    assertBasicRowInCassandraDB();
  }

  /**
   * Writes basic rows to multiple tables in Google Cloud Spanner.
   *
   * <p>This method performs the following operations:
   *
   * <ul>
   *   <li>Inserts or updates a row in the "users" table with an ID of 1.
   *   <li>Inserts or updates a row in the "users2" table with an ID of 2.
   *   <li>Executes a transactionally buffered insert/update operation in the "users" table with an
   *       ID of 3, using a transaction tag for tracking.
   * </ul>
   *
   * The transaction uses a Spanner client with a specific transaction tag
   * ("txBy=forwardMigration").
   */
  private void writeBasicRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(USER_TABLE).set("id").to(1).set("from").to("B").build();
    spannerResourceManager.write(m1);
  }

  /**
   * Asserts that a basic row exists in the Cassandra database.
   *
   * <p>This method performs the following steps:
   *
   * <ul>
   *   <li>Waits for the condition that ensures one row exists in the Cassandra table {@code
   *       USER_TABLE}.
   *   <li>Retrieves and logs rows from the Cassandra table.
   *   <li>Checks if exactly one row is present in the table.
   *   <li>Verifies that the row contains expected values for columns: {@code id}, {@code
   *       full_name}, and {@code from}.
   * </ul>
   *
   * @throws InterruptedException if the thread is interrupted while waiting for the row count
   *     condition.
   * @throws RuntimeException if reading from the Cassandra table fails.
   */
  private void assertBasicRowInCassandraDB() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount(USER_TABLE) == 2);
    assertThatResult(result).meetsConditions();

    Iterable<Row> rows;
    try {
      LOG.info("Reading from Cassandra table: {}", USER_TABLE);
      rows = cassandraResourceManager.readTable(USER_TABLE);
      LOG.info("Cassandra Rows: {}", rows.toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + USER_TABLE, e);
    }

    assertThat(rows).hasSize(2);

    for (Row row : rows) {
      LOG.info("Cassandra Row to Assert: {}", row.getFormattedContents());
      int id = row.getInt("id");
      if (id == 1) {
        assertThat(row.getString("full_name")).isEqualTo("A");
        assertThat(row.getString("from")).isEqualTo("B");
      } else if (id == 2) {
        assertThat(row.getString("full_name")).isEqualTo("BB");
      } else {
        throw new AssertionError("Unexpected row ID found: " + id);
      }
    }
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
}
