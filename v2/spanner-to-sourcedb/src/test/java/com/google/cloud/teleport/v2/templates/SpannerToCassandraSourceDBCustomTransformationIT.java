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
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
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

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToCassandraSourceDBCustomTransformationIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToCassandraSourceDBCustomTransformationIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToCassandraSourceIT/spanner-transformation-schema.sql";
  private static final String CASSANDRA_SCHEMA_FILE_RESOURCE =
      "SpannerToCassandraSourceIT/cassandra-transformation-schema.sql";
  private static final String CASSANDRA_CONFIG_FILE_RESOURCE =
      "SpannerToCassandraSourceIT/cassandra-config-template.conf";

  private static final String CUSTOMER_TABLE = "Customers";
  private static final HashSet<SpannerToCassandraSourceDBCustomTransformationIT> testInstances =
      new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  public static CassandraResourceManager cassandraResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    skipBaseCleanup = true;
    synchronized (SpannerToCassandraSourceDBCustomTransformationIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createAndUploadCassandraConfigToGcs(
            gcsResourceManager, cassandraResourceManager, CASSANDRA_CONFIG_FILE_RESOURCE);
        createCassandraSchema(cassandraResourceManager, CASSANDRA_SCHEMA_FILE_RESOURCE);
        pubsubResourceManager = setUpPubSubResourceManager();
        CustomTransformation customTransformation =
            CustomTransformation.builder(
                    "input/customShard.jar", "com.custom.CustomTransformationWithCassandraForIT")
                .build();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);
        createAndUploadJarToGcs(gcsResourceManager);
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
                customTransformation,
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
  public void testCustomTransformationForCassandra() throws InterruptedException, IOException {
    assertThatPipeline(jobInfo).isRunning();
    writeBasicRowInSpanner();
    assertBasicRowInCassandraDB();
  }

  private void writeBasicRowInSpanner() {
    Mutation m1 =
        Mutation.newInsertOrUpdateBuilder(CUSTOMER_TABLE)
            .set("id")
            .to(1)
            .set("first_name")
            .to("Jone")
            .set("last_name")
            .to("Woe")
            .build();
    spannerResourceManager.write(m1);
  }

  private void assertBasicRowInCassandraDB() throws InterruptedException {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> getRowCount(CUSTOMER_TABLE) == 1);

    /*
     * Added to handle updates.
     * TODO(khajanchi@), explore if this sleep be replaced with something more definite.
     */
    Thread.sleep(Duration.ofMinutes(1L).toMillis());

    assertThatResult(result).meetsConditions();

    Iterable<Row> rows;
    try {
      LOG.info("Reading from Cassandra table: {}", CUSTOMER_TABLE);
      rows = cassandraResourceManager.readTable(CUSTOMER_TABLE);
      LOG.info("Cassandra Rows: {}", rows.toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from Cassandra table: " + CUSTOMER_TABLE, e);
    }

    /*
     * Added to handle updates.
     * TODO(khajanchi@), explore if this sleep be replaced with something more definite.
     */
    Thread.sleep(Duration.ofMinutes(1L).toMillis());

    assertThat(rows).hasSize(1);

    for (Row row : rows) {
      LOG.info("Cassandra Row to Assert: {}", row.getFormattedContents());
      assertThat(row.getString("full_name")).isEqualTo("Jone Woe");
      assertThat(row.getString("first_name")).isEqualTo("Jone");
      assertThat(row.getString("last_name")).isEqualTo("Woe");
      assertThat(Objects.requireNonNull(row.getString("empty_string")).isEmpty()).isTrue();
      assertThat(row.isNull("null_key")).isTrue();
      assertThat(row.getInt("id")).isEqualTo(1);
    }
  }

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
