/*
 * Copyright (C) 2024 Google LLC
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
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SpannerToSourceDb} Flex template which tests a basic migration on
 * a simple schema with reserved keywords to Cassandra.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSourceDbReservedKeywordsCassandraIT extends SpannerToSourceDbITBase {

  private static final String SPANNER_DDL_RESOURCE =
      "SpannerToSourceDbReservedKeywordsCassandraIT/spanner-schema.sql";
  private static final String CASSANDRA_DDL_RESOURCE =
      "SpannerToSourceDbReservedKeywordsCassandraIT/cassandra-schema.cql";
  private static final String SESSION_FILE_RESOURCE =
      "SpannerToSourceDbReservedKeywordsCassandraIT/session.json";
  private static final String CASSANDRA_CONFIG_FILE_RESOURCE =
      "SpannerToSourceDbReservedKeywordsCassandraIT/cassandra-config-template.conf";

  private PipelineLauncher.LaunchInfo jobInfo;
  public SpannerResourceManager spannerResourceManager;
  private SpannerResourceManager spannerMetadataResourceManager;
  private CassandraResourceManager cassandraResourceManager;
  private GcsResourceManager gcsResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
    spannerMetadataResourceManager = createSpannerMetadataDatabase();
    cassandraResourceManager = generateKeyspaceAndBuildCassandraResource();
    gcsResourceManager = setUpSpannerITGcsResourceManager();
    createAndUploadCassandraConfigToGcs(
        gcsResourceManager, cassandraResourceManager, CASSANDRA_CONFIG_FILE_RESOURCE);
    gcsResourceManager.uploadArtifact(
        "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
    pubsubResourceManager = setUpPubSubResourceManager();
    subscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager)
                .replace("gs://" + gcsResourceManager.getBucket(), ""),
            gcsResourceManager);
    Map<String, String> jobParameters =
        new HashMap<>() {
          {
            put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
          }
        };
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
            CASSANDRA_SOURCE_TYPE,
            jobParameters);
  }

  @After
  public void tearDown() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        cassandraResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void testSpannerToCassandraReservedKeywords() throws InterruptedException, IOException {
    createCassandraSchema(cassandraResourceManager, CASSANDRA_DDL_RESOURCE);
    assertThatPipeline(jobInfo).isRunning();
    spannerResourceManager.write(generateData());
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)), () -> getRowCount("\"true\"") == 2);
    assertThatResult(result).meetsConditions();

    List<Row> actualData =
        StreamSupport.stream(cassandraResourceManager.readTable("\"true\"").spliterator(), false)
            .collect(Collectors.toList());
    List<Map<String, Object>> expectedData = getExpectedCassandraRows();

    assertThat(actualData).hasSize(expectedData.size());
    // Sort both lists by the primary key for deterministic comparison
    actualData.sort(Comparator.comparing(m -> m.getLong("COLUMN")));
    expectedData.sort(Comparator.comparing(m -> (Long) m.get("COLUMN")));

    for (int i = 0; i < expectedData.size(); i++) {
      assertThat(actualData.get(i).getLong("COLUMN")).isEqualTo(expectedData.get(i).get("COLUMN"));
      assertThat(actualData.get(i).getString("TABLE")).isEqualTo(expectedData.get(i).get("TABLE"));
      assertThat(actualData.get(i).getString("WITH")).isEqualTo(expectedData.get(i).get("WITH"));
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

  private List<Mutation> generateData() {
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("true")
            .set("COLUMN")
            .to(1)
            .set("TABLE")
            .to("value1")
            .set("WITH")
            .to("value1")
            .build());
    mutations.add(
        Mutation.newInsertOrUpdateBuilder("true")
            .set("COLUMN")
            .to(2)
            .set("TABLE")
            .to("value2")
            .set("WITH")
            .to("value2")
            .build());
    return mutations;
  }

  private List<Map<String, Object>> getExpectedCassandraRows() {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("COLUMN", 1L);
    row1.put("TABLE", "value1");
    row1.put("WITH", "value1");
    rows.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("COLUMN", 2L);
    row2.put("TABLE", "value2");
    row2.put("WITH", "value2");
    rows.add(row2);
    return rows;
  }
}
