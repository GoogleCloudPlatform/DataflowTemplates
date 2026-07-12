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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

/**
 * Integration test for {@link SpannerToSourceDb} Flex template replicating from Spanner to Spanner.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToSpannerIT extends SpannerToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSpannerIT.class);

  private static final Duration TEST_TIMEOUT = Duration.ofMinutes(15);

  private static final String SPANNER_DDL_RESOURCE = "SpannerToSourceDbIT/spanner-schema.sql";

  private static final String TABLE = "Users";
  private static final String TABLE_WITH_VIRTUAL_GEN_COL = "TableWithVirtualGeneratedColumn";
  private static final String TABLE_WITH_STORED_GEN_COL = "TableWithStoredGeneratedColumn";
  private static final String TABLE_WITH_IDENTITY_COL = "TableWithIdentityColumn";
  private static final String BOUNDARY_CHECK_TABLE =
      "testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO";

  private static final HashSet<SpannerToSpannerIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  private static SpannerResourceManager spannerResourceManager; // Source Spanner
  private static SpannerResourceManager spannerDestinationResourceManager; // Destination Spanner
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToSpannerIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);

        spannerDestinationResourceManager =
            SpannerResourceManager.builder("rr-dest-" + testName, PROJECT, REGION)
                .maybeUseStaticInstance()
                .build();
        createSpannerDDL(spannerDestinationResourceManager, SPANNER_DDL_RESOURCE);

        spannerMetadataResourceManager = spannerDestinationResourceManager;

        gcsResourceManager = setUpSpannerITGcsResourceManager();

        String spannerShardConfig =
            "["
                + "  {"
                + "    \"projectId\": \""
                + PROJECT
                + "\","
                + "    \"instanceId\": \""
                + spannerDestinationResourceManager.getInstanceId()
                + "\","
                + "    \"databaseId\": \""
                + spannerDestinationResourceManager.getDatabaseId()
                + "\""
                + "  }"
                + "]";
        gcsResourceManager.createArtifact("input/spanner-shard.json", spannerShardConfig);

        pubsubResourceManager = setUpPubSubResourceManager();
        SubscriptionName subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager)
                    .replace("gs://" + gcsResourceManager.getBucket(), ""),
                gcsResourceManager);

        Map<String, String> jobParameters = new HashMap<>();
        jobParameters.put(
            "sourceShardsFilePath", getGcsPath("input/spanner-shard.json", gcsResourceManager));

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
                Constants.SOURCE_SPANNER,
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToSpannerIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerDestinationResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToSpannerBasicReplication() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    Mutation m =
        Mutation.newInsertOrUpdateBuilder(TABLE)
            .set("id")
            .to(101)
            .set("full_name")
            .to("Alice")
            .set("from")
            .to("London")
            .build();
    spannerResourceManager.write(m);
    LOG.info("Successfully wrote row to source Spanner Users table");

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> {
                  List<Struct> rows =
                      spannerDestinationResourceManager.readTableRecords(
                          TABLE, List.of("id", "full_name", "from"));
                  for (Struct row : rows) {
                    if (!row.isNull("id")
                        && row.getLong("id") == 101
                        && !row.isNull("full_name")
                        && "Alice".equals(row.getString("full_name"))) {
                      LOG.info(
                          "Row successfully found in destination Spanner: id=101, full_name=Alice");
                      return true;
                    }
                  }
                  return false;
                });

    assertThatResult(result).meetsConditions();
  }

  @Test
  public void spannerToSpannerWithGeneratedColumns() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    // INSERT
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_STORED_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(1)
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_STORED_GEN_COL)
            .set("id")
            .to(2)
            .set("column1")
            .to(2)
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_VIRTUAL_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(1)
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_VIRTUAL_GEN_COL)
            .set("id")
            .to(2)
            .set("column1")
            .to(2)
            .build());
    spannerResourceManager.write(mutations);

    // Assert INSERT
    assertReplication(TABLE_WITH_VIRTUAL_GEN_COL, 2, List.of("id"), "id", 2L);
    assertReplication(TABLE_WITH_STORED_GEN_COL, 2, List.of("id"), "id", 2L);

    // UPDATE
    mutations = new ArrayList<>();
    mutations.add(
        Mutation.newUpdateBuilder(TABLE_WITH_STORED_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(3)
            .build());
    mutations.add(
        Mutation.newUpdateBuilder(TABLE_WITH_VIRTUAL_GEN_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to(4)
            .build());
    spannerResourceManager.write(mutations);

    // Assert UPDATE (wait for propagation)
    assertReplication(TABLE_WITH_VIRTUAL_GEN_COL, 2, List.of("id", "column1"), "column1", 4L);
    assertReplication(TABLE_WITH_STORED_GEN_COL, 2, List.of("id", "column1"), "column1", 3L);

    // DELETE
    Mutation m1 =
        Mutation.delete(
            TABLE_WITH_VIRTUAL_GEN_COL,
            com.google.cloud.spanner.Key.newBuilder().append(1).build());
    Mutation m2 =
        Mutation.delete(
            TABLE_WITH_VIRTUAL_GEN_COL,
            com.google.cloud.spanner.Key.newBuilder().append(2).build());
    Mutation m3 =
        Mutation.delete(
            TABLE_WITH_STORED_GEN_COL, com.google.cloud.spanner.Key.newBuilder().append(1).build());
    Mutation m4 =
        Mutation.delete(
            TABLE_WITH_STORED_GEN_COL, com.google.cloud.spanner.Key.newBuilder().append(2).build());
    spannerResourceManager.write(Arrays.asList(m1, m2, m3, m4));

    // Assert DELETE
    assertReplication(TABLE_WITH_VIRTUAL_GEN_COL, 0, List.of("id"), null, null);
    assertReplication(TABLE_WITH_STORED_GEN_COL, 0, List.of("id"), null, null);
  }

  @Test
  public void spannerToSpannerWithIdentityColumns() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    // INSERT
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_IDENTITY_COL)
            .set("id")
            .to(1)
            .set("column1")
            .to("id1")
            .build());
    mutations.add(
        Mutation.newInsertBuilder(TABLE_WITH_IDENTITY_COL)
            .set("id")
            .to(2)
            .set("column1")
            .to("id2")
            .build());
    spannerResourceManager.write(mutations);

    // Assert INSERT
    assertReplication(TABLE_WITH_IDENTITY_COL, 2, List.of("id", "column1"), "column1", "id2");
  }

  @Test
  public void spannerToSpannerMaxColAndTableNameTest() throws InterruptedException {
    assertThatPipeline(jobInfo).isRunning();

    Mutation.WriteBuilder mutationBuilder =
        Mutation.newInsertOrUpdateBuilder(BOUNDARY_CHECK_TABLE).set("id").to(1);
    mutationBuilder
        .set("col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvY")
        .to("SampleTestValue");
    spannerResourceManager.write(mutationBuilder.build());

    // Assert INSERT
    assertReplication(BOUNDARY_CHECK_TABLE, 1, List.of("id"), "id", 1L);
  }

  private void assertReplication(
      String table,
      int expectedRowCount,
      List<String> columns,
      String checkCol,
      Object expectedColValue) {
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, TEST_TIMEOUT),
                () -> {
                  List<Struct> rows =
                      spannerDestinationResourceManager.readTableRecords(table, columns);

                  if (rows.size() == expectedRowCount) {
                    if (expectedRowCount == 0) {
                      return true;
                    }
                    if (checkCol == null) {
                      return true;
                    }
                    for (Struct row : rows) {
                      if (row.isNull(checkCol)) {
                        continue;
                      }
                      if (expectedColValue instanceof Long
                          && row.getLong(checkCol) == ((Long) expectedColValue).longValue()) {
                        return true;
                      } else if (expectedColValue instanceof String
                          && expectedColValue.equals(row.getString(checkCol))) {
                        return true;
                      } else if (expectedColValue instanceof Boolean
                          && row.getBoolean(checkCol)
                              == ((Boolean) expectedColValue).booleanValue()) {
                        return true;
                      } else if (expectedColValue instanceof Double
                          && row.getDouble(checkCol) == ((Double) expectedColValue).doubleValue()) {
                        return true;
                      } else if (expectedColValue instanceof Float
                          && row.getFloat(checkCol) == ((Float) expectedColValue).floatValue()) {
                        return true;
                      } else if (expectedColValue instanceof java.math.BigDecimal
                          && expectedColValue.equals(row.getBigDecimal(checkCol))) {
                        return true;
                      } else if (expectedColValue instanceof com.google.cloud.ByteArray
                          && expectedColValue.equals(row.getBytes(checkCol))) {
                        return true;
                      } else if (expectedColValue instanceof com.google.cloud.Timestamp
                          && expectedColValue.equals(row.getTimestamp(checkCol))) {
                        return true;
                      } else if (expectedColValue instanceof com.google.cloud.Date
                          && expectedColValue.equals(row.getDate(checkCol))) {
                        return true;
                      }
                    }
                  }
                  return false;
                });
    assertThatResult(result).meetsConditions();
  }
}
