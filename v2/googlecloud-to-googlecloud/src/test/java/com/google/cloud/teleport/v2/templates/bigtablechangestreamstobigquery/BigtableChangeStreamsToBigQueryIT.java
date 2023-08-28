/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.api.gax.paging.Page;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerCluster;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToBigQuery}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableChangeStreamsToBigQueryIT.class);

  public static final String SOURCE_CDC_TABLE = "source_cdc_table";
  public static final String SOURCE_COLUMN_FAMILY = "cf";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private static final String TEST_REGION = "us-central1";
  private static final String TEST_ZONE = "us-central1-b";
  private BigtableResourceManager bigtableResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private LaunchInfo launchInfo;

  @Test
  public void testBigtableChangeStreamsToBigQuerySingleMutationE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;
    String clusterName = "cluster-c1";
    String appProfileId = generateAppProfileId();

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, Lists.asList(clusterName, new String[] {}));

    bigQueryResourceManager.createDataset(TEST_REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", SOURCE_CDC_TABLE)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes")));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    RowMutation rowMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, value);
    bigtableResourceManager.write(rowMutation);

    String query = newLookForValuesQuery(rowkey, column, value);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              assertEquals(
                  SOURCE_CDC_TABLE,
                  fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              assertTrue(fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);
            });
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + System.nanoTime();
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryMutationsStartTimeE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;
    String clusterName = "cluster-c1";
    String appProfileId = generateAppProfileId();

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, Lists.asList(clusterName, new String[] {}));

    bigQueryResourceManager.createDataset(TEST_REGION);

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String tooEarlyValue = UUID.randomUUID().toString();
    String valueToBeRead = UUID.randomUUID().toString();
    String nextValueToBeRead = UUID.randomUUID().toString();

    RowMutation earlyMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, tooEarlyValue);
    bigtableResourceManager.write(earlyMutation);

    TimeUnit.SECONDS.sleep(5);
    long afterFirstMutation = System.currentTimeMillis();
    afterFirstMutation -= (afterFirstMutation % 1000);
    TimeUnit.SECONDS.sleep(5);

    RowMutation toBeReadMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, valueToBeRead);
    bigtableResourceManager.write(toBeReadMutation);

    TimeUnit.SECONDS.sleep(5);

    RowMutation nextToBeReadMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, nextValueToBeRead);
    bigtableResourceManager.write(nextToBeReadMutation);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", SOURCE_CDC_TABLE)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes")
                    .addParameter(
                        "bigtableChangeStreamStartTimestamp",
                        Timestamp.of(new Date(afterFirstMutation)).toString())));

    assertThatPipeline(launchInfo).isRunning();

    String query = newLookForValuesQuery(rowkey, column, null);
    waitForQueryToReturnRows(query, 2, true);

    HashSet<String> toBeReadValues = new HashSet<>();
    toBeReadValues.add(valueToBeRead);
    toBeReadValues.add(nextValueToBeRead);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              assertTrue(
                  toBeReadValues.contains(
                      fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue()));
              assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              assertEquals(
                  SOURCE_CDC_TABLE,
                  fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              assertTrue(fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);

              toBeReadValues.remove(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue());
            });
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryDeadLetterQueueE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;
    String clusterName = "alexeyku-prod-c1";
    String appProfileId = generateAppProfileId();

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, Lists.asList(clusterName, new String[] {}));

    bigQueryResourceManager.createDataset(TEST_REGION);

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String goodValue = UUID.randomUUID().toString();

    // Making some 15MB value
    String tooBigValue =
        StringUtils.repeat(UUID.randomUUID().toString(), 15 * 1024 * 1024 / goodValue.length());

    long beforeMutations = System.currentTimeMillis();
    beforeMutations -= (beforeMutations % 1000);

    TimeUnit.SECONDS.sleep(5);

    RowMutation tooLargeMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, tooBigValue);
    bigtableResourceManager.write(tooLargeMutation);

    RowMutation smallMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, goodValue);
    bigtableResourceManager.write(smallMutation);

    TimeUnit.SECONDS.sleep(5);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", SOURCE_CDC_TABLE)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes")
                    .addParameter("dlqDirectory", getGcsPath("dlq"))
                    .addParameter(
                        "bigtableChangeStreamStartTimestamp",
                        Timestamp.of(new Date(beforeMutations)).toString())));

    assertThatPipeline(launchInfo).isRunning();

    String query = newLookForValuesQuery(rowkey, column, null);
    waitForQueryToReturnRows(query, 1, false);

    HashSet<String> toBeReadValues = new HashSet<>();
    toBeReadValues.add(goodValue);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              assertTrue(
                  toBeReadValues.contains(
                      fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue()));
              assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              assertEquals(
                  SOURCE_CDC_TABLE,
                  fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              assertTrue(fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);

              toBeReadValues.remove(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue());
            });

    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT).build().getService();

    String filterPrefix =
        String.join("/", getClass().getSimpleName(), gcsClient.runId(), "dlq", "severe");
    LOG.info("Looking for files with a prefix: {}", filterPrefix);

    await("The failed message was not found in DLQ")
        .atMost(Duration.ofMinutes(30))
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              Page<Blob> blobs =
                  storage.list(artifactBucketName, BlobListOption.prefix(filterPrefix));

              for (Blob blob : blobs.iterateAll()) {
                // Ignore temp files
                if (blob.getName().contains(".temp-beam/")) {
                  continue;
                }

                byte[] content = storage.readAllBytes(blob.getBlobId());
                ObjectMapper om = new ObjectMapper();
                JsonNode severeError = om.readTree(content);
                assertNotNull(severeError);
                JsonNode errorMessageNode = severeError.get("error_message");
                assertNotNull(errorMessageNode);
                assertTrue(errorMessageNode instanceof TextNode);
                String messageText = errorMessageNode.asText();
                assertTrue(
                    "Unexpected message text: " + messageText,
                    StringUtils.contains(
                        messageText, "Row payload too large. Maximum size 10000000"));
                return true;
              }

              return false;
            });
  }

  @Before
  public void setup() throws IOException {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    BigtableResourceManager.Builder rmBuilder =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider);

    bigtableResourceManager = rmBuilder.build();
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager, bigQueryResourceManager);
  }

  @NotNull
  private Supplier<Boolean> dataShownUp(String query, int minRows) {
    return () -> {
      try {
        return bigQueryResourceManager.runQuery(query).getTotalRows() >= minRows;
      } catch (Exception e) {
        if (ExceptionUtils.containsMessage(e, "Not found: Table")) {
          return false;
        } else {
          throw e;
        }
      }
    };
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    Config.Builder configBuilder =
        Config.builder().setJobId(info.jobId()).setProject(PROJECT).setRegion(REGION);

    // For DirectRunner tests, reduce the max time and the interval, as there is no worker required
    if (System.getProperty("directRunnerTest") != null) {
      configBuilder =
          configBuilder
              .setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME.minus(Duration.ofMinutes(3)))
              .setCheckAfter(Duration.ofSeconds(5));
    } else {
      configBuilder.setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME);
    }

    return configBuilder.build();
  }

  private void waitForQueryToReturnRows(String query, int resultsRequired, boolean cancelOnceDone)
      throws IOException {
    Config config = createConfig(launchInfo);
    Result result =
        cancelOnceDone
            ? pipelineOperator()
                .waitForConditionAndCancel(config, dataShownUp(query, resultsRequired))
            : pipelineOperator().waitForCondition(config, dataShownUp(query, resultsRequired));
    assertThatResult(result).meetsConditions();
  }

  private String newLookForValuesQuery(String rowkey, String column, String value) {
    return "SELECT * FROM `"
        + bigQueryResourceManager.getDatasetId()
        + "."
        + SOURCE_CDC_TABLE
        + "_changes`"
        + " WHERE "
        + String.format(
            "%s = '%s'", ChangelogColumn.COLUMN_FAMILY.getBqColumnName(), SOURCE_COLUMN_FAMILY)
        + (rowkey != null
            ? String.format(
                " AND %s = '%s'", ChangelogColumn.ROW_KEY_STRING.getBqColumnName(), rowkey)
            : "")
        + (column != null
            ? String.format(" AND %s = '%s'", ChangelogColumn.COLUMN.getBqColumnName(), column)
            : "")
        + (value != null
            ? String.format(" AND %s = '%s'", ChangelogColumn.VALUE_STRING.getBqColumnName(), value)
            : "");
  }
}
