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

import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.matchers.TemplateAsserts.assertThatResult;

import avro.shaded.com.google.common.collect.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManagerCluster;
import com.google.cloud.teleport.it.bigtable.BigtableTableSpec;
import com.google.cloud.teleport.it.bigtable.StaticBigtableResourceManager;
import com.google.cloud.teleport.it.common.ExceptionMessageUtils;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchConfig;
import com.google.cloud.teleport.it.launcher.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.launcher.PipelineOperator;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Config;
import com.google.cloud.teleport.it.launcher.PipelineOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link BigtableChangeStreamsToBigQuery}.
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToBigQueryIT extends TemplateTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToBigQueryIT.class);

  public static final String SOURCE_CDC_TABLE = "source_cdc_table";
  public static final String SOURCE_COLUMN_FAMILY = "cf";
  public static final String APP_PROFILE_ID = "cdc_app_profile";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private static final String TEST_REGION = "us-central1";
  private static final String TEST_ZONE = "us-central1-a";

  private static StaticBigtableResourceManager bigtableResourceManager;
  private static DefaultBigQueryResourceManager bigQueryResourceManager;

  private LaunchInfo launchInfo;

  @Test
  public void testBigtableChangeStreamsToBigQuerySingleMutationE2E() throws IOException {
    long timeNowMicros = System.currentTimeMillis() * 1000;

    //String name = testName.getMethodName();
    //String jobName = createJobName(name);
    String clusterName = "c1_cluster";

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[]{}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        APP_PROFILE_ID, true, Lists.asList(clusterName, new String[]{}));

    bigQueryResourceManager.createDataset(TEST_REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableTableId", SOURCE_CDC_TABLE)
                    .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableAppProfileId", APP_PROFILE_ID)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes")
            )
        );

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
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              Assert.assertFalse(
                  fvl.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()).isNull());
              Assert.assertFalse(
                  fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              Assert.assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              Assert.assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              Assert.assertEquals(
                  SOURCE_CDC_TABLE,
                  fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              Assert.assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);
            });
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryMutationsStartTimeE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;

    String clusterName = "c1_cluster";

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[]{}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        APP_PROFILE_ID, true, Lists.asList(clusterName, new String[]{}));

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

    Thread.sleep(10000L);
    long afterFirstMutation = System.currentTimeMillis();
    afterFirstMutation -= (afterFirstMutation % 1000);
    Thread.sleep(10000L);

    RowMutation toBeReadMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, valueToBeRead);
    bigtableResourceManager.write(toBeReadMutation);

    Thread.sleep(10000L);

    RowMutation nextToBeReadMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, nextValueToBeRead);
    bigtableResourceManager.write(nextToBeReadMutation);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableTableId", SOURCE_CDC_TABLE)
                    .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableAppProfileId", APP_PROFILE_ID)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes")
                    .addParameter("startTimestamp", Timestamp.of(new Date(afterFirstMutation)).toString())
            )
        );

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
              Assert.assertTrue(toBeReadValues.contains(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue()));
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              Assert.assertFalse(
                  fvl.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()).isNull());
              Assert.assertFalse(
                  fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              Assert.assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              Assert.assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              Assert.assertEquals(
                  SOURCE_CDC_TABLE,
                  fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              Assert.assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);

              toBeReadValues.remove(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue());
            });


  }


  @Test
  public void testBigtableChangeStreamsToBigQueryDeadLetterQueueE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;

    String clusterName = "c1_cluster";

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[]{}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        APP_PROFILE_ID, true, Lists.asList(clusterName, new String[]{}));

    bigQueryResourceManager.createDataset(TEST_REGION);

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String goodValue = UUID.randomUUID().toString();

    // Making some 15MB value
    String tooBigValue = StringUtils.repeat(UUID.randomUUID().toString(),
        15 * 1024 * 1024 / goodValue.length());

    long beforeMutations = System.currentTimeMillis();
    beforeMutations -= (beforeMutations % 1000);

    Thread.sleep(10000L);

    RowMutation tooLargeMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, tooBigValue);
    bigtableResourceManager.write(tooLargeMutation);

    RowMutation smallMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, goodValue);
    bigtableResourceManager.write(smallMutation);

    Thread.sleep(10000L);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableTableId", SOURCE_CDC_TABLE)
                    .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableAppProfileId", APP_PROFILE_ID)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes")
                    .addParameter("dlqRetryMinutes", "1")
                    .addParameter("dlqDirectory", getGcsPath("dlq"))
                    .addParameter("dlqMaxRetries", "1")
                    .addParameter("startTimestamp", Timestamp.of(new Date(beforeMutations)).toString())
            )
        );

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
              Assert.assertTrue(toBeReadValues.contains(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue()));
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              Assert.assertFalse(
                  fvl.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()).isNull());
              Assert.assertFalse(
                  fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              Assert.assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              Assert.assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              Assert.assertEquals(
                  SOURCE_CDC_TABLE,
                  fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              Assert.assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);

              toBeReadValues.remove(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue());
            });

    Storage storage = StorageOptions.newBuilder().build().getService();

    long started = System.currentTimeMillis();
    long waitForFile = Duration.ofMinutes(30).toMillis();

    String filterPrefix = String.join("/", getClass().getSimpleName(), artifactClient.runId(),
        "dlq", "severe");
    BlobListOption listOptions = BlobListOption.prefix(filterPrefix);

    LOG.info("Looking for files with a prefix: " + filterPrefix);

    boolean found = false;
    while (!found && System.currentTimeMillis() <= (started + waitForFile)) {
      Page<Blob> blobs = storage.list(artifactBucketName, listOptions);

      for (Blob blob : blobs.iterateAll()) {
        byte[] content = storage.readAllBytes(blob.getBlobId());
        ObjectMapper om = new ObjectMapper();
        JsonNode severeError = om.readTree(content);
        Assert.assertNotNull(severeError);

        String errorMessage = severeError.get("error_message").asText();
        Assert.assertEquals(
            "GenericData{classInfo=[errors, index], {errors=[GenericData{classInfo="
                + "[debugInfo, location, message, reason], {reason=row-too-large}}]}}",
            errorMessage);
        found = true;
      }

      if (!found) {
        Thread.sleep(1000);
      }
    }

    if (!found) {
      Assert.fail("The failed message was not found in DLQ");
    }
  }

  @Before
  public void setup() throws IOException {
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName, PROJECT)
            .setCredentials(credentials)
            .build();
    // TODO: StaticBigtableResourceManager has to be replaced with DefaultBigtableResourceManager
    // when it supports CDC configs
    bigtableResourceManager = StaticBigtableResourceManager.builder(/* testName.getMethodName(), */
            PROJECT)
        .setCredentialsProvider(credentialsProvider)
        .setInstanceId(System.getProperty("bigtableInstanceId"))
        .setTableId(System.getProperty("bigtableTableId"))
        .setAppProfileId(System.getProperty("bigtableAppProfileId"))
        .build();
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
        if (ExceptionMessageUtils.underlyingErrorContains(e, "Not found: Table")) {
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
          configBuilder.setTimeoutAfter(
                  EXPECTED_REPLICATION_MAX_WAIT_TIME.minus(Duration.ofMinutes(3)))
              .setCheckAfter(Duration.ofSeconds(5));
    } else {
      configBuilder.setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME);
    }

    return configBuilder.build();
  }


  private void waitForQueryToReturnRows(String query, int resultsRequired, boolean cancelOnceDone)
      throws IOException {
    Config config = createConfig(launchInfo);
    Result result = cancelOnceDone ? pipelineOperator().waitForConditionAndCancel(config,
        dataShownUp(query, resultsRequired))
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
        + String.format("%s = '%s'", ChangelogColumn.COLUMN_FAMILY.getBqColumnName(),
        SOURCE_COLUMN_FAMILY)
        + (rowkey != null ? String.format(" AND %s = '%s'",
        ChangelogColumn.ROW_KEY_STRING.getBqColumnName(), rowkey) : "")
        + (column != null ? String.format(" AND %s = '%s'",
        ChangelogColumn.COLUMN.getBqColumnName(), column) : "")
        + (value != null ? String.format(" AND %s = '%s'",
        ChangelogColumn.VALUE_STRING.getBqColumnName(), value) : "");
  }
}
