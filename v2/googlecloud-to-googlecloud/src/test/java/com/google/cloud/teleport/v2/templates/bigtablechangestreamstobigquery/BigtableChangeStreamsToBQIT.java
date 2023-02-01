/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.cloud.teleport.it.dataflow.DataflowUtils.createJobName;
import static com.google.common.truth.Truth.assertThat;

import avro.shaded.com.google.common.collect.Lists;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.bigtable.BigtableResourceManagerCluster;
import com.google.cloud.teleport.it.bigtable.BigtableTableSpec;
import com.google.cloud.teleport.it.bigtable.StaticBigtableResourceManager;
import com.google.cloud.teleport.it.common.ExceptionMessageUtils;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowClient.LaunchConfig;
import com.google.cloud.teleport.it.dataflow.DataflowOperator;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Config;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigtableChangeStreamsToBigQuery}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToBQIT extends TemplateTestBase {

  public static final String SOURCE_CDC_TABLE = "source_cdc_table";
  public static final String SOURCE_COLUMN_FAMILY = "cf";
  public static final String APP_PROFILE_ID = "cdc_app_profile";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private static final String TEST_REGION = "us-central1";
  private static final String TEST_ZONE = "us-central1-a";

  private static StaticBigtableResourceManager bigtableResourceManager;
  private static DefaultBigQueryResourceManager bigQueryResourceManager;

  private JobInfo jobInfo;

  @Test
  public void testBigtableChangeStreamsToBigQuerySingleMutationEndToEnd() throws IOException {
    long timeNowMcsec = System.currentTimeMillis() * 1000;

    String name = testName.getMethodName();
    String jobName = createJobName(name);
    String clusterName = "c1_cluster";

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(SOURCE_CDC_TABLE, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        APP_PROFILE_ID, true, Lists.asList(clusterName, new String[] {}));

    bigQueryResourceManager.createDataset(TEST_REGION);

    LaunchConfig.Builder options =
        LaunchConfig.builder(jobName, specPath)
            .addParameter("bigtableTableId", SOURCE_CDC_TABLE)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableAppProfileId", APP_PROFILE_ID)
            .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
            .addParameter("bigQueryChangelogTableName", SOURCE_CDC_TABLE + "_changes");

    jobInfo = launchTemplate(options);

    assertThat(jobInfo.state()).isIn(JobState.ACTIVE_STATES);

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    RowMutation rowMutation =
        RowMutation.create(SOURCE_CDC_TABLE, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, value);
    bigtableResourceManager.write(rowMutation);

    String query =
        String.format(
            "SELECT * FROM `"
                + bigQueryResourceManager.getDatasetId()
                + "."
                + SOURCE_CDC_TABLE
                + "_changes`"
                + " WHERE "
                + ChangelogColumn.ROW_KEY_STRING.getBqColumnName()
                + "='%s' AND "
                + ChangelogColumn.COLUMN_FAMILY.getBqColumnName()
                + "='%s' AND "
                + ChangelogColumn.COLUMN.getBqColumnName()
                + "='%s' AND "
                + ChangelogColumn.VALUE_STRING.getBqColumnName()
                + "='%s'",
            rowkey,
            SOURCE_COLUMN_FAMILY,
            column,
            value);

    Result result =
        new DataflowOperator(getDataflowClient())
            .waitForCondition(getWaitForPipelineConfig(jobInfo), dataShownUp(query));

    assertThat(result).isEqualTo(Result.CONDITION_MET);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              Assert.assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMcsec);
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

  @Before
  public void setup() throws IOException {
    bigQueryResourceManager =
        DefaultBigQueryResourceManager.builder(testName.getMethodName(), PROJECT).build();
    // TODO: StaticBigtableResourceManager has to be replaced with DefaultBigtableResourceManager
    // when it supports CDC configs
    bigtableResourceManager =
        StaticBigtableResourceManager.builder(/* testName.getMethodName(), */ PROJECT)
            .setCredentialsProvider(credentialsProvider)
            .setInstanceId(System.getProperty("bigtableInstanceId"))
            .setTableId(System.getProperty("bigtableTableId"))
            .setAppProfileId(System.getProperty("bigtableAppProfileId"))
            .build();
  }

  @After
  public void tearDownClass() {
    // If we destroy Cloud Bigtable / BigQuery resources before cancelling job, it might get
    // stuck in draining
    if (jobInfo != null) {
      try {
        getDataflowClient().cancelJob(PROJECT, TEST_REGION, jobInfo.jobId());
      } catch (Exception e) {
        e.printStackTrace(System.err);
      }
    }

    if (bigtableResourceManager != null) {
      bigtableResourceManager.cleanupAll();
    }
    if (bigQueryResourceManager != null) {
      bigQueryResourceManager.cleanupAll();
    }
  }

  @NotNull
  private Supplier<Boolean> dataShownUp(String query) {
    return () -> {
      try {
        return bigQueryResourceManager.runQuery(query).getTotalRows() != 0;
      } catch (Exception e) {
        if (ExceptionMessageUtils.underlyingErrorContains(e, "Not found: Table")) {
          return false;
        } else {
          throw e;
        }
      }
    };
  }

  public Config getWaitForPipelineConfig(JobInfo info) {
    return Config.builder()
        .setProject(PROJECT)
        .setRegion(REGION)
        .setJobId(info.jobId())
        .setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME)
        .build();
  }
}
