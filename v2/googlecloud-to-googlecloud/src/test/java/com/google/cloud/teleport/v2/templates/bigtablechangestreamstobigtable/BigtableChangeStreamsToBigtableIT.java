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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToBigtable}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableChangeStreamsToBigtable.class)
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToBigtableIT extends TemplateTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableChangeStreamsToBigtableIT.class);

  public static final String SOURCE_COLUMN_FAMILY = "cf";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private BigtableResourceManager bigtableResourceManager;
  private LaunchInfo launchInfo;

  @Before
  public void setup() throws IOException {
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager);
  }

  @Test
  public void testBigtableChangeStreamsToBigtableE2E() throws Exception {
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));

    BigtableTableSpec dstTableSpec = new BigtableTableSpec();
    dstTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));

    String srcTable = BigtableResourceManagerUtils.generateTableId("src-mutation");
    String dstTable = BigtableResourceManagerUtils.generateTableId("dst-mutation");

    bigtableResourceManager.createTable(srcTable, cdcTableSpec);
    bigtableResourceManager.createTable(dstTable, dstTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadProjectId", PROJECT)
                    .addParameter("bigtableReadTableId", srcTable)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigtableWriteProjectId", PROJECT)
                    .addParameter("bigtableWriteTableId", dstTable)
                    .addParameter(
                        "bigtableWriteInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableWriteColumnFamily", SOURCE_COLUMN_FAMILY)));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, value);
    bigtableResourceManager.write(rowMutation);

    Config config = createConfig(launchInfo);
    Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                config,
                () -> {
                  try {
                    List<Row> rows = bigtableResourceManager.readTable(dstTable);
                    for (Row row : rows) {
                      if (row.getKey().toStringUtf8().equals(rowkey)) {
                        return true;
                      }
                    }
                    return false;
                  } catch (Exception e) {
                    LOG.warn("Error reading destination table", e);
                    return false;
                  }
                });

    assertThatResult(result).meetsConditions();
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + randomAlphanumeric(8).toLowerCase() + "_" + System.nanoTime();
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    Config.Builder configBuilder =
        Config.builder().setJobId(info.jobId()).setProject(PROJECT).setRegion(REGION);

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
}
