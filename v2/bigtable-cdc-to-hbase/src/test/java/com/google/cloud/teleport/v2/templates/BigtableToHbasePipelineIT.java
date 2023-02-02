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
package com.google.cloud.teleport.v2.templates;


import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colFamily;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.colQualifier;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.rowKey;
import static com.google.cloud.teleport.v2.templates.constants.TestConstants.value;

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.it.TemplateTestBase;
import com.google.cloud.teleport.it.bigtable.StaticBigtableResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.BigtableToHbasePipeline.BigtableToHbasePipelineOptions;
import com.google.cloud.teleport.v2.templates.utils.HbaseUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End to end table that runs the pipeline from Bigtable to Hbase.
 * TODO: make Bigtable resource hermetic after Cdc GA and when we can enable cdc from admin API.
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableToHbasePipeline.class)
@RunWith(JUnit4.class)
public class BigtableToHbasePipelineIT extends TemplateTestBase {

  // private static StaticBigtableResourceManager bigtableResourceManager;
  private static BigtableToHbasePipelineOptions pipelineOptions;
  private static HBaseTestingUtility hBaseTestingUtility;
  private static Table hbaseTable;
  private static StaticBigtableResourceManager bigtableResourceManager;

  @BeforeClass
  public static void setUpCluster() throws Exception {

    // Parse input as though from a live run. This requires passing in params to test via -Dparameters="..."
    String input = System.getProperty("parameters");
    String[] keyValuePairs = input.split(",");
    Map<String, String> args = new HashMap<>();
    for (String pair : keyValuePairs) {
      String[] entry = pair.split("=");
      args.put(entry[0], entry[1]);
    }

    // Create pipeline options from some args
    // Note that we set start and end times in actual test run
    pipelineOptions = PipelineOptionsFactory.create().as(BigtableToHbasePipelineOptions.class);
    // Set bigtable change stream options
    pipelineOptions.setBigtableProjectId(args.get("bigtableProjectId"));
    pipelineOptions.setInstanceId(args.get("instanceId"));
    pipelineOptions.setTableId(args.get("tableId"));
    pipelineOptions.setAppProfileId(args.get("appProfileId"));
    // Set pipeline options
    pipelineOptions.setStreaming(true);
    pipelineOptions.setExperiments(Arrays.asList("use_runner_v2"));

    // Create Hbase cluster
    hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniCluster();
    // Create HBase table that mirrors persistent Hbase table.
    // TODO: make this hermetic after cdc GA
    hbaseTable = HbaseUtils.createTable(hBaseTestingUtility, pipelineOptions.getTableId());
  }

  @Before
  public void setUp() throws Exception {
    // TODO: StaticBigtableResourceManager has to be replaced with DefaultBigtableResourceManager
    //  when it supports CDC configs
    // Create Bigtable resource manager in setUp because we need credentialsProvider which requires non-static context
    bigtableResourceManager = StaticBigtableResourceManager.builder(pipelineOptions.getBigtableProjectId())
        .setCredentialsProvider(credentialsProvider)
        .setInstanceId(pipelineOptions.getInstanceId())
        .setTableId(pipelineOptions.getTableId())
        .setAppProfileId(pipelineOptions.getAppProfileId())
        .build();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (hBaseTestingUtility != null) {
      hBaseTestingUtility.shutdownMiniCluster();
    }
    if (bigtableResourceManager != null) {
      // TODO: cleanUpAll is a stub because we use persistent resources.
      bigtableResourceManager.cleanupAll();
    }
  }

  @Test
  public void testEndToEnd() throws Exception {
    // Set time to just cover the timeframe of the row mutation.
    Timestamp start = Timestamp.now();
    Timestamp endTime =
        Timestamp.ofTimeSecondsAndNanos(
            start.getSeconds() + 30,
            start.getNanos());
    pipelineOptions.setStartTimestamp(start.toString());
    pipelineOptions.setEndTimestamp(endTime.toString());

    // Write to Bigtable during this timeframe.
    RowMutation setCell = RowMutation.create(pipelineOptions.getTableId(), rowKey)
            .setCell(colFamily, colQualifier, value);
    bigtableResourceManager.write(setCell);

    PipelineResult pipelineResult = BigtableToHbasePipeline.bigtableToHbasePipeline(pipelineOptions, hBaseTestingUtility.getConfiguration());

    try {
      pipelineResult.waitUntilFinish();
    } catch (Exception e) {
      throw new Exception("Error: pipeline could not finish");
    }
    
    // Assert same value has been propagated to hbase table
    Assert.assertEquals(
        value,
        HbaseUtils.getCell(hbaseTable, rowKey,colFamily,colQualifier)
    );
    // delete cell from persisted table if run successful.
    // TODO: remove this after Cdc GA
    RowMutation deleteCell = RowMutation.create(pipelineOptions.getTableId(), rowKey)
        .deleteCells(colFamily, colQualifier);
    bigtableResourceManager.write(deleteCell);
  }
}
