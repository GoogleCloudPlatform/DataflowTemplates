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
import com.google.cloud.teleport.it.TemplateTestBase;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TODO: WIP.
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableToHbasePipeline.class)
@RunWith(JUnit4.class)
public class BigtableToHbasePipelineIT extends TemplateTestBase {

  // private static StaticBigtableResourceManager bigtableResourceManager;
  private static BigtableToHbasePipelineOptions pipelineOptions;
  private static HBaseTestingUtility hBaseTestingUtility;
  private static Table hbaseTable;
  @Before
  public void setUp() throws Exception {

    // Parse input as though from a live run.
    String input = System.getProperty("parameters");
    String[] keyValuePairs = input.split(",");
    Map<String, String> args = new HashMap<>();

    for (String pair : keyValuePairs) {
      String[] entry = pair.split("=");
      args.put(entry[0], entry[1]);
    }

    hBaseTestingUtility = new HBaseTestingUtility();
    hBaseTestingUtility.startMiniCluster();
    hbaseTable = HbaseUtils.createTable(hBaseTestingUtility, args.get("tableId"));

    // Create pipeline options.
    pipelineOptions = PipelineOptionsFactory.create().as(BigtableToHbasePipelineOptions.class);
    // Set bigtable change stream options
    pipelineOptions.setBigtableProjectId(args.get("bigtableProjectId"));
    pipelineOptions.setInstanceId(args.get("instanceId"));
    pipelineOptions.setTableId(args.get("tableId"));
    pipelineOptions.setAppProfileId(args.get("appProfileId"));
    // Set pipeline options
    pipelineOptions.setStreaming(true);
    pipelineOptions.setExperiments(Arrays.asList("use_runner_v2"));
    // Set time to be short-lived for this test
    Timestamp start = Timestamp.now();
    Timestamp endTime =
        Timestamp.ofTimeSecondsAndNanos(
            start.getSeconds() + 120,
            start.getNanos());
    pipelineOptions.setStartTimestamp(start.toString());
    pipelineOptions.setEndTimestamp(endTime.toString());

    // bigtableResourceManager =
    //     StaticBigtableResourceManager.builder(pipelineOptions.getBigtableProjectId())
    //         .setCredentialsProvider(credentialsProvider)
    //         .setInstanceId(pipelineOptions.getInstanceId())
    //         .setTableId(pipelineOptions.getTableId())
    //         .setAppProfileId(pipelineOptions.getAppProfileId())
    //         .build();
  }

  @After
  public void tearDown() throws IOException {
    hBaseTestingUtility.shutdownMiniCluster();
  }

  @Test
  public void testEndToEnd() throws Exception {
    PipelineResult pipelineResult = BigtableToHbasePipeline.bigtableToHbasePipeline(pipelineOptions, hBaseTestingUtility.getConfiguration());

    // RowMutation rowMutation = RowMutation
    //     .create(pipelineOptions.getTableId(), rowKey)
    //     .setCell(colFamily, colQualifier, value);
    // bigtableResourceManager.write(rowMutation);

    try {
      pipelineResult.waitUntilFinish();
    } catch (Exception e) {
      throw new Exception("Error: pipeline could not finish");
    }
    Assert.assertEquals(
        value,
        HbaseUtils.getCell(hbaseTable, rowKey,colFamily,colQualifier)
    );
  }
}
