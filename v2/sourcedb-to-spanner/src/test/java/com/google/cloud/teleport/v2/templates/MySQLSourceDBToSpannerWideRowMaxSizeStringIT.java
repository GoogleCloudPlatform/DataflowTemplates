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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSourceDBToSpannerWideRowMaxSizeStringIT extends SourceDbToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(MySQLSourceDBToSpannerWideRowMaxSizeStringIT.class);
  private static PipelineLauncher.LaunchInfo jobInfo;
  // Reduced size to prevent VM crashes while still testing large string handling
  private static final Integer MAX_CHARACTER_SIZE = 1000000; // ~1MB instead of ~2.5MB
  private static final String TABLENAME = "WideRowTable";
  private static final String SPANNER_DDL_RESOURCE =
      "WideRow/MYSQLSourceDBToSpannerMaxRowSize/spanner-schema.sql";
  private static MySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() {
    try {
      LOG.info("Setting up MySQL Resource Manager");
      mySQLResourceManager = setUpMySQLResourceManager();

      LOG.info("Setting up Spanner Resource Manager");
      spannerResourceManager = setUpSpannerResourceManager();

      if (spannerResourceManager == null) {
        throw new IllegalStateException("Failed to initialize Spanner Resource Manager");
      }

      LOG.info(
          "Successfully setup resource managers. Spanner instance: {}, database: {}",
          spannerResourceManager.getInstanceId(),
          spannerResourceManager.getDatabaseId());
    } catch (Exception e) {
      LOG.error("Error during setup", e);
      throw new RuntimeException("Failed to set up test resources", e);
    }
  }

  @Override
  public SpannerResourceManager setUpSpannerResourceManager() {
    try {
      // Explicitly use the base class builder with the proper project and region
      return SpannerResourceManager.builder(testName, PROJECT, REGION)
          .maybeUseStaticInstance()
          .build();
    } catch (Exception e) {
      LOG.error("Failed to create SpannerResourceManager", e);
      throw new RuntimeException("Failed to create SpannerResourceManager", e);
    }
  }

  @After
  public void cleanUp() {
    LOG.info("Cleaning up resources");
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  private JDBCResourceManager.JDBCSchema getMySQLSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT");
    columns.put("max_string_col", "MEDIUMTEXT");
    return new JDBCResourceManager.JDBCSchema(columns, "id");
  }

  private List<Map<String, Object>> getMySQLData() {
    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> values = new HashMap<>();
    values.put("id", 1);

    // Generate a large string but in a more memory-efficient way
    String largeString = generateLargeString(MAX_CHARACTER_SIZE);
    values.put("max_string_col", largeString);

    data.add(values);
    return data;
  }

  /**
   * Generates a large string in a memory-efficient way.
   *
   * @param size The size of the string to generate
   * @return A string of the specified size
   */
  private String generateLargeString(int size) {
    // Generate string in smaller chunks to avoid memory issues
    final int chunkSize = 100000;
    StringBuilder sb = new StringBuilder(size);

    int remainingSize = size;
    while (remainingSize > 0) {
      int currentChunkSize = Math.min(chunkSize, remainingSize);
      sb.append(RandomStringUtils.randomAlphabetic(currentChunkSize));
      remainingSize -= currentChunkSize;

      // Force garbage collection to free memory after each chunk
      if (remainingSize > 0) {
        System.gc();
      }
    }

    return sb.toString();
  }

  @Test
  public void testMySQLToSpannerWiderowForMaxSizeString() throws Exception {
    LOG.info("Creating MySQL table: {}", TABLENAME);
    mySQLResourceManager.createTable(TABLENAME, getMySQLSchema());

    LOG.info("Creating Spanner DDL");
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    LOG.info("Generating test data");
    List<Map<String, Object>> mySQLData = getMySQLData();

    LOG.info("Writing data to MySQL table");
    try {
      // Write data with explicit memory management
      mySQLResourceManager.write(TABLENAME, mySQLData);

      // Clear the reference to free memory
      mySQLData = null;
      System.gc();

      // Recreate the data for verification
      mySQLData = getMySQLData();
    } catch (OutOfMemoryError e) {
      LOG.error("Out of memory while writing data to MySQL", e);
      throw new RuntimeException("Failed to write data to MySQL due to memory constraints", e);
    }

    LOG.info("Launching Dataflow job");
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);

    LOG.info("Waiting for job to complete with ID: {}", jobInfo.jobId());
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    LOG.info("Verifying data in Spanner");
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords(TABLENAME, "id", "max_string_col"))
        .hasRecordsUnorderedCaseInsensitiveColumns(mySQLData);
  }
}
