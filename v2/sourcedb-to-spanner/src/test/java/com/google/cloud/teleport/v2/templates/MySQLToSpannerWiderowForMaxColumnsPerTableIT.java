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

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for testing MySQL to Spanner migration with wide tables (1016 columns). */
// @Ignore("test is completed")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpannerWiderowForMaxColumnsPerTableIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;
  private CloudMySQLResourceManager mySQLResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String TABLENAME = "WiderowTable";

  @Before
  public void setUp() {
    mySQLResourceManager = setUpCloudMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  /**
   * Builds the MySQL CREATE TABLE statement with the specified number of columns.
   *
   * @return MySQL DDL statement
   */
  private String getMySQLDDL(int maxColumns) {
    StringBuilder mysqlDDL = new StringBuilder();
    for (int i = 1; i <= maxColumns; i++) {
      mysqlDDL.append("col" + i + " INT");
      if (i != maxColumns) {
        mysqlDDL.append(", ");
      }
    }
    return String.format(
        "CREATE TABLE %s (id INT NOT NULL, %s, PRIMARY KEY (id))", TABLENAME, mysqlDDL);
  }

  /**
   * Builds the Spanner CREATE TABLE statement with the specified number of columns.
   *
   * @return Spanner DDL statement
   */
  private String getSpannerDDL(int maxColumns) {
    StringBuilder spannerDDL = new StringBuilder();
    for (int i = 1; i <= maxColumns; i++) {
      spannerDDL.append("col" + i + " INT64");
      if (i != maxColumns) {
        spannerDDL.append(", ");
      }
    }
    return String.format(
        "CREATE TABLE %s (id INT64 NOT NULL, %s) PRIMARY KEY (id)", TABLENAME, spannerDDL);
  }

  /**
   * Builds the MySQL INSERT statement with values for each column.
   *
   * @return MySQL INSERT statement
   */
  private String getMySQLInsertStatement(int maxColumns) {
    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    columns.append("id");
    values.append("1");

    for (int i = 1; i <= maxColumns; i++) {
      columns.append("col" + i);
      values.append(i);
      if (i != maxColumns) {
        columns.append(", ");
        values.append(", ");
      }
    }

    return String.format("INSERT INTO %s (%s) VALUES (%s)", TABLENAME, columns, values);
  }

  /**
   * Creates a list of column names for verification queries.
   *
   * @return List of column names
   */
  private List<String> getColumnsList(int maxColumns) {
    List<String> columns = new ArrayList<>();
    columns.add("id");
    for (int i = 1; i <= maxColumns; i++) {
      columns.add("col" + i);
    }
    return columns;
  }

  @Test
  public void testMaxColumnsPerTable() throws Exception {
    // Limits to the max columns supported by MySQL/CloudSQL (1017 columns total, including 'id')
    int maxColumns = 1016;

    // Create table in MySQL
    loadSQLToJdbcResourceManager(mySQLResourceManager, getMySQLDDL(maxColumns));

    // Insert test data in MySQL
    loadSQLToJdbcResourceManager(mySQLResourceManager, getMySQLInsertStatement(maxColumns));

    // Create matching table in Spanner
    spannerResourceManager.executeDdlStatement(getSpannerDDL(maxColumns));

    // Launch the migration job
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);

    // Wait for job completion
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    // Verify data in Spanner
    List<String> expectedColumns = getColumnsList(maxColumns);
    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords(TABLENAME, expectedColumns);

    // Verify row count
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }

  @Test
  public void testExceedingMaxColumnsPerTable() throws Exception {
    int maxColumns = 1025;
    try {
      spannerResourceManager.executeDdlStatement(getSpannerDDL(maxColumns));
      Assert.fail(
          "Expected exception due to exceeding maximum columns, but no exception was thrown.");
    } catch (Exception e) {
      // Assert that the exception message contains the expected text indicating failure.
      Assert.assertTrue(
          "Exception should mention column limitation",
          e.getMessage().contains("Failed to execute statement"));
    }
  }
}
