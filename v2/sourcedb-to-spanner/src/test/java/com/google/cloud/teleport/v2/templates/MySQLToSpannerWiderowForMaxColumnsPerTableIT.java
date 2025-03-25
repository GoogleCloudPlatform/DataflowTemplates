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
import java.util.StringJoiner;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for testing MySQL to Spanner migration with wide tables (1024 columns). */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpannerWiderowForMaxColumnsPerTableIT extends SourceDbToSpannerITBase {

  private PipelineLauncher.LaunchInfo jobInfo;
  private MySQLResourceManager mySQLResourceManager;
  private SpannerResourceManager spannerResourceManager;

  // Constants
  private static final Integer NUM_COLUMNS = 1016;
  private static final String TABLENAME = "WiderowTable";
  private static final int MAX_ALLOWED_PACKET = 128 * 1024 * 1024; // 128 MiB

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  private void increasePacketSize() {
    String allowedGlobalPacket = "SET GLOBAL max_allowed_packet = " + MAX_ALLOWED_PACKET;
    mySQLResourceManager.runSQLUpdate(allowedGlobalPacket);
  }

  /**
   * Builds the MySQL CREATE TABLE statement with the specified number of columns.
   *
   * @return MySQL DDL statement
   */
  private String getMySQLDDL() {
    // Use StringJoiner for more efficient string concatenation
    StringJoiner columnsJoiner = new StringJoiner(", ");

    for (int i = 0; i < NUM_COLUMNS; i++) {
      columnsJoiner.add("col" + i + " INT");
    }

    return String.format(
        "CREATE TABLE %s (id INT NOT NULL, %s, PRIMARY KEY (id))",
        TABLENAME, columnsJoiner.toString());
  }

  /**
   * Builds the Spanner CREATE TABLE statement with the specified number of columns.
   *
   * @return Spanner DDL statement
   */
  private String getSpannerDDL() {
    // Use StringJoiner for more efficient string concatenation
    StringJoiner columnsJoiner = new StringJoiner(", ");

    for (int i = 0; i < NUM_COLUMNS; i++) {
      columnsJoiner.add("col" + i + " INT64");
    }

    return String.format(
        "CREATE TABLE %s (id INT64 NOT NULL, %s) PRIMARY KEY (id)",
        TABLENAME, columnsJoiner.toString());
  }

  /**
   * Builds the MySQL INSERT statement with values for each column.
   *
   * @return MySQL INSERT statement
   */
  private String getMySQLInsertStatement() {
    // Use StringJoiner for more efficient string concatenation
    StringJoiner columnsJoiner = new StringJoiner(", ");
    StringJoiner valuesJoiner = new StringJoiner(", ");

    columnsJoiner.add("id");
    valuesJoiner.add("1");

    for (int i = 0; i < NUM_COLUMNS; i++) {
      columnsJoiner.add("col" + i);
      valuesJoiner.add(String.valueOf(i));
    }

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)",
        TABLENAME, columnsJoiner.toString(), valuesJoiner.toString());
  }

  /**
   * Creates a list of column names for verification queries.
   *
   * @return List of column names
   */
  private List<String> getColumnsList() {
    List<String> columns = new ArrayList<>();
    columns.add("id");
    for (int i = 0; i < NUM_COLUMNS; i++) {
      columns.add("col" + i);
    }
    return columns;
  }

  @Test
  public void testMaxColumnsPerTable() throws Exception {
    // Increase MySQL packet size to handle large statements
    increasePacketSize();

    // Create table in MySQL
    loadSQLToJdbcResourceManager(mySQLResourceManager, getMySQLDDL());

    // Insert test data in MySQL
    loadSQLToJdbcResourceManager(mySQLResourceManager, getMySQLInsertStatement());

    // Create matching table in Spanner
    spannerResourceManager.executeDdlStatement(getSpannerDDL());

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
    List<String> expectedColumns = getColumnsList();
    ImmutableList<Struct> wideRowData =
        spannerResourceManager.readTableRecords(TABLENAME, expectedColumns);

    // Verify row count
    SpannerAsserts.assertThatStructs(wideRowData).hasRows(1);
  }
}
