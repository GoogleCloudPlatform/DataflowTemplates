/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a basic migration on
 * a simple schema.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleSourceDbToSpannerSimpleIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(OracleSourceDbToSpannerSimpleIT.class);
  private static HashSet<OracleSourceDbToSpannerSimpleIT> testInstances = new HashSet<>();
  private PipelineLauncher.LaunchInfo jobInfo;

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleSourceDbToSpannerSimpleIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

  private static final String TABLE1 = "\"SimpleTable\"";

  private static final String TABLE2 = "\"StringTable\"";

  private JDBCResourceManager.JDBCSchema getOracleSchema(String idCol) {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("\"id\"", "INTEGER NOT NULL");
    columns.put("\"name\"", "VARCHAR2(200)");
    return new JDBCResourceManager.JDBCSchema(columns, idCol);
  }

  private List<Map<String, Object>> getOracleData() {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put("\"id\"", i);
      values.put("\"name\"", RandomStringUtils.randomAlphabetic(10));
      data.add(values);
    }
    return data;
  }

  private List<Map<String, Object>> getSpannerExpectedData(List<Map<String, Object>> oracleData) {
    List<Map<String, Object>> expected = new ArrayList<>();
    for (Map<String, Object> row : oracleData) {
      Map<String, Object> unquoted = new HashMap<>();
      unquoted.put("id", row.get("\"id\""));
      unquoted.put("name", row.get("\"name\""));
      expected.add(unquoted);
    }
    return expected;
  }

  @Before
  public void setUp() {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    spannerResourceManager = setUpSpannerResourceManager();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  private void createAndWriteTable(String tableName, String pkCol, List<Map<String, Object>> data)
      throws Exception {
    try (java.sql.Connection c =
            java.sql.DriverManager.getConnection(
                oracleResourceManager.getUri(), testUsername, "password");
        java.sql.Statement s = c.createStatement()) {
      s.execute(
          "CREATE TABLE "
              + tableName
              + " (\"id\" INTEGER NOT NULL, \"name\" VARCHAR2(200), PRIMARY KEY ("
              + pkCol
              + "))");
      for (Map<String, Object> r : data) {
        s.execute(
            "INSERT INTO "
                + tableName
                + " (\"id\", \"name\") VALUES ("
                + r.get("\"id\"")
                + ", '"
                + r.get("\"name\"")
                + "')");
      }
    }
  }

  private void writeTable(String tableName, List<Map<String, Object>> data) throws Exception {
    try (java.sql.Connection c =
            java.sql.DriverManager.getConnection(
                oracleResourceManager.getUri(), testUsername, "password");
        java.sql.Statement s = c.createStatement()) {
      for (Map<String, Object> r : data) {
        s.execute(
            "INSERT INTO "
                + tableName
                + " (\"id\", \"name\") VALUES ("
                + r.get("\"id\"")
                + ", '"
                + r.get("\"name\"")
                + "')");
      }
    }
  }

  private void truncateTableAsUser(String tableName) throws Exception {
    try (java.sql.Connection c =
            java.sql.DriverManager.getConnection(
                oracleResourceManager.getUri(), testUsername, "password");
        java.sql.Statement s = c.createStatement()) {
      s.execute("TRUNCATE TABLE " + tableName);
    }
  }

  @Test
  public void testOracleToSpanner() throws Exception {
    List<Map<String, Object>> oracleData = getOracleData();
    createAndWriteTable(TABLE1, "\"id\"", oracleData);
    createAndWriteTable(TABLE2, "\"name\"", oracleData);
    // deleted write TABLE1
    // deleted write TABLE2
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    List<Map<String, Object>> expectedData = getSpannerExpectedData(oracleData);
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("SimpleTable", "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedData);
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("StringTable", "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedData);

    truncateTableAsUser(TABLE2);
    List<Map<String, Object>> updatedOracleData = getOracleData();
    List<Map<String, Object>> expectedUpdatedData = getSpannerExpectedData(updatedOracleData);
    assertThat(updatedOracleData).isNotEqualTo(oracleData);
    writeTable(TABLE2, updatedOracleData);

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            ImmutableMap.of("insertOnlyModeForSpannerMutations", "true"),
            null);
    PipelineOperator.Result resultInsertsOnly =
        pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(resultInsertsOnly).isLaunchFinished();
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("SimpleTable", "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedData);
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("StringTable", "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedData);

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            ImmutableMap.of("insertOnlyModeForSpannerMutations", "false"),
            null);
    PipelineOperator.Result resultUpserts = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(resultUpserts).isLaunchFinished();
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("SimpleTable", "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedData);
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords("StringTable", "id", "name"))
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedUpdatedData);
  }
}
