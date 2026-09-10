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
package com.google.cloud.teleport.v2.templates.oracle;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.experimental.categories.Category({
  com.google.cloud.teleport.metadata.TemplateIntegrationTest.class
})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleNamespaceIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(OracleNamespaceIT.class);
  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    spannerResourceManager =
        SpannerResourceManager.builder("test-span-" + testName, PROJECT, REGION).build();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  @Test
  public void testOracleNamespace() throws java.io.IOException, InterruptedException {
    String namespace = testUsername;
    try (Connection connection =
            DriverManager.getConnection(
                oracleResourceManager.getUri(),
                oracleResourceManager.getUsername(),
                oracleResourceManager.getPassword());
        Statement stmt = connection.createStatement()) {
      if (!"SYSTEM".equalsIgnoreCase(testUsername)) {
        stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + testUsername);
      }

      try {
        stmt.execute(
            "CREATE TABLE \"singers\" ( \"singer_id\" NUMBER PRIMARY KEY, \"first_name\" VARCHAR2(1024) )");
        stmt.execute(
            "CREATE TABLE \"albums\" ( \"singer_id\" NUMBER NOT NULL, \"album_id\" NUMBER NOT NULL, \"album_serial_number\" NUMBER, PRIMARY KEY (\"singer_id\", \"album_id\"), CONSTRAINT \"album_id_fk\" FOREIGN KEY (\"album_id\") REFERENCES \"singers\" (\"singer_id\") )");
        stmt.execute(
            "CREATE INDEX \"album_serial_number_idx\" ON \"albums\" (\"album_serial_number\")");
        stmt.execute(
            "INSERT INTO \"singers\" (\"singer_id\", \"first_name\") VALUES (1, 'Singer 1')");
        stmt.execute(
            "INSERT INTO \"singers\" (\"singer_id\", \"first_name\") VALUES (2, 'Singer 2')");
        stmt.execute(
            "INSERT INTO \"albums\" (\"singer_id\", \"album_id\", \"album_serial_number\") VALUES (1, 1, 10)");
        stmt.execute(
            "INSERT INTO \"albums\" (\"singer_id\", \"album_id\", \"album_serial_number\") VALUES (1, 2, 11)");
        stmt.execute(
            "INSERT INTO \"albums\" (\"singer_id\", \"album_id\", \"album_serial_number\") VALUES (2, 2, 20)");
      } catch (Exception ex) {
        LOG.error("Failed to run DDL/DML: " + ex.getMessage(), ex);
        throw ex;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    createSpannerDDL(
        spannerResourceManager,
        "oracle/OracleNamespaceIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql");
    try {
      Map<String, String> jobParams = new HashMap<>();
      jobParams.put("namespace", namespace);
      PipelineLauncher.LaunchInfo jobInfo =
          launchDataflowJob(
              getClass().getSimpleName(),
              null,
              null,
              oracleResourceManager,
              spannerResourceManager,
              jobParams,
              null);
      PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
      assertThatResult(result).isLaunchFinished();

      List<Map<String, Object>> singersData = new ArrayList<>();
      Map<String, Object> row1 = new HashMap<>();
      row1.put("singer_id", 1);
      row1.put("first_name", "Singer 1");
      Map<String, Object> row2 = new HashMap<>();
      row2.put("singer_id", 2);
      row2.put("first_name", "Singer 2");
      singersData.add(row1);
      singersData.add(row2);

      SpannerAsserts.assertThatStructs(
              spannerResourceManager.readTableRecords("singers", "singer_id", "first_name"))
          .hasRecordsUnorderedCaseInsensitiveColumns(singersData);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void cleanUp() {
    if (oracleResourceManager != null) {
      // oracleResourceManager is shared; skipped cleanup
    }
    if (spannerResourceManager != null) {
      spannerResourceManager.cleanupAll();
    }
  }
}
