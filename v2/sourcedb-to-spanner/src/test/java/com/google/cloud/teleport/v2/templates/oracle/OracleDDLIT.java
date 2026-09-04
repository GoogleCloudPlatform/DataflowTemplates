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

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a migration with DDL
 * changes to schema. Changes include Index changes, Primary key transformations and Generated
 * columns migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleDDLIT extends SourceDbToSpannerITBase {
  private PipelineLauncher.LaunchInfo jobInfo;

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String SESSION_FILE_RESOURCE = "oracle/OracleDDLIT/oracle-session.json";
  private static final String ORACLE_DDL_RESOURCE = "oracle/OracleDDLIT/oracle-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/OracleDDLIT/oracle-google_standard_sql-spanner-schema.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    oracleResourceManager = SharedOracleBulkITContainer.getInstance();
    spannerResourceManager = setUpSpannerResourceManager();
    testUsername = setupOracleIsolatedUser(oracleResourceManager);
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void ddlModificationTest() throws Exception {
    loadOracleSQLFileResource(oracleResourceManager, ORACLE_DDL_RESOURCE, testUsername);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Map<String, String> jobParams = new HashMap<>();
    jobParams.put("jdbcDriverJars", oracleDriverGCSPath());
    jobParams.put("jdbcDriverClassName", "oracle.jdbc.OracleDriver");

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            SESSION_FILE_RESOURCE,
            "mapper",
            oracleResourceManager,
            spannerResourceManager,
            jobParams,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> companyOracle =
        runIsolatedSQLQuery(
            oracleResourceManager, testUsername, "SELECT COMPANY_ID, COMPANY_NAME FROM COMPANY");
    ImmutableList<Struct> companySpanner =
        spannerResourceManager.readTableRecords("COMPANY", "COMPANY_ID", "COMPANY_NAME");

    SpannerAsserts.assertThatStructs(companySpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(companyOracle);

    List<Map<String, Object>> employeeOracle =
        runIsolatedSQLQuery(
            oracleResourceManager,
            testUsername,
            "SELECT EMPLOYEE_ID, COMPANY_ID, EMPLOYEE_NAME, EMPLOYEE_ADDRESS FROM EMPLOYEE");
    ImmutableList<Struct> employeeSpanner =
        spannerResourceManager.readTableRecords(
            "EMPLOYEE", "EMPLOYEE_ID", "COMPANY_ID", "EMPLOYEE_NAME", "EMPLOYEE_ADDRESS");

    SpannerAsserts.assertThatStructs(employeeSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(employeeOracle);

    ImmutableList<Struct> employeeAttribute =
        spannerResourceManager.readTableRecords(
            "EMPLOYEE_ATTRIBUTE", "EMPLOYEE_ID", "ATTRIBUTE_NAME", "VALUE");

    SpannerAsserts.assertThatStructs(employeeAttribute).hasRows(4); // Supports composite keys

    ImmutableList<Struct> vendor =
        spannerResourceManager.readTableRecords("VENDOR", "VENDOR_ID", "FULL_NAME");

    SpannerAsserts.assertThatStructs(vendor).hasRows(3);
    SpannerAsserts.assertThatStructs(vendor)
        .hasRecordsUnorderedCaseInsensitiveColumns(
            runIsolatedSQLQuery(
                oracleResourceManager, testUsername, "SELECT VENDOR_ID, FULL_NAME FROM VENDOR"));
  }
}
