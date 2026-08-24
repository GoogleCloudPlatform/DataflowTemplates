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

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a basic migration on
 * a simple schema.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class OracleIdentitySchemaMapperWithTransformationIT extends SourceDbToSpannerITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(OracleIdentitySchemaMapperWithTransformationIT.class);
  private static final HashSet<OracleIdentitySchemaMapperWithTransformationIT> testInstances =
      new HashSet<>();
  private PipelineLauncher.LaunchInfo jobInfo;

  private org.apache.beam.it.jdbc.JDBCResourceManager oracleResourceManager;
  private SpannerResourceManager spannerResourceManager;

  private static final String ORACLE_DDL_RESOURCE =
      "oracle/OracleIdentitySchemaMapperWithTransformationIT/oracle-schema.sql";

  private static final String SPANNER_DDL_WITH_TRANSFORMATION_RESOURCE =
      "oracle/OracleIdentitySchemaMapperWithTransformationIT/oracle-GOOGLE_STANDARD_SQL-spanner-schema.sql";

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

  @Test
  public void autoInferSchemaWithTableFilter() throws Exception {
    loadOracleSQLFileResource(oracleResourceManager, ORACLE_DDL_RESOURCE, testUsername);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_WITH_TRANSFORMATION_RESOURCE);

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("namespace", testUsername);
    jobParameters.put("tables", "company");
    jobParameters.put("jdbcDriverJars", oracleDriverGCSPath());
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            oracleResourceManager,
            spannerResourceManager,
            jobParameters,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> companyOracle =
        runIsolatedSQLQuery(
            oracleResourceManager,
            testUsername,
            "SELECT \"company_id\", \"company_name\" FROM \"company\"");

    // We expect the Spanner output structs to have case preserved field names based on the Spanner
    // DDL, which are lowercased "company_id", etc.
    // Unquote the keys to match Spanner.
    List<Map<String, Object>> unquotedCompanyOracle =
        companyOracle.stream()
            .map(
                map -> {
                  Map<String, Object> newMap = new HashMap<>();
                  map.forEach((k, v) -> newMap.put(k.replace("\"", ""), v));
                  return newMap;
                })
            .collect(java.util.stream.Collectors.toList());

    ImmutableList<Struct> companySpanner =
        spannerResourceManager.readTableRecords("company", "company_id", "company_name");

    SpannerAsserts.assertThatStructs(companySpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(unquotedCompanyOracle);
    SpannerAsserts.assertThatStructs(companySpanner).hasRows(companyOracle.size());

    ImmutableList<Struct> employeeSpanner =
        spannerResourceManager.readTableRecords(
            "employee_sp",
            "employee_id",
            "company_id",
            "employee_name",
            "employee_address_sp",
            "created_on");
    SpannerAsserts.assertThatStructs(employeeSpanner).hasRows(0); // As the table is filtered

    ImmutableList<Struct> employeeAttribute =
        spannerResourceManager.readTableRecords(
            "employee_attribute", "employee_id", "attribute_name", "value", "updated_on");
    SpannerAsserts.assertThatStructs(employeeAttribute).hasRows(0); // As the table is filtered
  }
}
