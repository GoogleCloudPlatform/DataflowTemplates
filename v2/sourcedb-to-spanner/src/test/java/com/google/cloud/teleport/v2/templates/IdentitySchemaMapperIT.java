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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
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
import org.apache.beam.it.jdbc.MySQLResourceManager;
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
public class IdentitySchemaMapperIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(IdentitySchemaMapperIT.class);
  private static final HashSet<IdentitySchemaMapperIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String MYSQL_DDL_RESOURCE =
      "SingleShardWithTransformation/company-mysql-schema.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "SingleShardWithTransformation/company-spanner-schema.sql";

  private static final String SPANNER_DDL_WITH_TRANSFORMATION_RESOURCE =
      "SingleShardWithTransformation/company-spanner-schema-with-transformation.sql";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void simpleTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> companyMySQL = mySQLResourceManager.readTable("company");
    ImmutableList<Struct> companySpanner =
        spannerResourceManager.readTableRecords(
            "company", "company_id", "company_name", "created_on");

    SpannerAsserts.assertThatStructs(companySpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(companyMySQL);

    List<Map<String, Object>> employeeMySQL = mySQLResourceManager.readTable("employee");
    ImmutableList<Struct> employeeSpanner =
        spannerResourceManager.readTableRecords(
            "employee",
            "employee_id",
            "company_id",
            "employee_name",
            "employee_address",
            "created_on");

    SpannerAsserts.assertThatStructs(employeeSpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(employeeMySQL);

    ImmutableList<Struct> employeeAttribute =
        spannerResourceManager.readTableRecords(
            "employee_attribute", "employee_id", "attribute_name", "value", "updated_on");

    SpannerAsserts.assertThatStructs(employeeAttribute).hasRows(0);
  }

  @Test
  public void autoInferSchemaWithTableFilter() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_WITH_TRANSFORMATION_RESOURCE);

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("tables", "company");
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            jobParameters);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    List<Map<String, Object>> companyMySQL =
        mySQLResourceManager.runSQLQuery(
            "SELECT company_id, company_name as company_name_sp, created_on FROM company");
    ImmutableList<Struct> companySpanner =
        spannerResourceManager.readTableRecords(
            "company_sp", "company_id", "company_name_sp", "created_on");

    SpannerAsserts.assertThatStructs(companySpanner)
        .hasRecordsUnorderedCaseInsensitiveColumns(companyMySQL);
    SpannerAsserts.assertThatStructs(companySpanner).hasRows(companyMySQL.size());

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
