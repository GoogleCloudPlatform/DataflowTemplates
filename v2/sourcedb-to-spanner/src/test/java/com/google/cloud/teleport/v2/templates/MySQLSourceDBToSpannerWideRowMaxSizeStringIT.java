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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("This test is failing in the CI pipeline")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSourceDBToSpannerWideRowMaxSizeStringIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static final Integer MAX_CHARACTER_SIZE = 2621440;
  private static final String TABLENAME = "WiderowTable";
  private static MySQLResourceManager mySQLResourceManager;
  private static SpannerResourceManager spannerResourceManager;

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
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
    values.put("max_string_col", RandomStringUtils.randomAlphabetic(MAX_CHARACTER_SIZE));
    data.add(values);
    return data;
  }

  private String getSpannerSchema() {
    StringBuilder schema = new StringBuilder();
    schema.append("CREATE TABLE WiderowTable (");
    schema.append("id INT64 NOT NULL,");
    schema.append("max_string_col STRING(MAX),");
    schema.append(") PRIMARY KEY (id)");
    return schema.toString();
  }

  @Test
  public void testMySQLToSpannerWiderowForMaxSizeString() throws Exception {
    mySQLResourceManager.createTable(TABLENAME, getMySQLSchema());
    mySQLResourceManager.write(TABLENAME, getMySQLData());
    createSpannerDDL(spannerResourceManager, getSpannerSchema());
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords(TABLENAME, "id", "max_string_col"))
        .hasRecordsUnorderedCaseInsensitiveColumns(getMySQLData());
  }
}
