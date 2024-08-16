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
package com.google.cloud.teleport.v2.templates.postgresql;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SourceDbToSpanner;
import com.google.cloud.teleport.v2.templates.SourceDbToSpannerITBase;
import java.io.IOException;
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
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a basic migration on
 * a simple schema.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class SourceDbToSpannerSimpleIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static PostgresResourceManager postgreSQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String SPANNER_DDL_RESOURCE = "SourceDbToSpannerSimpleIT/spanner-schema.sql";

  private static final String TABLE1 = "simple_table";

  private static final String TABLE2 = "string_table";
  private static final String ID = "id";

  private static final String NAME = "name";

  private JDBCResourceManager.JDBCSchema getPostgreSQLSchema(String idCol) {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ID, "INT8 NOT NULL");
    columns.put(NAME, "TEXT");
    return new JDBCResourceManager.JDBCSchema(columns, idCol);
  }

  private List<Map<String, Object>> getPostgreSQLData() {
    List<Map<String, Object>> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put(ID, i);
      values.put(NAME, RandomStringUtils.randomAlphabetic(10));
      data.add(values);
    }
    return data;
  }

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() {
    postgreSQLResourceManager = setUpPostgreSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, postgreSQLResourceManager);
  }

  @Test
  public void simpleTest() throws IOException {
    List<Map<String, Object>> postgreSQLData = getPostgreSQLData();
    postgreSQLResourceManager.createTable(TABLE1, getPostgreSQLSchema(ID));
    postgreSQLResourceManager.createTable(TABLE2, getPostgreSQLSchema(NAME));
    postgreSQLResourceManager.write(TABLE1, postgreSQLData);
    postgreSQLResourceManager.write(TABLE2, postgreSQLData);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgreSQLResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();
    SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(TABLE1, ID, NAME))
        .hasRecordsUnorderedCaseInsensitiveColumns(postgreSQLData);
    SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(TABLE2, ID, NAME))
        .hasRecordsUnorderedCaseInsensitiveColumns(postgreSQLData);
  }
}
