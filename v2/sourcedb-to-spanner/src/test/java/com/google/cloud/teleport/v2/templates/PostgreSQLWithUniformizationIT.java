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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests all PostgreSQL data
 * types migration.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLWithUniformizationIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLWithUniformizationIT.class);

  public static PostgresResourceManager postgreSQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String POSTGRESQL_DDL_RESOURCE = "DataTypesIT/postgresql-few-data-types.sql";
  private static final String SPANNER_DDL_RESOURCE =
      "DataTypesIT/postgresql-spanner-schema-few-data-types.sql";

  /** Setup resource managers. */
  @Before
  public void setUp() {
    postgreSQLResourceManager = setUpPostgreSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job, all the resources, and resource managers. */
  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, postgreSQLResourceManager);
  }

  @Test
  public void withUniformizationTest() throws Exception {
    loadSQLFileResource(postgreSQLResourceManager, POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    System.setProperty("numWorkers", "20");
    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put("numPartitions", "100");
    jobParameters.put("uniformizationStageCountHint", "-1");
    PipelineLauncher.LaunchInfo jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            postgreSQLResourceManager,
            spannerResourceManager,
            jobParameters,
            null);
    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(30L)));
    assertThatResult(result).isLaunchFinished();

    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String type = entry.getKey();
      String tableName = String.format("t_%s", type);
      LOG.info("Asserting type:{}", type);

      List<Struct> rows = spannerResourceManager.readTableRecords(tableName, "id", "col");
      for (Struct row : rows) {
        LOG.info("Found row: {}", row);
      }
      SpannerAsserts.assertThatStructs(rows)
          .hasRecordsUnorderedCaseInsensitiveColumns(entry.getValue());
    }
  }

  private Map<String, List<Map<String, Object>>> getExpectedData() {
    HashMap<String, List<Map<String, Object>>> result = new HashMap<>();
    result.put("bigint", createRows("-9223372036854775808", "9223372036854775807", "42", "NULL"));
    result.put("bigserial", createRows("-9223372036854775808", "9223372036854775807", "42"));
    return result;
  }

  private List<Map<String, Object>> createRows(Object... values) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", i + 1);
      row.put("col", values[i]);
      rows.add(row);
    }
    return rows;
  }
}
