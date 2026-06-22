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
 * An integration test to reproduce the parallel read partition issues with specific PostgreSQL data types.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class PostgreSQLPartitioningTypesIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLPartitioningTypesIT.class);

  public static PostgresResourceManager postgreSQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String POSTGRESQL_DDL_RESOURCE = "PartitioningTypesIT/postgresql-partitioning-types.sql";
  private static final String SPANNER_DDL_RESOURCE = "PartitioningTypesIT/postgresql-spanner-schema.sql";

  @Before
  public void setUp() {
    postgreSQLResourceManager = setUpPostgreSQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, postgreSQLResourceManager);
  }

  @Test
  public void testPartitioningWithNonIntegerKeys() throws Exception {
    loadSQLFileResource(postgreSQLResourceManager, POSTGRESQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Map<String, String> jobParameters = new HashMap<>();
    // Setting numPartitions > 1 forces Dataflow to compute min/max boundaries
    // and split the read into multiple partitions.
    jobParameters.put("numPartitions", "10");

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
        pipelineOperator().waitUntilDone(createConfig(jobInfo, Duration.ofMinutes(15L)));
        
    assertThatResult(result).isLaunchFinished();

    // Verify if all the data made it to Spanner. If boundaries were rounded/truncated, 
    // some rows with fractional parts might be skipped.
    Map<String, List<Map<String, Object>>> expectedData = getExpectedData();
    for (Map.Entry<String, List<Map<String, Object>>> entry : expectedData.entrySet()) {
      String tableName = entry.getKey();
      LOG.info("Asserting table: {}", tableName);

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
    result.put("t_float", createRows(1.5, 2.5, 3.5));
    result.put("t_double", createRows(1.5, 2.5, 3.5));
    // For Spanner numeric, we might just compare as string or wait to see what comes back.
    // For simplicity of verification, let's just make sure 3 rows exist and check contents as strings if needed.
    // But truthmatchers handles Number types mostly fine. We can use String representations to be safe for dates/decimals.
    return result;
  }

  private List<Map<String, Object>> createRows(Object... ids) {
    List<Map<String, Object>> rows = new ArrayList<>();
    char colChar = 'a';
    for (Object id : ids) {
      Map<String, Object> row = new HashMap<>();
      row.put("id", id);
      row.put("col", String.valueOf(colChar));
      rows.add(row);
      colChar++;
    }
    return rows;
  }
}
