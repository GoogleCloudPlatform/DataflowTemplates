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
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.io.Resources;

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests schema mapping using
 * a schema overrides file with a common schema.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLFileOverridesSchemaMapperIT extends SourceDbToSpannerITBase {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLFileOverridesSchemaMapperIT.class);
  private static final HashSet<MySQLFileOverridesSchemaMapperIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  public static GcsResourceManager gcsResourceManager;

  // Common SQL and override file resources
  private static final String MYSQL_DDL_RESOURCE = "SchemaMapperIT/mysql-overrides-src.sql";
  private static final String SPANNER_DDL_RESOURCE = "SchemaMapperIT/spanner-overrides-target.sql";
  private static final String SCHEMA_OVERRIDE_FILE_RESOURCE = "SchemaMapperIT/file-overrides.json";
  private static final String SCHEMA_OVERRIDE_GCS_PREFIX = "MySQLFileOverridesCommonIT";

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   */
  @Before
  public void setUp() throws Exception {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
    gcsResourceManager = setUpSpannerITGcsResourceManager();

    gcsResourceManager.uploadArtifact(
        SCHEMA_OVERRIDE_GCS_PREFIX + "/file-overrides.json",
        Resources.getResource(SCHEMA_OVERRIDE_FILE_RESOURCE).getPath());
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager, mySQLResourceManager, gcsResourceManager);
  }

  @Test
  public void testMigrationWithFileOverridesAndCommonSchema() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DDL_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    Map<String, String> jobParameters = new HashMap<>();
    jobParameters.put(
        "schemaOverridesFilePath",
        getGcsPath(SCHEMA_OVERRIDE_GCS_PREFIX + "/file-overrides.json", gcsResourceManager));

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null, // No session file
            null,
            mySQLResourceManager,
            spannerResourceManager,
            jobParameters,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));

    // Assertions for source_table1 -> Target_Table_1
    List<Map<String, Object>> sourceTable1Data =
        mySQLResourceManager.runSQLQuery("SELECT id_col1, name_col1, data_col1 FROM source_table1");

    // Prepare expected data for Spanner with overridden column names for comparison
    List<Map<String, Object>> expectedSpannerTable1 = new java.util.ArrayList<>();
    for (Map<String, Object> row : sourceTable1Data) {
      Map<String, Object> newRow = new HashMap<>();
      newRow.put("id_col1", row.get("id_col1"));
      newRow.put("Target_Name_Col_1", row.get("name_col1")); // Overridden name
      newRow.put("data_col1", row.get("data_col1"));
      expectedSpannerTable1.add(newRow);
    }

    ImmutableList<Struct> spannerTable1 =
        spannerResourceManager.readTableRecords(
            "Target_Table_1", "id_col1", "Target_Name_Col_1", "data_col1");
    SpannerAsserts.assertThatStructs(spannerTable1)
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedSpannerTable1);
    SpannerAsserts.assertThatStructs(spannerTable1).hasRows(sourceTable1Data.size());

    // Assertions for source_table2 (table not renamed, one column renamed)
    List<Map<String, Object>> sourceTable2Data =
        mySQLResourceManager.runSQLQuery(
            "SELECT key_col2, category_col2, value_col2 FROM source_table2");

    List<Map<String, Object>> expectedSpannerTable2 = new java.util.ArrayList<>();
    for (Map<String, Object> row : sourceTable2Data) {
      Map<String, Object> newRow = new HashMap<>();
      newRow.put("key_col2", row.get("key_col2"));
      newRow.put("Target_Category_Col_2", row.get("category_col2")); // Overridden name
      newRow.put("value_col2", row.get("value_col2"));
      expectedSpannerTable2.add(newRow);
    }

    ImmutableList<Struct> spannerTable2 =
        spannerResourceManager.readTableRecords(
            "source_table2", "key_col2", "Target_Category_Col_2", "value_col2");
    SpannerAsserts.assertThatStructs(spannerTable2)
        .hasRecordsUnorderedCaseInsensitiveColumns(expectedSpannerTable2);
    SpannerAsserts.assertThatStructs(spannerTable2).hasRows(sourceTable2Data.size());
  }
}
