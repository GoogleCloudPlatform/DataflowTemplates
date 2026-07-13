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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSourceDbToSpanner4ByteStringPKIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static CloudMySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String TABLE = "table4bytepk";
  private static final String ID = "id";
  private static final String DESCRIPTION = "description";
  private static final String SPANNER_DDL_RESOURCE =
      "SourceDbToSpanner4ByteStringPKIT/spanner-schema.sql";

  private JDBCResourceManager.JDBCSchema getMySQLSchema() {
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ID, "VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL");
    columns.put(DESCRIPTION, "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, ID);
  }

  private List<Map<String, Object>> getMySQLData() {
    List<Map<String, Object>> data = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    row1.put(ID, "\uD83D\uDE00");
    row1.put(DESCRIPTION, "Grinning Face");
    data.add(row1);

    Map<String, Object> row2 = new HashMap<>();
    row2.put(ID, "\uD83D\uDE01");
    row2.put(DESCRIPTION, "Beaming Face with Smiling Eyes");
    data.add(row2);

    Map<String, Object> row3 = new HashMap<>();
    row3.put(ID, "\uD83D\uDE02");
    row3.put(DESCRIPTION, "Face with Tears of Joy");
    data.add(row3);

    return data;
  }

  @Before
  public void setUp() {
    mySQLResourceManager = setUpCloudMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  @Test
  public void testMySqlToSpanner() throws IOException {
    List<Map<String, Object>> mySQLData = getMySQLData();
    mySQLResourceManager.createTable(TABLE, getMySQLSchema());
    mySQLResourceManager.write(TABLE, mySQLData);

    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

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
            spannerResourceManager.readTableRecords(TABLE, ID, DESCRIPTION))
        .hasRecordsUnorderedCaseInsensitiveColumns(mySQLData);
  }
}
