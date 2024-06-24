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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link GCSToSourceDb} Flex template without launching reader job. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(GCSToSourceDb.class)
@RunWith(JUnit4.class)
public class GCSToSourceDbWithoutReaderIT extends GCSToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDbWithoutReaderIT.class);

  private static final String SESSION_FILE_RESOURSE = "GCSToSourceDbWithoutReaderIT/session.json";

  private static final String TABLE = "Users";
  private static HashSet<GCSToSourceDbWithoutReaderIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static CustomMySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (GCSToSourceDbWithoutReaderIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = CustomMySQLResourceManager.builder(testName).build();
        createMySQLSchema(jdbcResourceManager);

        gcsResourceManager = createGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, Arrays.asList(jdbcResourceManager));
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

        jobInfo =
            launchWriterDataflowJob(
                gcsResourceManager,
                spannerMetadataResourceManager,
                new HashMap<String, String>() {
                  {
                    put("startTimestamp", "2024-05-13T08:43:10.000Z");
                    put("windowDuration", "10s");
                  }
                });
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (GCSToSourceDbWithoutReaderIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        jdbcResourceManager, spannerMetadataResourceManager, gcsResourceManager);
  }

  @Test
  public void testGCSToSource() throws IOException, InterruptedException {
    // Write events to GCS
    gcsResourceManager.uploadArtifact(
        "output/Shard1/2024-05-13T08:43:10.000Z-2024-05-13T08:43:20.000Z-pane-0-last-0-of-1.txt",
        Resources.getResource("GCSToSourceDbWithoutReaderIT/events.txt").getPath());
    assertThatPipeline(jobInfo).isRunning();

    // Assert events on Mysql
    assertRowInMySQL();
  }

  private void assertRowInMySQL() throws InterruptedException {
    long rowCount = 0;
    JDBCRowsCheck rowsCheck =
        JDBCRowsCheck.builder(jdbcResourceManager, TABLE).setMinRows(1).setMaxRows(1).build();
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(5)), rowsCheck);
    List<Map<String, Object>> rows = jdbcResourceManager.readTable(TABLE);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("id")).isEqualTo(1);
    assertThat(rows.get(0).get("name")).isEqualTo("FF");
  }

  private void createMySQLSchema(CustomMySQLResourceManager jdbcResourceManager) {
    HashMap<String, String> columns = new HashMap<>();
    columns.put("id", "INT NOT NULL");
    columns.put("name", "VARCHAR(25)");
    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "id");

    jdbcResourceManager.createTable(TABLE, schema);
  }
}
