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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.CustomMySQLResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
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
public class GCSToSourceDbWithReaderIT extends GCSToSourceDbITBase {

  private static final Logger LOG = LoggerFactory.getLogger(GCSToSourceDbWithReaderIT.class);

  private static final String SPANNER_DDL_RESOURCE = "GCSToSourceDbWithReaderIT/spanner-schema.sql";
  private static final String SESSION_FILE_RESOURSE = "GCSToSourceDbWithReaderIT/session.json";

  private static final String TABLE = "Users";
  private static HashSet<GCSToSourceDbWithReaderIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo writerJobInfo;
  private static PipelineLauncher.LaunchInfo readerJobInfo;
  public static SpannerResourceManager spannerResourceManager;
  private static SpannerResourceManager spannerMetadataResourceManager;
  private static CustomMySQLResourceManager jdbcResourceManager;
  private static GcsResourceManager gcsResourceManager;
  private static FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (GCSToSourceDbWithReaderIT.class) {
      testInstances.add(this);
      if (writerJobInfo == null) {
        spannerResourceManager = createSpannerDatabase(SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = CustomMySQLResourceManager.builder(testName).build();
        createMySQLSchema(jdbcResourceManager);

        gcsResourceManager = createGcsResourceManager();
        createAndUploadShardConfigToGcs(gcsResourceManager, Arrays.asList(jdbcResourceManager));
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURSE).getPath());

        readerJobInfo =
            launchReaderDataflowJob(
                gcsResourceManager, spannerResourceManager, spannerMetadataResourceManager);
        writerJobInfo = launchWriterDataflowJob(gcsResourceManager, spannerMetadataResourceManager);
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
    for (GCSToSourceDbWithReaderIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        jdbcResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        flexTemplateDataflowJobResourceManager);
  }

  @Test
  public void testGCSToSource() throws IOException, InterruptedException {
    assertThatPipeline(readerJobInfo).isRunning();
    assertThatPipeline(writerJobInfo).isRunning();
    // Write row in Spanner
    writeRowInSpanner();

    // Assert events on Mysql
    assertRowInMySQL();
  }

  private void writeRowInSpanner() {
    // Write a single record to Spanner
    Mutation m =
        Mutation.newInsertOrUpdateBuilder("Users").set("id").to(1).set("name").to("FF").build();
    spannerResourceManager.write(m);
  }

  private void assertRowInMySQL() throws InterruptedException {
    long rowCount = 0;
    for (int i = 0; rowCount != 1 && i < 60; ++i) {
      rowCount = jdbcResourceManager.getRowCount(TABLE);
      LOG.info("Row count = {}, Waiting for 30s if row count not = 1", rowCount);
      Thread.sleep(10000);
    }
    assertThat(rowCount).isEqualTo(1);
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
