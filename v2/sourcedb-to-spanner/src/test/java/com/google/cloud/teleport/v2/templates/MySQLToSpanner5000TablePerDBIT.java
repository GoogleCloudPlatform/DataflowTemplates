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
import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.util.HashMap;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Ignore("SQL instance running inside github runner is crashing")
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLToSpanner5000TablePerDBIT extends SourceDbToSpannerITBase {

  private PipelineLauncher.LaunchInfo jobInfo;
  private MySQLResourceManager mySQLResourceManager;
  private SpannerResourceManager spannerResourceManager;

  // Reduced number of tables to prevent container crashes
  // You can gradually increase this value based on your environment's capacity
  private static final int NUM_TABLES = 5000; // Reduced from 5000
  private static final int MAX_ALLOWED_PACKET = 500 * 1024 * 1024;
  private static final String MYSQL_DUMP_FILE_RESOURCE =
      "WideRow/5000TablePerDBIT/mysql-schema.sql";
  private static final String SPANNER_SCHEMA_FILE_RESOURCE =
      "WideRow/5000TablePerDBIT/spanner-schema.sql";
  private static final String WORKER_MACHINE_TYPE = "n1-highmem-96";

  @Before
  public void setUp() {
    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  private void increasePacketSize() {
    String allowedGlobalPacket = "SET GLOBAL max_allowed_packet = " + MAX_ALLOWED_PACKET;
    mySQLResourceManager.runSQLUpdate(allowedGlobalPacket);
  }

  @Test
  public void testMySQLToSpannerMigration() throws Exception {
    increasePacketSize();
    loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_SCHEMA_FILE_RESOURCE);

    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            null,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            new HashMap<String, String>() {
              {
                put("workerMachineType", WORKER_MACHINE_TYPE);
              }
            },
            null);

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    for (int i = 0; i < NUM_TABLES; i++) {
      String tableName = "table" + i + 1;
      assertEquals(
          "Table " + tableName + " should have the same number of rows",
          0,
          spannerResourceManager.getRowCount(tableName).longValue());
    }
  }
}
