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

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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

/**
 * An integration test for {@link SourceDbToSpanner} Flex template which tests a single sharded
 * migration on a simple schema.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SourceDbToSpanner.class)
@RunWith(JUnit4.class)
public class MySQLSingleShardIT extends SourceDbToSpannerITBase {
  private static PipelineLauncher.LaunchInfo jobInfo;

  public static MySQLResourceManager mySQLResourceManager;
  public static SpannerResourceManager spannerResourceManager;

  private static final String MYSQL_DUMP_FILE_RESOURCE =
      "SingleShardWithTransformation/mysql-schema.sql";

  private static final String SPANNER_DDL_RESOURCE =
      "SingleShardWithTransformation/spanner-schema.sql";

  private static final String SESSION_FILE_RESOURCE = "SingleShardWithTransformation/session.json";

  private static final String TABLE = "SingleShardWithTransformationTable";

  private static final String PKID = "pkid";

  private static final String NAME = "name";

  private static final String STATUS = "status";

  private static final String SHARD_ID = "migration_shard_id";

  private void executeCommand(String command) throws IOException, InterruptedException {
    System.out.println("Executing: " + command);
    Process process = new ProcessBuilder("bash", "-c", command).start();

    // Capture stdout
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String s;
    while ((s = stdInput.readLine()) != null) {
      System.out.println(s);
    }

    // Capture stderr
    String errorOutput = getErrorOutput(process); // Use helper method
    System.err.println(errorOutput); // Print errors immediately

    int exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException(
          "Command failed with exit code "
              + exitCode
              + ":\n"
              + errorOutput); // Include errors in exception
    }
  }

  private String getErrorOutput(Process process) throws IOException {
    BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    StringBuilder errorOutput = new StringBuilder();
    String s;
    while ((s = stdError.readLine()) != null) {
      errorOutput.append(s).append("\n");
    }
    return errorOutput.toString();
  }

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class. \
   */
  @Before
  public void setUp() throws IOException, InterruptedException {
    String toolPath = "/home/runner/spanner-migration-tool";
    File toolDir = new File(toolPath);

    if (!toolDir.exists()) {
      String cloneCommand =
          "mkdir -p "
              + toolPath
              + " && cd "
              + toolPath
              + " && git clone https://github.com/cloudspannerecosystem/spanner-migration-tool.git .";
      executeCommand(cloneCommand);
    }

    String buildCommand = "cd " + toolPath + " && go build ./cmd/spanner-migration-tool";
    executeCommand(buildCommand);

    // Add to PATH (optional but recommended)
    String addToPathCommand =
        "export PATH=$PATH:" + toolPath + "/cmd"; // Adjust if the binary is in a different location
    executeCommand(addToPathCommand);

    mySQLResourceManager = setUpMySQLResourceManager();
    spannerResourceManager = setUpSpannerResourceManager();
  }

  /** Cleanup dataflow job and all the resources and resource managers. */
  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(spannerResourceManager, mySQLResourceManager);
  }

  /**
   * TODO: This IT is currently not complete since shard id population is pending on reader. This
   * test needs to be updated whenever reader support is added.
   */
  @Test
  public void singleShardWithIdPopulationTest() throws Exception {
    loadSQLFileResource(mySQLResourceManager, MYSQL_DUMP_FILE_RESOURCE);
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
    jobInfo =
        launchDataflowJob(
            getClass().getSimpleName(),
            SESSION_FILE_RESOURCE,
            null,
            mySQLResourceManager,
            spannerResourceManager,
            null,
            null);
    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(jobInfo));
    assertThatResult(result).isLaunchFinished();

    SpannerAsserts.assertThatStructs(
            spannerResourceManager.readTableRecords(TABLE, PKID, NAME, STATUS, SHARD_ID))
        .hasRecordsUnorderedCaseInsensitiveColumns(getExpectedData());
  }

  private List<Map<String, Object>> getExpectedData() {
    return List.of(
        Map.of(PKID, 1, NAME, "Alice", STATUS, "active", SHARD_ID, "NULL"),
        Map.of(PKID, 2, NAME, "Bob", STATUS, "inactive", SHARD_ID, "NULL"),
        Map.of(PKID, 3, NAME, "Carol", STATUS, "pending", SHARD_ID, "NULL"),
        Map.of(PKID, 4, NAME, "David", STATUS, "complete", SHARD_ID, "NULL"),
        Map.of(PKID, 5, NAME, "Emily", STATUS, "error", SHARD_ID, "NULL"));
  }
}
