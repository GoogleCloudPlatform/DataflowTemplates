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
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
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

  private String executeCommand(String command, String currentPath)
      throws IOException, InterruptedException {
    System.out.println("Executing: " + command);

    ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", command); // Explicitly use /bin/bash
    Map<String, String> env = pb.environment();
    env.put("PATH", currentPath); // Set the PATH

    Process process = pb.start();

    // Capture stdout and stderr concurrently
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
    BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

    StringBuilder stdoutBuilder = new StringBuilder();
    StringBuilder stderrBuilder = new StringBuilder();

    Thread stdoutThread =
        new Thread(
            () -> {
              String s;
              try {
                while ((s = stdInput.readLine()) != null) {
                  System.out.println(s); // Print stdout in real-time
                  stdoutBuilder.append(s).append("\n");
                }
              } catch (IOException e) {
                e.printStackTrace(); // Handle or log the exception as needed
              }
            });

    Thread stderrThread =
        new Thread(
            () -> {
              String s;
              try {
                while ((s = stdError.readLine()) != null) {
                  System.err.println(s); // Print stderr in real-time
                  stderrBuilder.append(s).append("\n");
                }
              } catch (IOException e) {
                e.printStackTrace(); // Handle or log the exception as needed
              }
            });

    stdoutThread.start();
    stderrThread.start();

    int exitCode = process.waitFor();
    stdoutThread.join(); // Wait for threads to finish reading output
    stderrThread.join();

    String stdout = stdoutBuilder.toString();
    String stderr = stderrBuilder.toString();

    if (exitCode != 0) {
      throw new RuntimeException(
          "Command failed with exit code "
              + exitCode
              + ":\n"
              + "Stdout:\n"
              + stdout
              + "\n"
              + "Stderr:\n"
              + stderr); // Include both outputs in exception
    }

    // Logic to extract the modified PATH (if command modifies it)
    // This will depend on what your command does.
    // Example: If your command is "export PATH=$PATH:/new/path", you would
    // need to parse the output to get the new PATH value.
    // If your command doesn't change PATH, you can simply return currentPath
    return currentPath; // Or extract and return the new path from stdout
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
    String clonedRepoPath = toolPath + "/spanner-migration-tool";

    // 1. Idempotent cloning (checks for existing repo)
    if (!Files.exists(Paths.get(clonedRepoPath))) { // Use Files.exists for better path handling
      executeCommand(
          "mkdir -p "
              + toolPath
              + " && git clone https://github.com/cloudspannerecosystem/spanner-migration-tool.git "
              + clonedRepoPath,
          System.getenv("PATH"));
    }

    // 2. Build with error checking
    executeCommand("cd " + clonedRepoPath + " && go build", System.getenv("PATH"));

    // 3. Add to PATH (correctly)
    String pathToAddTo =
        clonedRepoPath
            + "/cmd"; // Or wherever the binary is located. Best practice is to use the full path.
    String newPath = System.getenv("PATH") + ":" + pathToAddTo;
    // No command needed, just set the variable
    System.setProperty(
        "PATH",
        newPath); // Set the system property. Important: This is not persistent beyond the current
    // JVM.

    // 4. Verification (Optional but highly recommended)
    if (!Files.exists(Paths.get(pathToAddTo, "your_binary_name"))) { // Replace your_binary_name
      throw new RuntimeException(
          "Binary not found after build: " + pathToAddTo + "/your_binary_name");
    }

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
