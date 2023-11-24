/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.sqllauncher;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.zetasql.SqlException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlException;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataflowSqlLauncher}. */
@RunWith(JUnit4.class)
public final class DataflowSqlLauncherTest {

  @Rule public TemporaryFolder logFolder = new TemporaryFolder();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String OUTPUT_TABLE =
      "{ "
          + "\"projectId\" : \"fake-project\", "
          + "\"datasetId\" : \"fake-dataset\", "
          + "\"tableId\" : \"fake-table\" "
          + "}";
  private static final String OUTPUTS =
      "[{\"type\": \"bigquery\", \"table\": "
          + OUTPUT_TABLE
          + ", \"writeDisposition\": \"WRITE_EMPTY\"}]";

  private @Nullable String oldLoggingPath = null;

  /** Set up logging directory appropriate for tests. */

  /**
   * Test that basic valid SQL with deprecated --outputTable runs and parses.
   *
   * <p>This is deliberately missing many things required to actually run the query: project,
   * tempLocation, runner, jobName.
   */
  @Test
  public void smokeTestBuildPipelineDeprecatedOutputTable() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {
                  "--queryString=SELECT 1 AS col1", "--dryRun=true", "--outputTable=" + OUTPUT_TABLE
                })
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    DataflowSqlLauncher.buildPipeline(options);
  }

  /**
   * Test that basic valid SQL runs and parses.
   *
   * <p>This is deliberately missing many things required to actually run the query: project,
   * tempLocation, runner, jobName.
   */
  @Test
  public void smokeTestBuildPipelineOutputs() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {
                  "--queryString=SELECT 1 AS col1", "--dryRun=true", "--outputs=" + OUTPUTS
                })
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    DataflowSqlLauncher.buildPipeline(options);
  }

  @Test
  public void smokeTestBothOutputs() throws Exception {
    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage(allOf(containsString("outputTable"), containsString("outputs")));
    thrown.expectCause(instanceOf(InvalidSinkException.class));
    DataflowSqlLauncher.main(
        new String[] {
          "--dryRun=true",
          "--queryString=SELECT 1 AS col1",
          "--outputTable=" + OUTPUT_TABLE,
          "--outputs=" + OUTPUTS
        });
  }

  /**
   * Checks that a failure parsing --outputs bubbles out to main. More detailed failure cases for
   * --outputs parsing can be found in {@link SinkDefinitionTest}.
   */
  @Test
  public void smokeTestBadOutputs() throws Exception {
    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("outputs");
    thrown.expectCause(instanceOf(InvalidSinkException.class));
    DataflowSqlLauncher.main(
        new String[] {
          "--dryRun=true", "--queryString=SELECT 1 AS col1", "--outputs=[{\"type\": \"foobar\"}]"
        });
  }

  /** Exercise failure if querystring is not provided. */
  @Test
  public void smokeTestMissingQuery() throws Exception {
    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("queryString");
    thrown.expectCause(instanceOf(IllegalArgumentException.class));
    DataflowSqlLauncher.main(new String[] {"--dryRun=true", "--outputTable=" + OUTPUT_TABLE});
  }

  /** Exercise failure if output table is not provided. */
  @Test
  public void smokeTestMissingOutputs() throws Exception {
    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("outputs");
    thrown.expectCause(instanceOf(IllegalArgumentException.class));
    DataflowSqlLauncher.main(new String[] {"--dryRun=true", "--queryString=SELECT 1 AS col1"});
  }

  /** Checks that an exception thrown while parsing the query is rethrown. */
  @Test
  public void smokeTestBadQuery() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                "--queryString=This is not SQL", "--dryRun=true", "--outputs=" + OUTPUTS)
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("Syntax error");
    thrown.expectCause(instanceOf(SqlException.class));
    DataflowSqlLauncher.buildPipeline(options);
  }

  /** Checks that an exception thrown while translating/planning the query is rethrown. */
  @Test
  public void smokeTestCatchesZetaSqlException() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                "--queryString=SELECT CAST(@x AS NUMERIC) AS ColA",
                "--dryRun=true",
                "--outputs=" + OUTPUTS)
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.setQueryParameters(
        "[{\"parameterType\": {\"type\": \"DOUBLE\"}, \"parameterValue\": {\"value\":"
            + " \"1.7976931348623157e+308\"}, \"name\": \"x\"}]");
    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("Casting TYPE_DOUBLE as TYPE_NUMERIC would cause overflow of literal");
    thrown.expectCause(instanceOf(ZetaSqlException.class));
    DataflowSqlLauncher.buildPipeline(options);
  }

  @Test
  public void smokeTestShuffleModeNotSet() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {
                  "--queryString=SELECT 1 AS col1", "--dryRun=true", "--outputTable=" + OUTPUT_TABLE
                })
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    Pipeline p = DataflowSqlLauncher.buildPipeline(options);
    List<String> shuffleModeFlags =
        MoreObjects.firstNonNull(
                p.getOptions().as(ExperimentalOptions.class).getExperiments(),
                Collections.<String>emptyList())
            .stream()
            .filter(exp -> exp.startsWith("shuffle_mode="))
            .collect(Collectors.toList());

    // Only one shuffle_mode flag should be present, and it should be shuffle_mode=auto
    assertThat(shuffleModeFlags, equalTo(ImmutableList.of("shuffle_mode=auto")));
  }

  @Test
  public void smokeTestShuffleModeApplianceSet() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {
                  "--queryString=SELECT 1 AS col1",
                  "--dryRun=true",
                  "--outputTable=" + OUTPUT_TABLE,
                  "--experiments=shuffle_mode=appliance"
                })
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    Pipeline p = DataflowSqlLauncher.buildPipeline(options);
    List<String> shuffleModeFlags =
        p.getOptions().as(ExperimentalOptions.class).getExperiments().stream()
            .filter(exp -> exp.startsWith("shuffle_mode="))
            .collect(Collectors.toList());
    assertThat(shuffleModeFlags, contains("shuffle_mode=appliance"));
  }

  @Test
  public void smokeTestNoShuffleModeForcedStreaming() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {
                  "--queryString=SELECT 1 AS col1",
                  "--dryRun=true",
                  "--outputTable=" + OUTPUT_TABLE,
                  "--streaming=true"
                })
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());
    options.as(StreamingOptions.class).setStreaming(true);

    Pipeline p = DataflowSqlLauncher.buildPipeline(options);
    List<String> shuffleModeFlags =
        MoreObjects.firstNonNull(
                p.getOptions().as(ExperimentalOptions.class).getExperiments(),
                Collections.<String>emptyList())
            .stream()
            .filter(exp -> exp.startsWith("shuffle_mode="))
            .collect(Collectors.toList());
    assertThat(shuffleModeFlags, emptyIterable());
  }

  @Test
  public void testUnnamedOutputColumnsDisallowed() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {"--queryString=SELECT 1", "--dryRun=true", "--outputs=" + OUTPUTS})
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());

    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("All output columns in the top-level SELECT must be named.");
    DataflowSqlLauncher.buildPipeline(options);
  }

  @Test
  public void testBadPipelineOptions_multipleRunners() throws Exception {
    thrown.expect(BadTemplateArgumentsException.class);
    thrown.expectMessage("Bad pipeline options");

    DataflowSqlLauncher.main(new String[] {"--runner=DataflowRunner", "--runner=DirectRunner"});
  }

  @Test
  public void testBadPipelineOptions_dataflowRunnerBadTempLocation() throws Exception {
    DataflowSqlLauncherOptions options =
        PipelineOptionsFactory.fromArgs(
                new String[] {
                  "--queryString=SELECT 1 AS `foo`",
                  "--dryRun=true",
                  "--outputs=" + OUTPUTS,
                  "--runner=DataflowRunner"
                })
            .withValidation()
            .as(DataflowSqlLauncherOptions.class);

    options.as(GcpOptions.class).setGcpCredential(new TestCredential());
    options.as(GcpOptions.class).setProject("test-project");
    options.as(DataflowPipelineOptions.class).setRegion("test-region");
    options.as(DataflowPipelineOptions.class).setGcpTempLocation("gs://does-not/exist");

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Output path does not exist or is not writeable: gs://does-not/exist");
    thrown.expectCause(
        hasProperty("message", containsString("The specified bucket does not exist.")));
    DataflowSqlLauncher.buildPipeline(options);
  }
}
