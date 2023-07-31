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
package com.google.cloud.teleport.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(value = BigQueryToTFRecord.class)
@RunWith(JUnit4.class)
public class BigQueryToTFRecordIT extends TemplateTestBase {

  private static final String GCS_RESULTS_SUFFIX = "/results/";

  private static final String SQL_QUERY =
      "Select station_number, year, mean_temp, fog, rain, \"Some Comments\" as comments,\n"
          + " [1,2,3] as test_int_array, [\"hello\", \"world\"] as test_string_array,\n"
          + " [2.40, 2.60] as test_float_array, [true, false] as test_boolean_array\n"
          + " from `bigquery-public-data.samples.gsod`\n"
          + " where station_number=31590 and year=1933 and month=5 and day=11;";

  @Test
  public void testBigQueryToTfRecord() throws IOException, URISyntaxException {
    // Act
    PipelineLauncher.LaunchInfo info =
        launchTemplate(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("readQuery", SQL_QUERY)
                .addParameter("outputDirectory", getGcsPath(testName) + GCS_RESULTS_SUFFIX));
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDoneAndFinish(createConfig(info));

    List<Artifact> artifacts =
        gcsClient.listArtifacts(testName + GCS_RESULTS_SUFFIX, Pattern.compile(".*tfrecord"));

    // Assert
    assertThatResult(result).isLaunchFinished();
    assertThat(artifacts.get(0).contents())
        .isEqualTo(
            Files.readAllBytes(
                Path.of(Resources.getResource("BigQueryToTFRecordIT/output.tfrecord").toURI())));
  }
}
