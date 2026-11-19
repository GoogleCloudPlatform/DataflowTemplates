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
package com.google.cloud.teleport.templates.yaml;

import static org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts.assertThatBigQueryRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(GcsTextToBigQueryYaml.class)
@RunWith(JUnit4.class)
public class GcsTextToBigQueryYamlIT extends TemplateTestBase {

  private BigQueryResourceManager bigQueryResourceManager;

  private static final String TABLE_NAME = "text_records";

  @Before
  public void setUp() {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager);
  }

  @Test
  public void testGcsTextToBigQuery() throws IOException {
    // 1. Upload text test data to Cloud Storage
    String textContent = "first line\nsecond line\nthird line\n";
    gcsClient.createArtifact("input/data.txt", textContent);

    // 2. Setup BigQuery target dataset and table (ReadFromText emits rows with a "line" column)
    Schema bqSchema = Schema.of(Field.of("line", StandardSQLTypeName.STRING));
    bigQueryResourceManager.createDataset(REGION);
    TableId bqTable = bigQueryResourceManager.createTable(TABLE_NAME, bqSchema);

    // 3. Launch the Pipeline
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("inputPath", getGcsPath("input/data.txt"))
            .addParameter("table", toTableSpecLegacy(bqTable));

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // 4. Wait for job to finish and assert results
    Result result = pipelineOperator().waitUntilDone(createConfig(info));
    assertThatResult(result).isLaunchFinished();

    // 5. Verify records appear in BigQuery
    TableResult records = bigQueryResourceManager.readTable(TABLE_NAME);
    assertThatBigQueryRecords(records)
        .hasRecordsUnordered(
            List.of(
                Map.of("line", "first line"),
                Map.of("line", "second line"),
                Map.of("line", "third line")));
  }
}
