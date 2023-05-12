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

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;
import static com.google.cloud.teleport.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatGenericRecords;
import static com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.teleport.bigtable.BigtableToAvro;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.utils.ResourceManagerUtils;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.artifacts.Artifact;
import com.google.cloud.teleport.it.gcp.artifacts.utils.AvroTestUtil;
import com.google.cloud.teleport.it.gcp.bigtable.BigtableResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link BigtableToAvro} classic template. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableToAvro.class)
@RunWith(JUnit4.class)
public class BigtableToAvroIT extends TemplateTestBase {

  private BigtableResourceManager bigtableResourceManager;

  private static final String AVRO_SCHEMA =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"BigtableRow\",\n"
          + "  \"namespace\": \"com.google.cloud.teleport.bigtable\",\n"
          + "  \"fields\": [\n"
          + "    {\n"
          + "      \"name\": \"key\",\n"
          + "      \"type\": \"bytes\"\n"
          + "    },\n"
          + "    {\n"
          + "      \"name\": \"cells\",\n"
          + "      \"type\": {\n"
          + "        \"type\": \"array\",\n"
          + "        \"items\": {\n"
          + "          \"type\": \"record\",\n"
          + "          \"name\": \"BigtableCell\",\n"
          + "          \"fields\": [\n"
          + "            {\n"
          + "              \"name\": \"family\",\n"
          + "              \"type\": \"string\"\n"
          + "            },\n"
          + "            {\n"
          + "              \"name\": \"qualifier\",\n"
          + "              \"type\": \"bytes\"\n"
          + "            },\n"
          + "            {\n"
          + "              \"name\": \"timestamp\",\n"
          + "              \"type\": \"long\",\n"
          + "              \"logicalType\": \"timestamp-micros\"\n"
          + "            },\n"
          + "            {\n"
          + "              \"name\": \"value\",\n"
          + "              \"type\": \"bytes\"\n"
          + "            }\n"
          + "          ]\n"
          + "        }\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(AVRO_SCHEMA);

  @Before
  public void setUp() throws IOException {
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT)
            .setCredentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager);
  }

  @Test
  public void testBigtableToAvro() throws IOException {
    // Arrange
    String tableId = generateTableId(testName);
    bigtableResourceManager.createTable(tableId, ImmutableList.of("family1", "family2"));

    long timestamp = System.currentTimeMillis() * 1000;
    bigtableResourceManager.write(
        ImmutableList.of(
            RowMutation.create(tableId, "key1").setCell("family1", "column1", timestamp, "value1"),
            RowMutation.create(tableId, "key2").setCell("family2", "column2", timestamp, "value2"),
            RowMutation.create(tableId, "key3")
                .setCell("family2", "column2", timestamp, "value3")));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableId)
            .addParameter("outputDirectory", getGcsPath("output/"))
            .addParameter("filenamePrefix", "bigtable-to-avro-output-");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", Pattern.compile(".*"));
    assertThat(artifacts).isNotEmpty();

    List<GenericRecord> allRecords = new ArrayList<>();
    for (Artifact artifact : artifacts) {
      try {
        allRecords.addAll(AvroTestUtil.readRecords(SCHEMA, artifact.contents()));
      } catch (Exception e) {
        throw new RuntimeException("Error reading " + artifact.name() + " as Avro.", e);
      }
    }

    assertThatGenericRecords(allRecords)
        .hasRecordsWithStrings(ImmutableList.of("value1", "value2", "value3"));
  }
}
