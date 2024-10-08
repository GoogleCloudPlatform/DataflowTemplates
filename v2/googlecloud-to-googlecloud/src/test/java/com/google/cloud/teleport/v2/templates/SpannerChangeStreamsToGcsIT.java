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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.gcp.artifacts.matchers.ArtifactAsserts.assertThatGenericRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.utils.AvroTestUtil;
import org.apache.beam.it.gcp.artifacts.utils.JsonTestUtil;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration test for {@link SpannerChangeStreamsToGcs Spanner Change Streams to GCS} template.
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(SpannerChangeStreamsToGcs.class)
@RunWith(JUnit4.class)
public class SpannerChangeStreamsToGcsIT extends TemplateTestBase {

  private static final int MESSAGES_COUNT = 20;
  private static final Pattern RESULT_REGEX = Pattern.compile(".*result-.*");

  private SpannerResourceManager spannerResourceManager;

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testSpannerChangeStreamsToGcs() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    testSpannerChangeStreamsToGcsBase(Function.identity());
  }

  @Test
  public void testSpannerChangeStreamsToGcsStaging() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .maybeUseCustomHost()
            .build();
    testSpannerChangeStreamsToGcsBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testSpannerChangeStreamsToGcsBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + "  Float32Col FLOAT32,\n"
                + "  Float64Col FLOAT64,\n"
                + ") PRIMARY KEY(Id)",
            testName);
    spannerResourceManager.executeDdlStatement(createTableStatement);

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);

    // Act
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                .addParameter("spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                .addParameter("spannerChangeStreamName", testName + "_stream")
                .addParameter("gcsOutputDirectory", getGcsPath("output/"))
                .addParameter("outputFilenamePrefix", "result-")
                .addParameter("outputFileFormat", "TEXT")
                .addParameter("numShards", "1")
                .addParameter("windowDuration", "1m")
                .addParameter("rpcPriority", "HIGH"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<Mutation> expectedData = generateTableRows(testName);
    spannerResourceManager.write(expectedData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                () -> {
                  List<Artifact> outputFiles = gcsClient.listArtifacts("output/", RESULT_REGEX);
                  return !outputFiles.isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", RESULT_REGEX);

    List<Map<String, Object>> records = new ArrayList<>();
    artifacts.forEach(
        artifact -> {
          DataChangeRecord s =
              new Gson().fromJson(new String(artifact.contents()), DataChangeRecord.class);
          for (Mod mod : s.getMods()) {
            Map<String, Object> record = new HashMap<>();
            try {
              record.putAll(JsonTestUtil.readRecord(mod.getKeysJson()));
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error reading " + mod.getKeysJson() + " as JSON within " + artifact.name() + ".",
                  e);
            }
            try {
              record.putAll(JsonTestUtil.readRecord(mod.getNewValuesJson()));
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error reading "
                      + mod.getNewValuesJson()
                      + " as JSON within "
                      + artifact.name()
                      + ".",
                  e);
            }
            records.add(record);
          }
        });

    List<Map<String, Object>> expectedRecords = new ArrayList<>();
    expectedData.forEach(
        mutation -> {
          Map<String, Object> expectedRecord =
              mutation.asMap().entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey(), e -> valueToString(e.getValue())));
          expectedRecords.add(expectedRecord);
        });
    assertThatRecords(records).hasRecordsUnorderedCaseInsensitiveColumns(expectedRecords);
  }

  @Test
  public void testSpannerChangeStreamsToGcsAvro() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION).maybeUseStaticInstance().build();
    testSpannerChangeStreamsToGcsAvroBase(Function.identity());
  }

  @Test
  public void testSpannerChangeStreamsToGcsAvroStaging() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .maybeUseCustomHost()
            .build();
    testSpannerChangeStreamsToGcsAvroBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testSpannerChangeStreamsToGcsAvroBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    String createTableStatement =
        String.format(
            "CREATE TABLE `%s` (\n"
                + "  Id INT64 NOT NULL,\n"
                + "  FirstName String(1024),\n"
                + "  LastName String(1024),\n"
                + "  Float32Col FLOAT32,\n"
                + "  Float64Col FLOAT64,\n"
                + ") PRIMARY KEY(Id)",
            testName);
    spannerResourceManager.executeDdlStatement(createTableStatement);

    String createChangeStreamStatement =
        String.format("CREATE CHANGE STREAM %s_stream FOR %s", testName, testName);
    spannerResourceManager.executeDdlStatement(createChangeStreamStatement);

    // Act
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("spannerInstanceId", spannerResourceManager.getInstanceId())
                .addParameter("spannerDatabase", spannerResourceManager.getDatabaseId())
                .addParameter("spannerMetadataInstanceId", spannerResourceManager.getInstanceId())
                .addParameter("spannerMetadataDatabase", spannerResourceManager.getDatabaseId())
                .addParameter("spannerChangeStreamName", testName + "_stream")
                .addParameter("gcsOutputDirectory", getGcsPath("output/"))
                .addParameter("outputFilenamePrefix", "result-")
                .addParameter("outputFileFormat", "AVRO")
                .addParameter("numShards", "1")
                .addParameter("windowDuration", "1m")
                .addParameter("rpcPriority", "HIGH"));

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    List<Mutation> expectedData = generateTableRows(testName);
    spannerResourceManager.write(expectedData);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(info),
                () -> {
                  List<Artifact> outputFiles = gcsClient.listArtifacts("output/", RESULT_REGEX);
                  return !outputFiles.isEmpty();
                });

    // Assert
    assertThatResult(result).meetsConditions();

    List<Artifact> artifacts = gcsClient.listArtifacts("output/", RESULT_REGEX);

    List<GenericRecord> records = new ArrayList<>();
    artifacts.forEach(
        artifact -> {
          try {
            records.addAll(
                AvroTestUtil.readRecords(
                    ReflectData.get()
                        .getSchema(com.google.cloud.teleport.v2.DataChangeRecord.class),
                    artifact.contents()));
          } catch (IOException e) {
            throw new RuntimeException("Error reading " + artifact.name() + " as JSON.", e);
          }
        });

    List<String> expectedRecords = new ArrayList<>();
    expectedData.forEach(
        mutation -> {
          expectedRecords.addAll(
              mutation.asMap().values().stream()
                  .map(SpannerChangeStreamsToGcsIT::valueToString)
                  .collect(Collectors.toList()));
        });

    assertThatGenericRecords(records).hasRecordsWithStrings(expectedRecords);
  }

  private static String valueToString(Value val) {
    switch (val.getType().getCode()) {
      case INT64:
        return Long.toString(val.getInt64());
      case FLOAT32:
        return Float.toString(val.getFloat32());
      case FLOAT64:
        return Double.toString(val.getFloat64());
      case STRING:
        return val.getString();
      default:
        throw new IllegalArgumentException("Unsupported type: " + val.getType());
    }
  }

  private static List<Mutation> generateTableRows(String tableId) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < MESSAGES_COUNT; i++) {
      Mutation.WriteBuilder mutation = Mutation.newInsertBuilder(tableId);
      mutation.set("Id").to(i);
      mutation.set("FirstName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      mutation.set("LastName").to(RandomStringUtils.randomAlphanumeric(1, 20));
      // Avoiding random numbers as asserting their presence via string match is
      // error-prone for floats.
      mutation.set("Float32Col").to(i + 0.5);
      mutation.set("Float64Col").to(i + 1.5);
      mutations.add(mutation.build());
    }

    return mutations;
  }
}
