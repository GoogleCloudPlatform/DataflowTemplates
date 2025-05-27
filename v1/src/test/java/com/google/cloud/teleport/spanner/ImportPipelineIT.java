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
package com.google.cloud.teleport.spanner;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts.assertThatStructs;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.metadata.SpannerStagingTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerTemplateITBase;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Integration test for {@link ImportPipeline} classic template. */
@Category({TemplateIntegrationTest.class, SpannerStagingTest.class})
@TemplateIntegrationTest(ImportPipeline.class)
@RunWith(Parameterized.class)
public class ImportPipelineIT extends SpannerTemplateITBase {

  private SpannerResourceManager spannerResourceManager;

  private void uploadImportPipelineArtifacts(String subdirectory) throws IOException {
    gcsClient.uploadArtifact(
        "input/EmptyTable.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/EmptyTable.avro").getPath());
    gcsClient.uploadArtifact(
        "input/EmptyTable-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/EmptyTable-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Singers.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Singers.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Singers-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Singers-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Float32Table.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Float32Table.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Float32Table-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Float32Table-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/UuidTable.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/UuidTable.avro").getPath());
    gcsClient.uploadArtifact(
        "input/UuidTable-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/UuidTable-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Identity.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Identity.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Identity-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Identity-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Sequence1.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Sequence1.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Sequence1-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Sequence1-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/Sequence2.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Sequence2.avro").getPath());
    gcsClient.uploadArtifact(
        "input/Sequence2-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/Sequence2-manifest.json")
            .getPath());

    if (Objects.equals(subdirectory, "googlesql")) {
      gcsClient.uploadArtifact(
          "input/ModelStruct.avro-00000-of-00001",
          Resources.getResource("ImportPipelineIT/" + subdirectory + "/ModelStruct.avro")
              .getPath());
      gcsClient.uploadArtifact(
          "input/ModelStruct-manifest.json",
          Resources.getResource("ImportPipelineIT/" + subdirectory + "/ModelStruct-manifest.json")
              .getPath());
    }

    gcsClient.uploadArtifact(
        "input/spanner-export.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/spanner-export.json")
            .getPath());
  }

  private void uploadImportPipelineArtifactsUuid(String subdirectory) throws IOException {
    gcsClient.uploadArtifact(
        "input/UuidTable.avro-00000-of-00001",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/UuidTable.avro").getPath());
    gcsClient.uploadArtifact(
        "input/UuidTable-manifest.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/UuidTable-manifest.json")
            .getPath());
    gcsClient.uploadArtifact(
        "input/spanner-export.json",
        Resources.getResource("ImportPipelineIT/" + subdirectory + "/spanner-export-uuid.json")
            .getPath());
  }

  private List<Map<String, Object>> getExpectedRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(ImmutableMap.of("Id", 1, "FirstName", "Roger", "LastName", "Waters"));
    expectedRows.add(ImmutableMap.of("Id", 2, "FirstName", "Nick", "LastName", "Mason"));
    expectedRows.add(ImmutableMap.of("Id", 3, "FirstName", "David", "LastName", "Gilmour"));
    expectedRows.add(ImmutableMap.of("Id", 4, "FirstName", "Richard", "LastName", "Wright"));
    return expectedRows;
  }

  private List<Map<String, Object>> getFloat32TableExpectedRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(ImmutableMap.of("Key", "1", "Float32Value", 3.14f));
    expectedRows.add(ImmutableMap.of("Key", "2", "Float32Value", 1.1f));
    expectedRows.add(ImmutableMap.of("Key", "3", "Float32Value", 3.402823E38f));
    expectedRows.add(ImmutableMap.of("Key", "4", "Float32Value", Float.NaN));
    expectedRows.add(ImmutableMap.of("Key", "5", "Float32Value", Float.POSITIVE_INFINITY));
    expectedRows.add(ImmutableMap.of("Key", "6", "Float32Value", Float.NEGATIVE_INFINITY));
    expectedRows.add(ImmutableMap.of("Key", "7", "Float32Value", 1.175493E-38));
    expectedRows.add(ImmutableMap.of("Key", "8", "Float32Value", -3.402823E38f));
    // The custom assertions in Beam do not seem to support null values.
    // Using the string NULL to match the string representation created in
    // assertThatStructs. The actual value in avro is a plain `null`.
    expectedRows.add(ImmutableMap.of("Key", "9", "Float32Value", "NULL"));
    return expectedRows;
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  @Test
  public void testGoogleSqlImportPipeline() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testGoogleSqlImportPipelineBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testGoogleSqlImportPipelineBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    uploadImportPipelineArtifacts("googlesql");
    String setDefaultTimeZoneStatement =
        "ALTER DATABASE db SET OPTIONS (default_time_zone = 'UTC')";
    spannerResourceManager.executeDdlStatement(setDefaultTimeZoneStatement);

    String resourceFileName = "ImportPipelineIT/googlesql/spanner-gsql-ddl.sql";
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(resourceFileName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("inputDir", getGcsPath("input/")));
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Struct> emptyTableRecords =
        spannerResourceManager.readTableRecords("EmptyTable", ImmutableList.of("id"));
    assertThat(emptyTableRecords).isEmpty();

    List<Struct> singersRecords =
        spannerResourceManager.readTableRecords(
            "Singers", ImmutableList.of("Id", "FirstName", "LastName"));
    assertThat(singersRecords).hasSize(4);
    assertThatStructs(singersRecords).hasRecordsUnordered(getExpectedRows());

    List<Struct> float32Records =
        spannerResourceManager.readTableRecords(
            "Float32Table", ImmutableList.of("Key", "Float32Value"));

    assertThat(float32Records).hasSize(9);
    assertThatStructs(float32Records).hasRecordsUnordered(getFloat32TableExpectedRows());
  }

  @Test
  public void testPostgresImportPipeline() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    testPostgresImportPipelineBase(
        paramAdder ->
            paramAdder.addParameter("spannerHost", spannerResourceManager.getSpannerHost()));
  }

  private void testPostgresImportPipelineBase(
      Function<PipelineLauncher.LaunchConfig.Builder, PipelineLauncher.LaunchConfig.Builder>
          paramsAdder)
      throws IOException {
    // Arrange
    uploadImportPipelineArtifacts("postgres");
    String setDefaultTimeZoneStatement = "ALTER DATABASE db SET spanner.default_time_zone = 'UTC'";
    spannerResourceManager.executeDdlStatement(setDefaultTimeZoneStatement);

    String resourceFileName = "ImportPipelineIT/postgres/spanner-pg-ddl.sql";
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(resourceFileName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder.apply(
            PipelineLauncher.LaunchConfig.builder(testName, specPath)
                .addParameter("spannerProjectId", PROJECT)
                .addParameter("instanceId", spannerResourceManager.getInstanceId())
                .addParameter("databaseId", spannerResourceManager.getDatabaseId())
                .addParameter("inputDir", getGcsPath("input/")));
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Struct> emptyTableRecords =
        spannerResourceManager.readTableRecords("EmptyTable", ImmutableList.of("id"));
    assertThat(emptyTableRecords).isEmpty();

    List<Struct> singersRecords =
        spannerResourceManager.readTableRecords(
            "Singers", ImmutableList.of("Id", "FirstName", "LastName"));
    assertThat(singersRecords).hasSize(4);
    assertThatStructs(singersRecords).hasRecordsUnordered(getExpectedRows());

    List<Struct> float32Records =
        spannerResourceManager.readTableRecords(
            "Float32Table", ImmutableList.of("Key", "Float32Value"));

    assertThat(float32Records).hasSize(9);
    assertThatStructs(float32Records).hasRecordsUnordered(getFloat32TableExpectedRows());
  }

  // TODO(b/395532087): Consolidate this with other tests after UUID launch.
  @Test
  @Ignore("Update Beam SpannerIO to support UUID")
  public void testGoogleSqlImportPipeline_UUID() throws IOException {
    // Run only on staging environment
    if (!SpannerResourceManager.STAGING_SPANNER_HOST.equals(spannerHost)) {
      return;
    }

    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.GOOGLE_STANDARD_SQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    uploadImportPipelineArtifactsUuid("googlesql");

    String resourceFileName = "ImportPipelineIT/googlesql/spanner-gsql-uuid-ddl.sql";
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(resourceFileName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("inputDir", getGcsPath("input/"))
            .addParameter("spannerHost", spannerResourceManager.getSpannerHost());
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Struct> uuidRecords =
        spannerResourceManager.runQuery(
            String.format(
                "SELECT CAST(Key as STRING) as Key, CAST(Val1 as String) AS Val1, Val2, %s FROM"
                    + " UuidTable",
                "CASE WHEN Val3 IS NULL THEN NULL ELSE ARRAY(SELECT CAST(e AS STRING) FROM"
                    + " UNNEST(Val3) AS e) END AS Val3"));
    assertThat(uuidRecords).hasSize(4);
    assertThatRecords(structsToRecords(uuidRecords))
        .hasRecordsUnordered(getUuidTableExpectedRows());
  }

  // TODO(b/395532087): Consolidate this with other tests after UUID launch.
  @Test
  @Ignore("Update Beam SpannerIO to support UUID")
  public void testPostgresImportPipeline_UUID() throws IOException {
    // Run only on staging environment
    if (!SpannerResourceManager.STAGING_SPANNER_HOST.equals(spannerHost)) {
      return;
    }

    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
            .maybeUseStaticInstance()
            .useCustomHost(spannerHost)
            .build();
    uploadImportPipelineArtifactsUuid("postgres");

    String resourceFileName = "ImportPipelineIT/postgres/spanner-pg-uuid-ddl.sql";
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(resourceFileName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).toList();
    spannerResourceManager.executeDdlStatements(ddls);

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("inputDir", getGcsPath("input/"))
            .addParameter("spannerHost", spannerResourceManager.getSpannerHost());
    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Struct> uuidRecords =
        spannerResourceManager.runQuery(
            String.format(
                "SELECT CAST(Key as TEXT) as Key, CAST(Val1 as TEXT) AS Val1, Val2, %s FROM"
                    + " UuidTable",
                "CASE WHEN Val3 IS NULL THEN NULL ELSE ARRAY(SELECT CAST(e AS TEXT) FROM"
                    + " UNNEST(Val3) AS e) END AS Val3"));
    assertThat(uuidRecords).hasSize(4);
    assertThatRecords(structsToRecords(uuidRecords))
        .hasRecordsUnordered(getUuidTableExpectedRows());
  }

  /**
   * Converts a list of Structs to a list of Maps, handling null values and converting keys to
   * lowercase.
   */
  private List<Map<String, Object>> structsToRecords(List<Struct> structs) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (Struct struct : structs) {
        Map<String, Object> record = new HashMap<>();

        for (Type.StructField field : struct.getType().getStructFields()) {
          Value fieldValue = struct.getValue(field.getName());
          String stringValue = fieldValue.toString();
          if (fieldValue.isNull()) {
            stringValue = null;
          } else if (fieldValue.getType() == Type.array(Type.string())) {
            stringValue = new ArrayList<>(fieldValue.getAsStringList()).toString();
          }
          record.put(field.getName().toLowerCase(), stringValue);
        }

        records.add(record);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  private List<Map<String, Object>> getUuidTableExpectedRows() {
    List<Map<String, Object>> expectedRows = new ArrayList<>();
    expectedRows.add(
        new HashMap<>() {
          {
            put("key", "00000000-0000-0000-0000-000000000000");
            put("val1", "00000000-0000-0000-0000-000000000000");
            put("val2", 0);
            put(
                "val3",
                List.of(
                    "00000000-0000-0000-0000-000000000001",
                    "00000000-0000-0000-0000-000000000002"));
          }
        });
    expectedRows.add(
        new HashMap<>() {
          {
            put("key", "11111111-1111-1111-1111-111111111111");
            put("val1", null);
            put("val2", 1);
            put(
                "val3",
                List.of(
                    "11111111-1111-1111-1111-111111111111",
                    "11111111-1111-1111-1111-111111111112"));
          }
        });

    expectedRows.add(
        new HashMap<>() {
          {
            put("key", "22222222-2222-2222-2222-222222222222");
            put("val1", "22222222-2222-2222-2222-222222222222");
            put("val2", 2);
            put("val3", null);
          }
        });
    expectedRows.add(
        new HashMap<>() {
          {
            put("key", "ffffffff-ffff-ffff-ffff-ffffffffffff");
            put("val1", null);
            put("val2", 3);
            put("val3", null);
          }
        });
    return expectedRows;
  }
}
