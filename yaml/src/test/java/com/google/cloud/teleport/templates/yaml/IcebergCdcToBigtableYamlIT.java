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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link IcebergCdcToBigtableYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergCdcToBigtableYaml.class)
@RunWith(JUnit4.class)
public final class IcebergCdcToBigtableYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCdcToBigtableYamlIT.class);

  private static final String CATALOG_NAME = "hadoop_catalog";
  private static final String ICEBERG_TABLE_NAME = "source_table";
  private static final long CELL_TIMESTAMP_MICROS = 5000L;

  private final String namespace =
      "iceberg_cdc_bigtable_ns_" + UUID.randomUUID().toString().replace("-", "");
  private final String icebergTableIdentifier = namespace + "." + ICEBERG_TABLE_NAME;

  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;
  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setUp() throws IOException {
    warehouseGcsResourceManager =
        artifactBucketName != null && !artifactBucketName.isEmpty()
            ? GcsResourceManager.builder(
                    artifactBucketName, getClass().getSimpleName(), credentials)
                .build()
            : GcsResourceManager.builder(getClass().getSimpleName(), credentials).build();
    warehouseGcsResourceManager.registerTempDir(namespace);

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
    icebergResourceManager.createNamespace(namespace);

    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        icebergResourceManager, warehouseGcsResourceManager, bigtableResourceManager);
  }

  @Test
  public void testIcebergCdcToBigtable() throws IOException {
    // Arrange: Re-invoke createNamespace as a propagation barrier for BigLake REST catalog.
    icebergResourceManager.createNamespace(namespace);
    LOG.info("Namespace '{}' created/verified successfully", namespace);

    Schema sourceSchema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())),
            Set.of(1));
    icebergResourceManager
        .catalog()
        .createTable(
            TableIdentifier.parse(icebergTableIdentifier),
            sourceSchema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "2"));

    // INSERT snapshots committed before the job starts
    Table sourceTable = icebergResourceManager.loadTable(icebergTableIdentifier);
    DataFile aliceFile = appendRecord(sourceTable, Map.of("id", "row1", "name", "Alice"));
    DataFile bobFile = appendRecord(sourceTable, Map.of("id", "row2", "name", "Bob"));
    appendRecord(sourceTable, Map.of("id", "row3", "name", "Charlie"));
    // Missing 'name' (null), which fails the MapToFields transformation and is routed to the
    // error output
    appendRecord(sourceTable, Map.of("id", "bad_row"));
    LOG.info("Iceberg source table seeded with INSERT snapshots");

    String tableId = generateTableId("test_table");
    bigtableResourceManager.createTable(tableId, ImmutableList.of("cf1"));

    String errorPath = getGcsPath("errors/error.json");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", icebergTableIdentifier)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter("catalogProperties", new JSONObject(getCatalogProperties()).toString())
            .addParameter("startingStrategy", "earliest")
            .addParameter("pollIntervalSeconds", "10")
            .addParameter("windowing", "{\"type\":\"fixed\",\"size\":\"10s\"}")
            .addParameter("language", "python")
            .addParameter(
                "fields",
                "{"
                    + "\"key\": {\"expression\": \"id.encode('utf-8')\", \"output_type\": \"bytes\"},"
                    + "\"type\": {\"expression\": \"'DeleteFromRow' if _change_type == 'DELETE' else 'SetCell'\", \"output_type\": \"string\"},"
                    + "\"family_name\": {\"expression\": \"'cf1'\", \"output_type\": \"string\"},"
                    + "\"column_qualifier\": {\"expression\": \"b'cq1'\", \"output_type\": \"bytes\"},"
                    + "\"value\": {\"expression\": \"name.encode('utf-8')\", \"output_type\": \"bytes\"},"
                    + "\"timestamp_micros\": {\"expression\": \""
                    + CELL_TIMESTAMP_MICROS
                    + "\", \"output_type\": \"integer\"}"
                    + "}")
            .addParameter("projectId", PROJECT)
            .addParameter("instanceId", bigtableResourceManager.getInstanceId())
            .addParameter("tableId", tableId)
            .addParameter("errorPath", errorPath);

    // Act: INSERT changes are read from the snapshots committed before launch
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result insertResult =
        pipelineOperator()
            .waitForCondition(createConfig(info), () -> readBigtableRows(tableId).size() == 3);
    assertThatResult(insertResult).meetsConditions();

    // UPDATE and DELETE snapshots committed while the job is running
    sourceTable
        .newOverwrite()
        .deleteFile(aliceFile)
        .addFile(writeDataFile(sourceTable, Map.of("id", "row1", "name", "Alicia")))
        .commit();
    sourceTable.newDelete().deleteFile(bobFile).commit();
    LOG.info("Iceberg source table updated with UPDATE and DELETE snapshots");

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndFinish(
                createConfig(info),
                () -> {
                  Map<String, Row> rows = readBigtableRows(tableId);
                  return rows.size() == 2
                      && rows.containsKey("row1")
                      && "Alicia".equals(cellValue(rows.get("row1")));
                });

    // Assert
    assertThatResult(result).meetsConditions();

    Map<String, Row> rowMap = readBigtableRows(tableId);
    assertThat(rowMap.keySet()).containsExactly("row1", "row3");
    assertCell(rowMap.get("row1"), "Alicia");
    assertCell(rowMap.get("row3"), "Charlie");

    List<Artifact> errorArtifacts =
        gcsClient.listArtifacts("errors/", Pattern.compile(".*error\\.json.*"));
    assertThat(errorArtifacts).isNotEmpty();

    StringBuilder errorContent = new StringBuilder();
    for (Artifact artifact : errorArtifacts) {
      errorContent.append(new String(artifact.contents(), StandardCharsets.UTF_8));
    }
    assertThat(errorContent.toString()).contains("bad_row");
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  private Map<String, Row> readBigtableRows(String tableId) {
    return bigtableResourceManager.readTable(tableId).stream()
        .collect(Collectors.toMap(row -> row.getKey().toStringUtf8(), row -> row));
  }

  private String cellValue(Row row) {
    return row.getCells().get(0).getValue().toStringUtf8();
  }

  private void assertCell(Row row, String expectedValue) {
    assertThat(row.getCells()).hasSize(1);
    assertThat(row.getCells().get(0).getFamily()).isEqualTo("cf1");
    assertThat(row.getCells().get(0).getQualifier().toStringUtf8()).isEqualTo("cq1");
    assertThat(cellValue(row)).isEqualTo(expectedValue);
    assertThat(row.getCells().get(0).getTimestamp()).isEqualTo(CELL_TIMESTAMP_MICROS);
  }

  private DataFile appendRecord(Table table, Map<String, Object> recordMap) throws IOException {
    DataFile dataFile = writeDataFile(table, recordMap);
    table.newAppend().appendFile(dataFile).commit();
    return dataFile;
  }

  private DataFile writeDataFile(Table table, Map<String, Object> recordMap) throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
      record.setField(entry.getKey(), entry.getValue());
    }
    OutputFile outputFile =
        table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + ".parquet");
    DataWriter<Record> icebergDataWriter =
        Parquet.writeData(outputFile)
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(table.spec())
            .schema(table.schema())
            .overwrite()
            .build();
    icebergDataWriter.write(record);
    icebergDataWriter.close();
    return icebergDataWriter.toDataFile();
  }

  private Map<String, String> getCatalogProperties() {
    return Map.of(
        "type", "rest",
        "uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        "warehouse", "gs://" + warehouseGcsResourceManager.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager");
  }
}
