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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link IcebergCdcToIcebergYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(IcebergCdcToIcebergYaml.class)
@RunWith(JUnit4.class)
public class IcebergCdcToIcebergYamlIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCdcToIcebergYamlIT.class);

  private static final String CATALOG_NAME = "hadoop_catalog";
  private final String namespace =
      "iceberg_namespace_" + UUID.randomUUID().toString().replace("-", "");
  private static final String SOURCE_TABLE_NAME = "source_table";
  private static final String SINK_TABLE_NAME = "iceberg_table";
  private static final String ERROR_TABLE_NAME = "error_table";
  private final String sourceTableIdentifier = namespace + "." + SOURCE_TABLE_NAME;
  private final String sinkTableIdentifier = namespace + "." + SINK_TABLE_NAME;
  private final String errorTableIdentifier = namespace + "." + ERROR_TABLE_NAME;

  private IcebergResourceManager icebergResourceManager;
  private GcsResourceManager warehouseGcsResourceManager;

  @Before
  public void setUp() throws IOException {
    warehouseGcsResourceManager =
        artifactBucketName != null && !artifactBucketName.isEmpty()
            ? GcsResourceManager.builder(
                    artifactBucketName, getClass().getSimpleName(), credentials)
                .build()
            : GcsResourceManager.builder(getClass().getSimpleName(), credentials).build();
    warehouseGcsResourceManager.registerTempDir(namespace);
    LOG.info("Warehouse bucket created: {}", warehouseGcsResourceManager.getBucket());

    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
    icebergResourceManager.createNamespace(namespace);
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(icebergResourceManager, warehouseGcsResourceManager);
  }

  @Test
  public void testIcebergCdcToIceberg() throws IOException {
    // Re-invoke createNamespace to act as a propagation barrier and cache-warming step for BigLake
    // REST catalog before Dataflow launches. Do not remove: prevents eventual consistency failures.
    icebergResourceManager.createNamespace(namespace);
    LOG.info("Namespace '{}' created/verified successfully", namespace);

    Schema sourceSchema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "active", Types.IntegerType.get())),
            Set.of(1));
    icebergResourceManager
        .catalog()
        .createTable(
            TableIdentifier.parse(sourceTableIdentifier),
            sourceSchema,
            PartitionSpec.unpartitioned(),
            Map.of("format-version", "2"));

    Table sourceTable = icebergResourceManager.loadTable(sourceTableIdentifier);
    DataFile aliceFile = appendRecord(sourceTable, Map.of("id", 1, "name", "Alice", "active", 1));
    DataFile bobFile = appendRecord(sourceTable, Map.of("id", 2, "name", "Bob", "active", 0));
    appendRecord(sourceTable, Map.of("id", 3, "name", "Charlie", "active", 1));
    sourceTable
        .newOverwrite()
        .deleteFile(aliceFile)
        .addFile(writeDataFile(sourceTable, Map.of("id", 1, "name", "Alicia", "active", 1)))
        .commit();
    sourceTable.newDelete().deleteFile(bobFile).commit();
    LOG.info("Iceberg source table seeded with INSERT, UPDATE, and DELETE snapshots");

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("sourceTable", sourceTableIdentifier)
            .addParameter("table", sinkTableIdentifier)
            .addParameter("errorTable", errorTableIdentifier)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString())
            .addParameter("equalityColumns", "[\"id\"]");

    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    LOG.info("Pipeline executed successfully");

    assertThatResult(result).isLaunchFinished();
    List<Record> icebergRecords = icebergResourceManager.read(sinkTableIdentifier);
    LOG.info("Iceberg sink records: {}", icebergRecords);
    assertNotNull(icebergRecords);
    assertEquals(2, icebergRecords.size());
    icebergRecords.sort(Comparator.comparingInt(r -> ((Number) r.getField("id")).intValue()));

    Record actualRecord1 = icebergRecords.get(0);
    Record actualRecord2 = icebergRecords.get(1);
    assertEquals(1, ((Number) actualRecord1.getField("id")).intValue());
    assertEquals("Alicia", actualRecord1.getField("name"));
    assertEquals(1, ((Number) actualRecord1.getField("active")).intValue());
    assertEquals(3, ((Number) actualRecord2.getField("id")).intValue());
    assertEquals("Charlie", actualRecord2.getField("name"));
    assertEquals(1, ((Number) actualRecord2.getField("active")).intValue());
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
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
