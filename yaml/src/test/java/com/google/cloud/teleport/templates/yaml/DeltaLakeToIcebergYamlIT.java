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

import com.google.cloud.teleport.it.iceberg.IcebergResourceManager;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.artifacts.utils.ParquetTestUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link DeltaLakeToIcebergYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DeltaLakeToIcebergYaml.class)
@RunWith(JUnit4.class)
public class DeltaLakeToIcebergYamlIT extends TemplateTestBase {

  private IcebergResourceManager icebergResourceManager;

  private static final String CATALOG_NAME = "hadoop_catalog";
  private final String namespace =
      "deltalake_iceberg_ns_" + UUID.randomUUID().toString().replace("-", "");
  private static final String ICEBERG_TABLE_NAME = "iceberg_table";
  private final String icebergTableIdentifier = namespace + "." + ICEBERG_TABLE_NAME;

  @Before
  public void setUp() throws IOException {
    gcsClient.registerTempDir(namespace);

    // Initialize Iceberg resource manager
    icebergResourceManager =
        IcebergResourceManager.builder(testName)
            .setCatalogName(CATALOG_NAME)
            .setCatalogProperties(getCatalogProperties())
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(icebergResourceManager);
  }

  @Test
  public void testDeltaLakeToIceberg() throws IOException {
    // 1. Arrange: Create Delta Lake source table in GCS
    String deltaTableDir = "delta-table";
    org.apache.avro.Schema avroSchema =
        new org.apache.avro.Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"test_record\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"string\"},"
                    + "{\"name\":\"state\",\"type\":\"string\"},"
                    + "{\"name\":\"price\",\"type\":\"double\"}"
                    + "]}");
    org.apache.avro.generic.GenericRecord avroRecord =
        new org.apache.avro.generic.GenericData.Record(avroSchema);
    avroRecord.put("id", "007");
    avroRecord.put("state", "CA");
    avroRecord.put("price", 26.23);
    byte[] parquetBytes = ParquetTestUtil.createParquetFile(avroSchema, List.of(avroRecord));

    // Upload data Parquet file
    gcsClient.createArtifact(deltaTableDir + "/part-00000.parquet", parquetBytes);

    // Create and upload Delta Lake transaction log
    String commitContent =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},"
            + "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":["
            + "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},"
            + "{\\\"name\\\":\\\"state\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},"
            + "{\\\"name\\\":\\\"price\\\",\\\"type\\\":\\\"double\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}"
            + "]}\",\"partitionColumns\":[],\"configuration\":{},\"createdAt\":123456789}}\n"
            + "{\"add\":{\"path\":\"part-00000.parquet\",\"partitionValues\":{},\"size\":"
            + parquetBytes.length
            + ","
            + "\"modificationTime\":123456789,\"dataChange\":true}}";

    gcsClient.createArtifact(
        deltaTableDir + "/_delta_log/00000000000000000000.json", commitContent);

    String deltaTableGcsPath = getGcsPath(deltaTableDir);

    // 2. Arrange: Create destination Iceberg table
    icebergResourceManager.createNamespace(namespace);
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.required(2, "state", Types.StringType.get()),
            Types.NestedField.required(3, "price", Types.DoubleType.get()));
    icebergResourceManager.createTable(icebergTableIdentifier, icebergSchema);

    // 3. Act: Configure options and launch template
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("deltaLakeTable", deltaTableGcsPath)
            .addParameter(
                "deltaLakeHadoopConfig", new org.json.JSONObject(getGcsHadoopConfig()).toString())
            .addParameter("table", icebergTableIdentifier)
            .addParameter("catalogName", CATALOG_NAME)
            .addParameter(
                "catalogProperties", new org.json.JSONObject(getCatalogProperties()).toString());

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // 4. Assert
    assertThatResult(result).isLaunchFinished();

    List<Record> records = icebergResourceManager.read(icebergTableIdentifier);
    assertEquals(1, records.size());

    Record record = records.get(0);
    assertEquals("007", record.getField("id"));
    assertEquals("CA", record.getField("state"));
    assertEquals(26.23, record.getField("price"));
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(PROJECT)
        .setRegion(REGION)
        .build();
  }

  private Map<String, String> getCatalogProperties() {
    return Map.of(
        "type", "rest",
        "uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog",
        "warehouse", "gs://" + gcsClient.getBucket(),
        "header.x-goog-user-project", PROJECT,
        "rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager",
        "rest-metrics-reporting-enabled", "false");
  }

  private Map<String, String> getGcsHadoopConfig() {
    return Map.of(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "fs.gs.auth.type", "APPLICATION_DEFAULT",
        "fs.gs.project.id", PROJECT);
  }
}
