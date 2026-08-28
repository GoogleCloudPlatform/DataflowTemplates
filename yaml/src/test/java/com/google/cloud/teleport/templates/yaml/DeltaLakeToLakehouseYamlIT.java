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
import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link DeltaLakeToLakehouseYaml} template. */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DeltaLakeToLakehouseYaml.class)
@RunWith(JUnit4.class)
public class DeltaLakeToLakehouseYamlIT extends TemplateTestBase {

  private IcebergResourceManager icebergResourceManager;

  private static final String CATALOG_NAME = "hadoop_catalog";
  private final String namespace =
      "deltalake_lakehouse_ns_" + UUID.randomUUID().toString().replace("-", "");
  private static final String LAKEHOUSE_TABLE_NAME = "lakehouse_table";
  private final String lakehouseTableIdentifier = namespace + "." + LAKEHOUSE_TABLE_NAME;

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
  public void testDeltaLakeToLakehouse() throws Exception {
    // 1. Arrange: Create Delta Lake source table in GCS
    String deltaTableDir = "delta-table";
    String deltaTableGcsPath = getGcsPath(deltaTableDir);

    Configuration configuration = new Configuration();
    getGcsHadoopConfig().forEach(configuration::set);
    Engine engine = DefaultEngine.create(configuration);
    Table table = Table.forPath(engine, deltaTableGcsPath);

    StructType deltaSchema =
        new StructType()
            .add("id", StringType.STRING)
            .add("state", StringType.STRING)
            .add("price", DoubleType.DOUBLE);

    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "DeltaLakeToLakehouseYamlIT", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, deltaSchema);
    Transaction txn = txnBuilder.build(engine);
    io.delta.kernel.data.Row txnState = txn.getTransactionState(engine);

    ColumnVector idVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return StringType.STRING;
          }

          @Override
          public int getSize() {
            return 1;
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return false;
          }

          @Override
          public String getString(int rowId) {
            return "007";
          }
        };

    ColumnVector stateVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return StringType.STRING;
          }

          @Override
          public int getSize() {
            return 1;
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return false;
          }

          @Override
          public String getString(int rowId) {
            return "CA";
          }
        };

    ColumnVector priceVector =
        new ColumnVector() {
          @Override
          public DataType getDataType() {
            return DoubleType.DOUBLE;
          }

          @Override
          public int getSize() {
            return 1;
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return false;
          }

          @Override
          public double getDouble(int rowId) {
            return 26.23;
          }
        };

    ColumnVector[] vectors = new ColumnVector[] {idVector, stateVector, priceVector};
    ColumnarBatch columnarBatch = new DefaultColumnarBatch(1, deltaSchema, vectors);
    FilteredColumnarBatch filteredBatch =
        new FilteredColumnarBatch(columnarBatch, Optional.empty());

    CloseableIterator<FilteredColumnarBatch> data =
        io.delta.kernel.internal.util.Utils.toCloseableIterator(
            Collections.singletonList(filteredBatch).iterator());

    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(engine, txnState, data, Collections.emptyMap());

    DataWriteContext writeContext =
        Transaction.getWriteContext(engine, txnState, Collections.emptyMap());

    CloseableIterator<DataFileStatus> dataFiles =
        engine
            .getParquetHandler()
            .writeParquetFiles(
                writeContext.getTargetDirectory(),
                physicalData,
                writeContext.getStatisticsColumns());

    CloseableIterator<io.delta.kernel.data.Row> dataActions =
        Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);

    List<io.delta.kernel.data.Row> addActionsList = new ArrayList<>();
    while (dataActions.hasNext()) {
      addActionsList.add(dataActions.next());
    }

    CloseableIterable<io.delta.kernel.data.Row> dataActionsIterable =
        CloseableIterable.inMemoryIterable(
            io.delta.kernel.internal.util.Utils.toCloseableIterator(addActionsList.iterator()));

    TransactionCommitResult commitResult = txn.commit(engine, dataActionsIterable);
    if (commitResult.getVersion() < 0) {
      throw new RuntimeException("Table creation/write failed");
    }

    // 2. Arrange: Create destination Lakehouse table
    icebergResourceManager.createNamespace(namespace);
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.required(2, "state", Types.StringType.get()),
            Types.NestedField.required(3, "price", Types.DoubleType.get()));
    icebergResourceManager.createTable(lakehouseTableIdentifier, icebergSchema);

    // 3. Act: Configure options and launch template
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("deltaLakeTable", deltaTableGcsPath)
            .addParameter(
                "deltaLakeHadoopConfig", new org.json.JSONObject(getGcsHadoopConfig()).toString())
            .addParameter("lakehouseTable", lakehouseTableIdentifier)
            .addParameter("lakehouseCatalogName", CATALOG_NAME)
            .addParameter(
                "lakehouseCatalogProperties",
                new org.json.JSONObject(getCatalogProperties()).toString());

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // 4. Assert
    assertThatResult(result).isLaunchFinished();

    List<Record> records = icebergResourceManager.read(lakehouseTableIdentifier);
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
