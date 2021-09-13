/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.teleport.v2.templates.DataplexBigQueryToGcs.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.transforms.AbstractDataplexBigQueryToGcsTransform.FileFormat;
import com.google.cloud.teleport.v2.transforms.AbstractDataplexBigQueryToGcsTransform.Options;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DataplexBigQueryTableToGcsTransform}. */
@RunWith(JUnit4.class)
public class DataplexBigQueryTableToGcsTransformTest {
  private static final String PROJECT = "test-project1";
  private static final String DATASET = "test-dataset1";
  private static final String TABLE = "tableA";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Rule public final TestPipeline testPipeline = TestPipeline.create();

  @Mock private static BigQuery bqMock;
  private BigQueryServices bqFakeServices;
  private FakeDatasetService fakeDatasetService;
  private DataplexBigQueryToGcsOptions options;
  private BigQueryTable table;
  private File outDir;
  private List<TableRow> defaultTableRecords;
  private List<String> defaultExpectedRecords;
  private Schema avroSchema;

  @Before
  public void setUp() throws InterruptedException, IOException {
    options = TestPipeline.testingPipelineOptions().as(DataplexBigQueryToGcsOptions.class);
    options.setProject(PROJECT);
    options.setUpdateDataplexMetadata(true);
    // Required when using BigQueryIO.withMethod(EXPORT).
    options.setTempLocation(tmpDir.newFolder("bqTmp").getAbsolutePath());

    outDir = tmpDir.newFolder("out");

    TableSchema bqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("ts").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("s1").setType("STRING"),
                    new TableFieldSchema().setName("i1").setType("INTEGER")));

    avroSchema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
                    + "[{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                    + "{\"name\":\"s1\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"i1\",\"type\":[\"null\",\"long\"]}]}");

    table =
        BigQueryTable.builder()
            .setTableName(TABLE)
            .setDatasetId(DatasetId.of(PROJECT, DATASET))
            .setSchema(avroSchema)
            .setLastModificationTime(System.currentTimeMillis() * 1000)
            .build();

    defaultTableRecords =
        ImmutableList.of(
            new TableRow().set("ts", 1L).set("s1", "1001").set("i1", 2001L),
            new TableRow().set("ts", 2L).set("s1", "1002").set("i1", 2002L),
            new TableRow().set("ts", 3L).set("s1", "1003").set("i1", 2003L),
            new TableRow().set("ts", 4L).set("s1", "1004").set("i1", null),
            new TableRow().set("ts", 5L).set("s1", "1005").set("i1", 2005L));

    defaultExpectedRecords =
        Arrays.asList(
            "{\"ts\": 1, \"s1\": \"1001\", \"i1\": 2001}",
            "{\"ts\": 2, \"s1\": \"1002\", \"i1\": 2002}",
            "{\"ts\": 3, \"s1\": \"1003\", \"i1\": 2003}",
            "{\"ts\": 4, \"s1\": \"1004\", \"i1\": null}",
            "{\"ts\": 5, \"s1\": \"1005\", \"i1\": 2005}");

    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
    fakeDatasetService.createTable(
        new Table().setTableReference(table.toTableReference()).setSchema(bqSchema));
    bqFakeServices =
        new FakeBigQueryServices()
            .withJobService(new FakeJobService())
            .withDatasetService(fakeDatasetService);

    when(bqMock.query(any())).thenReturn(new EmptyTableResult(null));
  }

  @Test
  public void testTargetPathContainsTableName() {
    DataplexBigQueryTableToGcsTransform t =
        new DataplexBigQueryTableToGcsTransform(options, "test-bucket1", table);

    String expectedPath = String.format("gs://test-bucket1/%s", table.getTableName());
    assertThat(t.getTargetPath()).isEqualTo(expectedPath);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_mainPathWithAllStepsEnabled() throws Exception {
    insertDefaultTableData();
    options.setDeleteSourceData(true);
    options.setUpdateDataplexMetadata(true);

    runTransform(options);

    verify(bqMock, times(1))
        .query(
            QueryJobConfiguration.newBuilder("truncate table `test-project1.test-dataset1.tableA`")
                .build());
    verifyNoMoreInteractions(bqMock);

    PCollection<String> actualRecords =
        testPipeline
            .apply(ParquetIO.read(avroSchema).from(outDir.toPath().resolve("*.parquet").toString()))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Object::toString));

    PAssert.that(actualRecords).containsInAnyOrder(defaultExpectedRecords);

    testPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withAvroFileFormat_producesAvroFiles() throws Exception {
    insertDefaultTableData();
    options.setFileFormat(FileFormat.AVRO);

    runTransform(options);

    PCollection<String> actualRecords =
        testPipeline
            .apply(
                AvroIO.readGenericRecords(avroSchema)
                    .from(outDir.toPath().resolve("*.avro").toString()))
            .apply(MapElements.into(TypeDescriptors.strings()).via(Object::toString));

    PAssert.that(actualRecords).containsInAnyOrder(defaultExpectedRecords);

    testPipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataDefault_doesntTruncateTable() throws Exception {
    insertDefaultTableData();

    runTransform(options);

    verifyNoMoreInteractions(bqMock);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataDisabled_doesntTruncateTable() throws Exception {
    insertDefaultTableData();
    options.setDeleteSourceData(false);

    runTransform(options);

    verifyNoMoreInteractions(bqMock);
  }

  private void insertDefaultTableData() throws IOException, InterruptedException {
    fakeDatasetService.insertAll(table.toTableReference(), defaultTableRecords, null);
  }

  private void runTransform(Options options) {
    Pipeline p = Pipeline.create(options);
    p.apply(new TestDataplexBigQueryTableToGcsTransform(options, table, outDir, bqFakeServices));
    p.run();
  }

  private static class TestDataplexBigQueryTableToGcsTransform
      extends DataplexBigQueryTableToGcsTransform {

    private final String targetPath;
    private final BigQueryServices testBqServices;

    public TestDataplexBigQueryTableToGcsTransform(
        Options options, BigQueryTable table, File targetDir, BigQueryServices testBqServices) {
      // Bucket used in getTargetPath() doesn't matter as we're overriding this method anyway.
      super(options, "non-existing-bucket", table);
      this.targetPath = targetDir.getAbsolutePath();
      this.testBqServices = testBqServices;
    }

    @Override
    protected String getTargetPath() {
      return targetPath;
    }

    @Override
    protected TypedRead<GenericRecord> getBigQueryRead() {
      return super.getBigQueryRead().withTestServices(testBqServices);
    }

    @Override
    BigQuery createBqClient() {
      return bqMock;
    }
  }
}
