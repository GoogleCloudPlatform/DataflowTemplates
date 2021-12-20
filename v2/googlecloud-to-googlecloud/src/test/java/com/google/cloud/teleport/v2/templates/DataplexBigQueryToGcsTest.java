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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Base64;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatistics2;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.templates.DataplexBigQueryToGcs.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryTableToGcsTransform.FileFormat;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
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

/** Unit tests for {@link DataplexBigQueryToGcs}. */
@RunWith(JUnit4.class)
public class DataplexBigQueryToGcsTest {
  private static final String PROJECT = "test-project1";
  private static final String DATASET = "test-dataset1";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Rule public final TestPipeline testPipeline = TestPipeline.create();

  @Mock private static BigQuery bqMock;
  private BigQueryServices bqFakeServices;
  private CustomFakeJobService fakeJobService;
  private FakeDatasetService fakeDatasetService;
  private DataplexBigQueryToGcsOptions options;
  private File outDir;
  private TableRow[] defaultRecords;
  private String[] defaultExpectedRecords;
  private TableSchema bqSchema;
  private Schema avroSchema;
  private Map<String, BigQueryTable> tableByName;

  @Before
  public void setUp() throws InterruptedException, IOException {
    options = TestPipeline.testingPipelineOptions().as(DataplexBigQueryToGcsOptions.class);
    options.setProject(PROJECT);
    options.setUpdateDataplexMetadata(true);
    // Required when using BigQueryIO.withMethod(EXPORT).
    options.setTempLocation(tmpDir.newFolder("bqTmp").getAbsolutePath());

    outDir = tmpDir.newFolder("out");

    bqSchema =
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

    long modTime = System.currentTimeMillis() * 1000;

    BigQueryTablePartition p1 =
        BigQueryTablePartition.builder()
            .setPartitionName("p1")
            .setLastModificationTime(modTime)
            .build();
    BigQueryTablePartition p2 =
        BigQueryTablePartition.builder()
            .setPartitionName("p2")
            .setLastModificationTime(modTime)
            .build();

    BigQueryTable t1 =
        BigQueryTable.builder()
            .setTableName("partitioned_table")
            .setProject(PROJECT)
            .setDataset(DATASET)
            .setSchema(avroSchema)
            .setLastModificationTime(modTime)
            .setPartitioningColumn("ts")
            .setPartitions(Arrays.asList(p1, p2))
            .build();

    BigQueryTable t2 =
        BigQueryTable.builder()
            .setTableName("unpartitioned_table")
            .setProject(PROJECT)
            .setDataset(DATASET)
            .setSchema(avroSchema)
            .setLastModificationTime(modTime)
            .build();

    tableByName = new HashMap<>();
    tableByName.put(t1.getTableName(), t1);
    tableByName.put(t2.getTableName(), t2);

    defaultRecords =
        new TableRow[] {
          new TableRow().set("ts", 1L).set("s1", "1001").set("i1", 2001L),
          new TableRow().set("ts", 2L).set("s1", "1002").set("i1", 2002L),
          new TableRow().set("ts", 3L).set("s1", "1003").set("i1", 2003L),
          new TableRow().set("ts", 4L).set("s1", "1004").set("i1", null),
          new TableRow().set("ts", 5L).set("s1", "1005").set("i1", 2005L)
        };

    defaultExpectedRecords =
        new String[] {
          "{\"ts\": 1, \"s1\": \"1001\", \"i1\": 2001}",
          "{\"ts\": 2, \"s1\": \"1002\", \"i1\": 2002}",
          "{\"ts\": 3, \"s1\": \"1003\", \"i1\": 2003}",
          "{\"ts\": 4, \"s1\": \"1004\", \"i1\": null}",
          "{\"ts\": 5, \"s1\": \"1005\", \"i1\": 2005}"
        };

    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);
    fakeDatasetService.createTable(
        new Table()
            .setTableReference(t1.toTableReference())
            .setSchema(bqSchema)
            .setRequirePartitionFilter(true)
            .setTimePartitioning(new TimePartitioning().setField("ts").setType("DAY")));
    fakeDatasetService.createTable(
        new Table().setTableReference(t2.toTableReference()).setSchema(bqSchema));
    fakeJobService = new CustomFakeJobService();
    bqFakeServices =
        new FakeBigQueryServices()
            .withJobService(fakeJobService)
            .withDatasetService(fakeDatasetService);

    when(bqMock.query(any())).thenReturn(new EmptyTableResult(null));
    when(bqMock.delete(any(TableId.class))).thenReturn(true);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testE2E_mainPathWithAllStepsEnabled() throws Exception {
    insertTableData("unpartitioned_table", defaultRecords);
    insertPartitionData("partitioned_table", "p1", Arrays.copyOfRange(defaultRecords, 0, 2));
    insertPartitionData("partitioned_table", "p2", Arrays.copyOfRange(defaultRecords, 2, 5));
    // Some data is inserted into p3 just to check that it actually does NOT get exported.
    // The partitioned_table BigQueryTable object doesn't actually have p3 in the partition list.
    insertPartitionData("partitioned_table", "p3", defaultRecords);

    options.setDeleteSourceData(true);
    options.setUpdateDataplexMetadata(true);

    runTransform("unpartitioned_table", "partitioned_table");

    verify(bqMock, times(1))
        .query(
            QueryJobConfiguration.newBuilder(
                    "truncate table `test-project1.test-dataset1.unpartitioned_table`")
                .build());
    verify(bqMock, times(1)).delete(tableId("partitioned_table$p1"));
    verify(bqMock, times(1)).delete(tableId("partitioned_table$p2"));
    verifyNoMoreInteractions(bqMock);

    PCollection<String> actualUnpartitionedRecords =
        testPipeline
            .apply(
                "readTableFiles",
                ParquetIO.read(avroSchema)
                    .from(
                        outDir.getAbsolutePath()
                            + "/unpartitioned_table/output-unpartitioned_table.parquet"))
            .apply(
                "mapTableFiles", MapElements.into(TypeDescriptors.strings()).via(Object::toString));
    PCollection<String> actualPartitionedRecords1 =
        testPipeline
            .apply(
                "readP1Files",
                ParquetIO.read(avroSchema)
                    .from(
                        outDir.getAbsolutePath()
                            + "/partitioned_table/ts_pid=p1/output-partitioned_table-p1.parquet"))
            .apply("mapP1Files", MapElements.into(TypeDescriptors.strings()).via(Object::toString));
    PCollection<String> actualPartitionedRecords2 =
        testPipeline
            .apply(
                "readP2Files",
                ParquetIO.read(avroSchema)
                    .from(
                        outDir.getAbsolutePath()
                            + "/partitioned_table/ts_pid=p2/output-partitioned_table-p2.parquet"))
            .apply("mapP2Files", MapElements.into(TypeDescriptors.strings()).via(Object::toString));
    PCollection<String> actualPartitionedRecords3 =
        testPipeline
            .apply(
                "readP3Files",
                ParquetIO.read(avroSchema)
                    .from(outDir.getAbsolutePath() + "/partitioned_table/ts_pid=p3/*.parquet"))
            .apply("mapP3Files", MapElements.into(TypeDescriptors.strings()).via(Object::toString));

    PAssert.that(actualUnpartitionedRecords).containsInAnyOrder(defaultExpectedRecords);
    PAssert.that(actualPartitionedRecords1)
        .containsInAnyOrder(Arrays.copyOfRange(defaultExpectedRecords, 0, 2));
    PAssert.that(actualPartitionedRecords2)
        .containsInAnyOrder(Arrays.copyOfRange(defaultExpectedRecords, 2, 5));
    PAssert.that(actualPartitionedRecords3).empty();

    testPipeline.run();
  }

  /** Tests export in Avro format using non-default compression. */
  @Test
  @Category(NeedsRunner.class)
  public void testE2E_withAvroFileFormatAndGzipCompression_producesAvroFiles() throws Exception {
    insertTableData("unpartitioned_table", defaultRecords);
    options.setFileFormat(FileFormat.AVRO);
    options.setFileCompression(DataplexCompression.GZIP);

    runTransform("unpartitioned_table");

    PCollection<String> actualRecords =
        testPipeline
            .apply(
                "readTableFiles",
                AvroIO.readGenericRecords(avroSchema)
                    .from(outDir.getAbsolutePath() + "/unpartitioned_table/*.avro"))
            .apply(
                "mapTableFiles", MapElements.into(TypeDescriptors.strings()).via(Object::toString));
    PAssert.that(actualRecords).containsInAnyOrder(defaultExpectedRecords);

    testPipeline.run();
  }

  @Test
  public void testE2E_withDeleteSourceDataDefault_doesntTruncateData() throws Exception {
    insertTableData("unpartitioned_table", defaultRecords);
    insertPartitionData("partitioned_table", "p1", defaultRecords);
    insertPartitionData("partitioned_table", "p2", defaultRecords);

    runTransform("unpartitioned_table", "partitioned_table");

    verifyNoMoreInteractions(bqMock);
  }

  @Test
  public void testE2E_withDeleteSourceDataDisabled_doesntTruncateData() throws Exception {
    options.setDeleteSourceData(false);
    insertTableData("unpartitioned_table", defaultRecords);
    insertPartitionData("partitioned_table", "p1", defaultRecords);
    insertPartitionData("partitioned_table", "p2", defaultRecords);

    runTransform("unpartitioned_table", "partitioned_table");

    verifyNoMoreInteractions(bqMock);
  }

  @Test
  public void testGetFilesInDirectory_withValidPath_returnsPathsOfFilesInDirectory()
      throws Exception {
    File outputDir1 = tmpDir.newFolder("out", "unpartitioned_table");
    File outputFile1 =
        new File(outputDir1.getAbsolutePath() + "/" + "output-unpartitioned_table.parquet");
    outputFile1.createNewFile();
    File outputDir2 = tmpDir.newFolder("out", "partitioned_table", "p2_pid=partition");
    File outputFile2 =
        new File(outputDir2.getAbsolutePath() + "/" + "output-partitioned_table-partition.parquet");
    outputFile2.createNewFile();

    List<String> files = DataplexBigQueryToGcs.getFilesInDirectory(outDir.getAbsolutePath());
    assertThat(files.size()).isEqualTo(2);
  }

  private void insertTableData(String tableName, TableRow... records) throws Exception {
    fakeDatasetService.insertAll(
        tableByName.get(tableName).toTableReference(), Arrays.asList(records), null);
  }

  private void insertPartitionData(String tableName, String partitionName, TableRow... records)
      throws Exception {
    // Partition transform isn't supposed to read the table directly, but it's supposed to read the
    // table data with a SQL query (with a partition decorator). So we register expected sql with
    // some results here, instead of inserting the records into the table.

    String expectedSql =
        String.format("select * from [%s.%s.%s$%s]", PROJECT, DATASET, tableName, partitionName);
    fakeJobService.expectQuery(
        expectedSql, tableByName.get(tableName), bqSchema, Arrays.asList(records));
  }

  private void runTransform(String... tableNames) {
    BigQueryTable[] tables = new BigQueryTable[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      tables[i] = tableByName.get(tableNames[i]);
    }
    runTransform(tables);
  }

  private void runTransform(BigQueryTable... tables) {
    Pipeline p = Pipeline.create(options);
    DataplexBigQueryToGcs.transformPipeline(
        p, Arrays.asList(tables), options, outDir.getAbsolutePath(), bqFakeServices, () -> bqMock);
    p.run();
  }

  private static TableId tableId(String tableName) {
    return TableId.of(PROJECT, DATASET, tableName);
  }

  private static class CustomFakeJobService extends FakeJobService {
    private final Map<String, String> queryResults = new HashMap<>();

    protected void expectQuery(
        String sql, BigQueryTable table, TableSchema resultSchema, List<TableRow> results)
        throws IOException {
      expectDryRunQuery(
          PROJECT,
          sql,
          new JobStatistics()
              .setQuery(
                  new JobStatistics2()
                      .setReferencedTables(Collections.singletonList(table.toTableReference()))));

      // The following hack is needed because of what looks like a bug in FakeJobService.
      // At some point it expects the **query** string to contain base64-encoded query **results**,
      // so in startQueryJob we're replacing the query with the corresponding results, see:
      // https://github.com/apache/beam/blob/390b482963e162bfd1d28791138fa6af56f4e841/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/testing/FakeJobService.java#L472

      String tableString =
          BigQueryHelpers.toJsonString(
              new Table().setSchema(resultSchema).setTableReference(table.toTableReference()));

      KvCoder<String, List<TableRow>> coder =
          KvCoder.of(StringUtf8Coder.of(), ListCoder.of(TableRowJsonCoder.of()));
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      coder.encode(KV.of(tableString, results), os);
      String queryResult = new String(Base64.encodeBase64(os.toByteArray()));

      queryResults.put(sql, queryResult);
    }

    @Override
    public void startQueryJob(JobReference jobRef, JobConfigurationQuery query) {
      String queryResult = queryResults.get(query.getQuery());
      if (queryResult != null) {
        query.setQuery(queryResult);
      }
      super.startQueryJob(jobRef, query);
    }
  }
}
