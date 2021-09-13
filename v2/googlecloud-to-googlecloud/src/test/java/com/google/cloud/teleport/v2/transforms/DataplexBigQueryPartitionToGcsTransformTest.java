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
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.templates.DataplexBigQueryToGcs.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.transforms.AbstractDataplexBigQueryToGcsTransform.FileFormat;
import com.google.cloud.teleport.v2.transforms.AbstractDataplexBigQueryToGcsTransform.Options;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
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
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
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

/** Unit tests for {@link DataplexBigQueryPartitionToGcsTransform}. */
@RunWith(JUnit4.class)
public class DataplexBigQueryPartitionToGcsTransformTest {
  private static final String PROJECT = "test-project1";
  private static final String DATASET = "test-dataset1";
  private static final String TABLE = "tableA-partitioned";

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Rule public final TestPipeline testPipeline = TestPipeline.create();

  @Mock private static BigQuery bqMock;
  private BigQueryServices bqFakeServices;
  private CustomFakeJobService fakeJobService;
  private FakeDatasetService fakeDatasetService;
  private DataplexBigQueryToGcsOptions options;
  private BigQueryTable table;
  private BigQueryTablePartition fakePartition;
  private File outDir;
  private List<TableRow> defaultPartitionRecords;
  private List<String> defaultExpectedRecords;
  private TableSchema bqSchema;
  private Schema avroSchema;

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

    fakePartition =
        BigQueryTablePartition.builder()
            .setPartitionName("p2")
            .setLastModificationTime(modTime)
            .build();

    table =
        BigQueryTable.builder()
            .setTableName(TABLE)
            .setDatasetId(DatasetId.of(PROJECT, DATASET))
            .setSchema(avroSchema)
            .setLastModificationTime(modTime)
            .setPartitioningColumn("ts")
            .build();

    defaultPartitionRecords =
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
        new Table()
            .setTableReference(table.toTableReference())
            .setSchema(bqSchema)
            .setRequirePartitionFilter(true)
            .setTimePartitioning(new TimePartitioning().setField("ts").setType("DAY")));
    fakeJobService = new CustomFakeJobService();
    bqFakeServices =
        new FakeBigQueryServices()
            .withJobService(fakeJobService)
            .withDatasetService(fakeDatasetService);

    when(bqMock.query(any())).thenReturn(new EmptyTableResult(null));
    when(bqMock.delete(any(TableId.class))).thenReturn(true);
  }

  @Test
  public void testTargetPathContainsPartitionNameAndValue() {
    DataplexBigQueryPartitionToGcsTransform t =
        new DataplexBigQueryPartitionToGcsTransform(options, "test-bucket1", table, fakePartition);

    String expectedPath =
        String.format(
            "gs://test-bucket1/%s/ts=%s", table.getTableName(), fakePartition.getPartitionName());
    assertThat(t.getTargetPath()).isEqualTo(expectedPath);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_mainPathWithAllStepsEnabled() throws Exception {
    insertDefaultPartitionData();
    options.setDeleteSourceData(true);
    options.setUpdateDataplexMetadata(true);

    runTransform(options, table, fakePartition);

    String expectedDeletedPartitionName =
        table.getTableName() + "$" + fakePartition.getPartitionName();
    verify(bqMock, times(1)).delete(TableId.of(PROJECT, DATASET, expectedDeletedPartitionName));
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
    insertDefaultPartitionData();
    options.setFileFormat(FileFormat.AVRO);

    runTransform(options, table, fakePartition);

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
  public void testTransform_withDeleteSourceDataDefault_doesntTruncatePartition() throws Exception {
    insertDefaultPartitionData();

    runTransform(options, table, fakePartition);

    verifyNoMoreInteractions(bqMock);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataDisabled_doesntTruncatePartition()
      throws Exception {
    insertDefaultPartitionData();
    options.setDeleteSourceData(false);

    runTransform(options, table, fakePartition);

    verifyNoMoreInteractions(bqMock);
  }

  /**
   * Test that DataplexBigQueryPartitionToGcsTransform doesn't attempt to delete special BigQuery
   * partitions even if {@code deleteSourceData = true}.
   *
   * <p>As per <a
   * href="https://cloud.google.com/bigquery/docs/managing-partitioned-tables#delete_a_partition">
   * this documentation</a>, special partitions "__NULL__" and "__UNPARTITIONED__" cannot be
   * deleted.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testTransform_withDeleteSourceDataEnabled_doesntTruncateSpecialPartitions()
      throws Exception {
    options.setDeleteSourceData(true);

    BigQueryTablePartition.Builder builder =
        BigQueryTablePartition.builder().setLastModificationTime(System.currentTimeMillis() * 1000);
    BigQueryTablePartition p1 = builder.setPartitionName("__NULL__").build();
    BigQueryTablePartition p2 = builder.setPartitionName("__UNPARTITIONED__").build();
    BigQueryTablePartition p3 = builder.setPartitionName("NORMAL_PARTITION").build();
    insertDefaultPartitionData(p1);
    insertDefaultPartitionData(p2);
    insertDefaultPartitionData(p3);

    runTransform(options, table, p1);
    runTransform(options, table, p2);
    runTransform(options, table, p3);

    String expectedDeletedPartitionName = table.getTableName() + "$NORMAL_PARTITION";
    verify(bqMock, times(1)).delete(TableId.of(PROJECT, DATASET, expectedDeletedPartitionName));
    verifyNoMoreInteractions(bqMock);
  }

  private void insertDefaultPartitionData() throws Exception {
    insertDefaultPartitionData(fakePartition);
  }

  private void insertDefaultPartitionData(BigQueryTablePartition targetPartition) throws Exception {
    // This transform isn't supposed to read the table directly, but it's supposed to read the table
    // data with a SQL query (with a partition decorator). So we register expected sql with some
    // results here, instead of inserting the records into the table.

    String expectedSql =
        String.format(
            "select * from [%s.%s.%s$%s]",
            PROJECT, DATASET, table.getTableName(), targetPartition.getPartitionName());
    fakeJobService.expectQuery(expectedSql, table, bqSchema, defaultPartitionRecords);
  }

  private void runTransform(
      Options options, BigQueryTable table, BigQueryTablePartition partition) {
    Pipeline p = Pipeline.create(options);
    p.apply(
        new TestDataplexBigQueryPartitionToGcsTransform(
            options, table, outDir, bqFakeServices, partition));
    p.run();
  }

  private static class TestDataplexBigQueryPartitionToGcsTransform
      extends DataplexBigQueryPartitionToGcsTransform {

    private final String targetPath;
    private final BigQueryServices testBqServices;

    public TestDataplexBigQueryPartitionToGcsTransform(
        Options options,
        BigQueryTable table,
        File targetDir,
        BigQueryServices testBqServices,
        BigQueryTablePartition partition) {
      // Bucket used in getTargetPath() doesn't matter as we're overriding this method anyway.
      super(options, "non-existing-bucket", table, partition);
      this.targetPath = targetDir.getAbsolutePath();
      this.testBqServices = testBqServices;
    }

    protected String getOriginalTargetPath() {
      return super.getTargetPath();
    }

    @Override
    protected String getTargetPath() {
      return targetPath;
    }

    @Override
    protected TypedRead<GenericRecord> getBigQueryRead() {
      return super.getBigQueryRead().withTestServices(testBqServices).withoutValidation();
    }

    @Override
    BigQuery createBqClient() {
      return bqMock;
    }
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
