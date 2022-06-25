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
import static com.google.re2j.Pattern.CASE_INSENSITIVE;
import static com.google.re2j.Pattern.DOTALL;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
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
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaPartitionField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroSchema;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DataplexClientFactory;
import com.google.cloud.teleport.v2.options.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.utils.BigQueryMetadataLoader;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.Schemas;
import com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.cloud.teleport.v2.values.DataplexEnums.CompressionFormat;
import com.google.cloud.teleport.v2.values.DataplexEnums.EntityType;
import com.google.cloud.teleport.v2.values.DataplexEnums.FieldType;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageFormat;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageSystem;
import com.google.common.collect.ImmutableList;
import com.google.re2j.Pattern;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
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
  private static final String ASSET_NAME =
      "projects/test-project1/locations/us-central1/lakes/lake1/zones/zone1/assets/asset1";
  private static final int MAX_PARALLEL_REQUESTS = 5;

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final TestPipeline testPipeline = TestPipeline.create();

  // These have to be static, otherwise they won't be serialized properly when passed to transforms.
  @Mock private static BigQuery bqMock;
  @Mock private static DataplexClient dataplexClientMock;

  @Mock private BigQueryStorageClient bqsMock;
  @Mock private TableResult tableResultMock;

  private BigQueryMetadataLoader metadataLoader;
  private BigQueryServices bqFakeServices;
  private CustomFakeJobService fakeJobService;
  private FakeDatasetService fakeDatasetService;
  private DataplexClientFactory dataplexClientFactory;
  private DataplexBigQueryToGcsOptions options;
  private File outDir;
  private TableRow[] defaultRecords;
  private String[] defaultExpectedRecords;
  private TableSchema bqSchema;
  private Schema avroSchema;
  private GoogleCloudDataplexV1Schema dataplexPartitionedSchema;
  private GoogleCloudDataplexV1Schema dataplexUnpartitionedSchema;
  private GoogleCloudDataplexV1Entity partitionedEntityWithEmptySchema;
  private GoogleCloudDataplexV1Entity unpartitionedEntityWithEmptySchema;
  private Map<String, BigQueryTable> tableByName;

  @Before
  public void setUp() throws InterruptedException, IOException {
    options = TestPipeline.testingPipelineOptions().as(DataplexBigQueryToGcsOptions.class);
    options.setProject(PROJECT);
    options.setUpdateDataplexMetadata(true);
    options.setEnforceSamePartitionKey(false);
    options.setDestinationStorageBucketAssetName(ASSET_NAME);
    // Required when using BigQueryIO.withMethod(EXPORT).
    options.setTempLocation(tmpDir.newFolder("bqTmp").getAbsolutePath());

    outDir = tmpDir.newFolder("out");

    bqSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("ts").setType("TIMESTAMP"),
                    new TableFieldSchema().setName("s1").setType("STRING"),
                    new TableFieldSchema().setName("d1").setType("DATE"),
                    new TableFieldSchema().setName("t1").setType("TIME").setMode("REQUIRED"),
                    new TableFieldSchema().setName("dt").setType("DATETIME"),
                    new TableFieldSchema().setName("i1").setType("INTEGER")));

    avroSchema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
                    + "[{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                    + "{\"name\":\"s1\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},"
                    + "{\"name\":\"t1\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},"
                    + "{\"name\":\"dt\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"datetime\"}]},"
                    + "{\"name\":\"i1\",\"type\":[\"null\",\"long\"]}]}");

    dataplexUnpartitionedSchema =
        new GoogleCloudDataplexV1Schema()
            .setFields(
                Arrays.asList(
                    dataplexField("ts", "TIMESTAMP", "NULLABLE"),
                    dataplexField("s1", "STRING", "NULLABLE"),
                    dataplexField("d1", "DATE", "NULLABLE"),
                    dataplexField("t1", "TIME", "REQUIRED"),
                    // "datetime" is not actually a valid Avro logical type, so parsed as STRING:
                    dataplexField("dt", "STRING", "NULLABLE"),
                    dataplexField("i1", "INT64", "NULLABLE")));
    dataplexPartitionedSchema =
        new GoogleCloudDataplexV1Schema()
            .setFields(
                Arrays.asList(
                    dataplexField("s1", "STRING", "NULLABLE"),
                    dataplexField("d1", "DATE", "NULLABLE"),
                    dataplexField("t1", "TIME", "REQUIRED"),
                    // "datetime" is not actually a valid Avro logical type, so parsed as STRING:
                    dataplexField("dt", "STRING", "NULLABLE"),
                    dataplexField("i1", "INT64", "NULLABLE")))
            .setPartitionFields(Arrays.asList(dataplexPartitionField("ts", "TIMESTAMP")));

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
            .setDataplexEntityName("partitioned_table_entity")
            .build();

    BigQueryTable t2 =
        BigQueryTable.builder()
            .setTableName("unpartitioned_table")
            .setProject(PROJECT)
            .setDataset(DATASET)
            .setSchema(avroSchema)
            .setLastModificationTime(modTime)
            .setDataplexEntityName("unpartitioned_table_entity")
            .build();

    tableByName = new HashMap<>();
    tableByName.put(t1.getTableName(), t1);
    tableByName.put(t2.getTableName(), t2);

    defaultRecords =
        new TableRow[] {
          new TableRow()
              .set("ts", 1L)
              .set("s1", "1001")
              .set("d1", "1970-01-01")
              .set("t1", "00:00:00.000001")
              .set("dt", "2020-01-01T00:42:00.123")
              .set("i1", 2001L),
          new TableRow()
              .set("ts", 2L)
              .set("s1", "1002")
              .set("d1", "1970-01-02")
              .set("t1", "00:00:00.000002")
              .set("dt", "2020-01-02T00:42:00.123")
              .set("i1", 2002L),
          new TableRow()
              .set("ts", 3L)
              .set("s1", "1003")
              .set("d1", "1970-01-03")
              .set("t1", "00:00:00.000003")
              .set("dt", "2020-01-03T00:42:00.123")
              .set("i1", 2003L),
          new TableRow()
              .set("ts", 4L)
              .set("s1", "1004")
              .set("d1", "1970-01-04")
              .set("t1", "00:00:00.000004")
              .set("dt", "2020-01-04T00:42:00.123")
              .set("i1", null),
          new TableRow()
              .set("ts", 5L)
              .set("s1", "1005")
              .set("d1", "1970-01-05")
              .set("t1", "00:00:00.000005")
              .set("dt", "2020-01-05T00:42:00.123")
              .set("i1", 2005L)
        };

    defaultExpectedRecords =
        new String[] {
          "{\"ts\": 1, \"s1\": \"1001\", \"d1\": 0, \"t1\": 1, \"dt\": \"2020-01-01T00:42:00.123\","
              + " \"i1\": 2001}",
          "{\"ts\": 2, \"s1\": \"1002\", \"d1\": 1, \"t1\": 2, \"dt\": \"2020-01-02T00:42:00.123\","
              + " \"i1\": 2002}",
          "{\"ts\": 3, \"s1\": \"1003\", \"d1\": 2, \"t1\": 3, \"dt\": \"2020-01-03T00:42:00.123\","
              + " \"i1\": 2003}",
          "{\"ts\": 4, \"s1\": \"1004\", \"d1\": 3, \"t1\": 4, \"dt\": \"2020-01-04T00:42:00.123\","
              + " \"i1\": null}",
          "{\"ts\": 5, \"s1\": \"1005\", \"d1\": 4, \"t1\": 5, \"dt\": \"2020-01-05T00:42:00.123\","
              + " \"i1\": 2005}"
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

    when(tableResultMock.iterateAll())
        .thenReturn(Collections.singleton(fields("unpartitioned_table", "0", null)));
    when(bqMock.query(any())).thenReturn(tableResultMock);
    when(bqMock.delete(any(TableId.class))).thenReturn(true);
    when(bqsMock.createReadSession(any()))
        .thenReturn(
            ReadSession.newBuilder()
                .setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchema.toString()))
                .build());

    metadataLoader = new BigQueryMetadataLoader(bqMock, bqsMock, MAX_PARALLEL_REQUESTS);

    partitionedEntityWithEmptySchema =
        defaultDataplexEntity()
            .setName("partitioned_table_entity")
            .setId("partitioned_table")
            .setDataPath("gs://test-bucket1/partitioned_table");
    unpartitionedEntityWithEmptySchema =
        defaultDataplexEntity()
            .setName("unpartitioned_table_entity")
            .setId("unpartitioned_table")
            .setDataPath("gs://test-bucket1/unpartitioned_table");
    Map<String, GoogleCloudDataplexV1Entity> entities =
        Stream.of(partitionedEntityWithEmptySchema, unpartitionedEntityWithEmptySchema)
            .collect(Collectors.toMap(GoogleCloudDataplexV1Entity::getName, Function.identity()));

    GoogleCloudDataplexV1Partition fakePartition =
        new GoogleCloudDataplexV1Partition()
            .setName("fake_partition_name")
            .setLocation("gs://test-bucket1/fake_table/fake_partition=20220101/fake_file")
            .setValues(Arrays.asList("20220101"));

    when(dataplexClientMock.listEntities(eq(DataplexUtils.getZoneFromAsset(ASSET_NAME)), any()))
        .thenAnswer(
            invocation ->
                ImmutableList.of(
                    partitionedEntityWithEmptySchema.clone(),
                    unpartitionedEntityWithEmptySchema.clone()));
    when(dataplexClientMock.getEntities(any()))
        .thenAnswer(
            invocation -> {
              List<String> names = invocation.getArgument(0);
              return ImmutableList.copyOf(
                  names.stream().map(s -> entities.get(s).clone()).collect(Collectors.toList()));
            });
    when(dataplexClientMock.createPartition(any(), any())).thenReturn(fakePartition);
    when(dataplexClientMock.createOrUpdatePartition(any(), any())).thenReturn(fakePartition);
    dataplexClientFactory = () -> dataplexClientMock;
  }

  @Test
  @Category(NeedsRunner.class)
  public void testE2E_mainPathWithAllStepsEnabled() throws Exception {
    when(bqMock.query(any())).thenReturn(new EmptyTableResult(null));

    insertTableData("unpartitioned_table", defaultRecords);
    insertPartitionData("partitioned_table", "p1", Arrays.copyOfRange(defaultRecords, 0, 2));
    insertPartitionData("partitioned_table", "p2", Arrays.copyOfRange(defaultRecords, 2, 5));
    // Some data is inserted into p3 just to check that it actually does NOT get exported.
    // The partitioned_table BigQueryTable object doesn't actually have p3 in the partition list.
    insertPartitionData("partitioned_table", "p3", defaultRecords);

    options.setDeleteSourceData(true);
    options.setUpdateDataplexMetadata(true);

    runTransform("unpartitioned_table", "partitioned_table");

    // Verify metadata was correctly updated in Dataplex:

    verify(dataplexClientMock, times(1))
        .updateEntity(
            partitionedEntityWithEmptySchema
                .clone()
                .setFormat(
                    new GoogleCloudDataplexV1StorageFormat()
                        .setMimeType(StorageFormat.PARQUET.getMimeType())
                        .setCompressionFormat(
                            CompressionFormat.COMPRESSION_FORMAT_UNSPECIFIED.name()))
                .setSchema(
                    cloneAndApplyHiveStyle(dataplexPartitionedSchema, "ts", "ts_pid")
                        .setUserManaged(true)));
    verify(dataplexClientMock, times(1))
        .updateEntity(
            unpartitionedEntityWithEmptySchema
                .setFormat(
                    new GoogleCloudDataplexV1StorageFormat()
                        .setMimeType(StorageFormat.PARQUET.getMimeType())
                        .setCompressionFormat(
                            CompressionFormat.COMPRESSION_FORMAT_UNSPECIFIED.name()))
                .clone()
                .setSchema(dataplexUnpartitionedSchema.clone().setUserManaged(true)));
    // Don't know the generated temp file names used for partition.getLocation(),
    // so need a custom arg matcher to verify values only:
    verify(dataplexClientMock, times(1))
        .createOrUpdatePartition(
            eq("partitioned_table_entity"),
            argThat(p -> p.getValues().size() == 1 && p.getValues().get(0).equals("p1")));
    verify(dataplexClientMock, times(1))
        .createOrUpdatePartition(
            eq("partitioned_table_entity"),
            argThat(p -> p.getValues().size() == 1 && p.getValues().get(0).equals("p2")));

    // Verify source BQ tables were deleted:

    verify(bqMock, times(1))
        .query(
            QueryJobConfiguration.newBuilder(
                    "truncate table `test-project1.test-dataset1.unpartitioned_table`")
                .build());
    verify(bqMock, times(1)).delete(tableId("partitioned_table$p1"));
    verify(bqMock, times(1)).delete(tableId("partitioned_table$p2"));
    verifyNoMoreInteractions(bqMock);

    // Verify pipeline output:

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
    options.setFileFormat(FileFormatOptions.AVRO);
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
  @Category(NeedsRunner.class)
  public void testE2E_withEnforceSamePartitionKeyEnabled_producesRenamedColumns() throws Exception {
    options.setEnforceSamePartitionKey(true);
    options.setFileFormat(FileFormatOptions.AVRO);

    insertPartitionData("partitioned_table", "p1", Arrays.copyOfRange(defaultRecords, 0, 2));
    insertPartitionData("partitioned_table", "p2", Arrays.copyOfRange(defaultRecords, 2, 5));

    runTransform("partitioned_table");

    Schema targetFileSchema =
        Schemas.avroSchemaFromDataFile(
            outDir.getAbsolutePath() + "/partitioned_table/ts=p1/output-partitioned_table-p1.avro");

    // We extract Avro schema from the target data file and use it below instead of the manually
    // created avroSchema (used in other tests)) to double-check the schema written to the file
    // has the renamed ts_pkey column name and not the original "ts" name.
    // Otherwise, AvroIO will just read the data file using whatever schema we manually provide
    // and using the field names *we* provide and not those written to the file (it still works
    // if the number of fields and their order/type match).

    PCollection<String> actualRecords1 =
        testPipeline
            .apply(
                "readP1",
                AvroIO.readGenericRecords(targetFileSchema)
                    .from(
                        outDir.getAbsolutePath()
                            + "/partitioned_table/ts=p1/output-partitioned_table-p1.avro"))
            .apply("mapP1", MapElements.into(TypeDescriptors.strings()).via(Object::toString));
    PCollection<String> actualRecords2 =
        testPipeline
            .apply(
                "readP2",
                AvroIO.readGenericRecords(targetFileSchema)
                    .from(
                        outDir.getAbsolutePath()
                            + "/partitioned_table/ts=p2/output-partitioned_table-p2.avro"))
            .apply("mapP2", MapElements.into(TypeDescriptors.strings()).via(Object::toString));

    // Column "ts" should've been renamed to "ts_pkey":

    String[] expectedRecords1 =
        new String[] {
          "{\"ts_pkey\": 1, \"s1\": \"1001\", \"d1\": 0, \"t1\": 1, \"dt\":"
              + " \"2020-01-01T00:42:00.123\", \"i1\": 2001}",
          "{\"ts_pkey\": 2, \"s1\": \"1002\", \"d1\": 1, \"t1\": 2, \"dt\":"
              + " \"2020-01-02T00:42:00.123\", \"i1\": 2002}"
        };
    String[] expectedRecords2 =
        new String[] {
          "{\"ts_pkey\": 3, \"s1\": \"1003\", \"d1\": 2, \"t1\": 3, \"dt\":"
              + " \"2020-01-03T00:42:00.123\", \"i1\": 2003}",
          "{\"ts_pkey\": 4, \"s1\": \"1004\", \"d1\": 3, \"t1\": 4, \"dt\":"
              + " \"2020-01-04T00:42:00.123\", \"i1\": null}",
          "{\"ts_pkey\": 5, \"s1\": \"1005\", \"d1\": 4, \"t1\": 5, \"dt\":"
              + " \"2020-01-05T00:42:00.123\", \"i1\": 2005}"
        };

    PAssert.that(actualRecords1).containsInAnyOrder(expectedRecords1);
    PAssert.that(actualRecords2).containsInAnyOrder(expectedRecords2);

    // Verify that "_pid "is *not* appended in the *file path* if enforceSamePartitionKey = true,
    // i.e. the below files should not have been created:

    PCollection<String> actualNonExistingRecords =
        testPipeline
            .apply(
                "readFiles",
                AvroIO.readGenericRecords(targetFileSchema)
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
                    .from(outDir.getAbsolutePath() + "/partitioned_table/ts_pid=p1/*.avro"))
            .apply("mapFiles", MapElements.into(TypeDescriptors.strings()).via(Object::toString));

    PAssert.that(actualNonExistingRecords).empty();

    testPipeline.run();
  }

  @Test
  public void testE2E_withTargetStrategyFail_throwsException() throws Exception {
    options.setFileFormat(FileFormatOptions.PARQUET);
    options.setWriteDisposition(WriteDispositionOptions.FAIL);

    writeOutputFile("unpartitioned_table", "output-unpartitioned_table.parquet", "Test data");

    try {
      DataplexBigQueryToGcs.buildPipeline(
          options,
          metadataLoader,
          dataplexClientMock,
          outDir.getAbsolutePath(),
          DatasetId.of(PROJECT, DATASET));
      fail("Expected a WriteDispositionException");
    } catch (Exception e) {
      assertThat(e).hasCauseThat().hasCauseThat().isInstanceOf(WriteDispositionException.class);
    }
  }

  private static final Pattern TABLE_QUERY_PATTERN =
      Pattern.compile(
          "select.*table_id.*last_modified_time.*partitioning_column", CASE_INSENSITIVE | DOTALL);
  private static final Pattern PARTITION_QUERY_PATTERN =
      Pattern.compile("select.*partition_id.*last_modified_time", CASE_INSENSITIVE | DOTALL);

  /**
   * Tests that the pipeline throws an exception if {@code writeDisposition = FAIL}, {@code
   * enforceSamePartitionKey = true}, and one of the target files exist, when processing a
   * partitioned table.
   *
   * <p>This is a special case because depending on the {@code enforceSamePartitionKey} param the
   * generated file path can be different (for partitioned tables only!), so this verifies that
   * {@link com.google.cloud.teleport.v2.utils.DataplexBigQueryToGcsFilter
   * DataplexBigQueryToGcsFilter} can find such files correctly.
   */
  @Test
  public void testE2E_withTargetStrategyFail_andEnforceSamePartitionKeyEnabled_throwsException()
      throws Exception {
    options.setFileFormat(FileFormatOptions.PARQUET);
    options.setWriteDisposition(WriteDispositionOptions.FAIL);
    options.setEnforceSamePartitionKey(true);

    writeOutputFile("partitioned_table/ts=p2", "output-partitioned_table-p2.parquet", "Test data");

    when(bqMock.query(any()))
        .then(
            invocation -> {
              Iterable<FieldValueList> result = null;
              QueryJobConfiguration q = (QueryJobConfiguration) invocation.getArguments()[0];
              if (TABLE_QUERY_PATTERN.matcher(q.getQuery()).find()) {
                result = Arrays.asList(fields("partitioned_table", "0", "ts"));
              } else if (PARTITION_QUERY_PATTERN.matcher(q.getQuery()).find()) {
                result = Arrays.asList(fields("p1", "0"), fields("p2", "0"));
              }
              when(tableResultMock.iterateAll()).thenReturn(result);
              return tableResultMock;
            });

    try {
      DataplexBigQueryToGcs.buildPipeline(
          options,
          metadataLoader,
          dataplexClientMock,
          outDir.getAbsolutePath(),
          DatasetId.of(PROJECT, DATASET));
      fail("Expected a WriteDispositionException");
    } catch (Exception e) {
      assertThat(e).hasCauseThat().hasCauseThat().isInstanceOf(WriteDispositionException.class);
      assertThat(e)
          .hasCauseThat()
          .hasCauseThat()
          .hasMessageThat()
          .contains(
              "Target File partitioned_table/ts=p2/output-partitioned_table-p2.parquet exists for"
                  + " partitioned_table$p2.");
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testE2E_withTargetStrategySkip_skipsTable() throws Exception {
    options.setFileFormat(FileFormatOptions.PARQUET);
    options.setWriteDisposition(WriteDispositionOptions.SKIP);
    File outputFile =
        writeOutputFile("unpartitioned_table", "output-unpartitioned_table.parquet", "Test data");

    Pipeline p =
        DataplexBigQueryToGcs.buildPipeline(
            options,
            metadataLoader,
            dataplexClientMock,
            outDir.getAbsolutePath(),
            DatasetId.of(PROJECT, DATASET));
    p.run();
    testPipeline.run();

    // Checking to see if the file was skipped and data was not overwritten
    assertThat(readFirstLine(outputFile)).isEqualTo("Test data");
  }

  /**
   * Verifies that given a list of BQ tables, loadDataplexMetadata() loads and updates the names of
   * the corresponding entities in Dataplex.
   *
   * <p>Also checks that non-existing entities are created, and already existing entities are not
   * re-created and their name is re-used.
   */
  @Test
  public void test_loadDataplexMetadata_populatesEntityName() throws IOException {
    options.setEnforceSamePartitionKey(true);

    String zoneName = DataplexUtils.getZoneFromAsset(ASSET_NAME);
    String shortAssetName = DataplexUtils.getShortAssetNameFromAsset(ASSET_NAME);
    BigQueryTable t1 = tableByName.get("partitioned_table");
    BigQueryTable t2 = tableByName.get("unpartitioned_table");
    BigQueryTable t3 = t1.toBuilder().setTableName("new_table").build();

    GoogleCloudDataplexV1Entity existingEntity1 =
        defaultDataplexEntity()
            .setName("partitioned_table_entity")
            .setId("partitioned_table")
            .setDataPath("gs://target_bucket/partitioned_table");
    GoogleCloudDataplexV1Entity existingEntity2 =
        defaultDataplexEntity()
            .setName("unpartitioned_table_entity")
            .setId("unpartitioned_table")
            .setDataPath("gs://target_bucket/unpartitioned_table");
    // This is an extra entity returned by Mock Dataplex with a different dataPath,
    // to verify only entities with matching dataPath are picked up:
    GoogleCloudDataplexV1Entity unusedEntity =
        defaultDataplexEntity()
            .setName("should_not_be_used")
            .setId("partitioned_table")
            .setDataPath("gs://wrong_bucket/partitioned_table");
    when(dataplexClientMock.listEntities(eq(DataplexUtils.getZoneFromAsset(ASSET_NAME)), any()))
        .thenAnswer(
            invocation -> {
              String filter = invocation.getArgument(1);
              if (filter.equals("asset=" + shortAssetName)) {
                return ImmutableList.of(
                    existingEntity1.clone(), existingEntity2.clone(), unusedEntity.clone());
              } else {
                return ImmutableList.of();
              }
            });
    when(dataplexClientMock.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity entity = invocation.getArgument(1);
              return entity.clone().setName("generated_name-" + entity.getId());
            });

    List<BigQueryTable> actualTables =
        DataplexBigQueryToGcs.loadDataplexMetadata(
            options, dataplexClientMock, Arrays.asList(t1, t2, t3), "gs://target_bucket");

    List<BigQueryTable> expectedTables =
        Arrays.asList(
            t1.toBuilder().setDataplexEntityName("partitioned_table_entity").build(),
            t2.toBuilder().setDataplexEntityName("unpartitioned_table_entity").build(),
            t3.toBuilder().setDataplexEntityName("generated_name-new_table").build());
    GoogleCloudDataplexV1Entity expectedNewEntity =
        defaultDataplexEntity()
            .setId("new_table")
            .setDataPath("gs://target_bucket/new_table")
            .setSchema(cloneAndApplyHiveStyle(dataplexPartitionedSchema).setUserManaged(true));

    assertThat(actualTables).isEqualTo(expectedTables);
    verify(dataplexClientMock, times(1)).createEntity(zoneName, expectedNewEntity);
    verify(dataplexClientMock, atLeastOnce()).listEntities(any(), any());
    // Make sure no other entities were created:
    verify(dataplexClientMock, times(1)).createEntity(any(), any());
  }

  @Test
  public void
      test_loadDataplexMetadata_withEnforcedSamePartitionKeyDisabled_renamesPartitionKeyInDataplexSchema()
          throws IOException {
    options.setEnforceSamePartitionKey(false);
    String zoneName = DataplexUtils.getZoneFromAsset(ASSET_NAME);
    BigQueryTable table = tableByName.get("partitioned_table");

    when(dataplexClientMock.listEntities(any(), any())).thenReturn(ImmutableList.of());
    when(dataplexClientMock.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity entity = invocation.getArgument(1);
              return entity.clone().setName("generated_name-" + entity.getId());
            });

    DataplexBigQueryToGcs.loadDataplexMetadata(
        options, dataplexClientMock, Arrays.asList(table), "gs://target_bucket");

    GoogleCloudDataplexV1Entity expectedNewEntity =
        defaultDataplexEntity()
            .setId("partitioned_table")
            .setDataPath("gs://target_bucket/partitioned_table")
            .setSchema(
                cloneAndApplyHiveStyle(dataplexPartitionedSchema, "ts", "ts_pid")
                    .setUserManaged(true));
    verify(dataplexClientMock, times(1)).createEntity(zoneName, expectedNewEntity);
    // Make sure no other entities were created:
    verify(dataplexClientMock, times(1)).createEntity(any(), any());
  }

  private String readFirstLine(File outputFile) throws FileNotFoundException {
    Scanner fileReader = new Scanner(outputFile);
    String result = fileReader.nextLine();
    fileReader.close();
    return result;
  }

  private File writeOutputFile(String folderName, String filename, String data) throws IOException {
    File outputDir = tmpDir.newFolder("out", folderName);
    outputDir.mkdirs();
    File outputFile = new File(outputDir.getAbsolutePath() + "/" + filename);
    outputFile.createNewFile();
    FileWriter writer = new FileWriter(outputFile);
    writer.write(data);
    writer.close();
    return outputFile;
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
        p,
        Arrays.asList(tables),
        options,
        outDir.getAbsolutePath(),
        dataplexClientFactory,
        bqFakeServices,
        () -> bqMock);
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

  private static FieldValueList fields(Object... fieldValues) {
    List<FieldValue> list = new ArrayList<>(fieldValues.length);
    for (Object fieldValue : fieldValues) {
      list.add(FieldValue.of(FieldValue.Attribute.RECORD, fieldValue));
    }
    return FieldValueList.of(list);
  }

  private static GoogleCloudDataplexV1Entity defaultDataplexEntity() {
    return new GoogleCloudDataplexV1Entity()
        .setAsset(DataplexUtils.getShortAssetNameFromAsset(ASSET_NAME))
        .setType(EntityType.TABLE.name())
        .setSystem(StorageSystem.CLOUD_STORAGE.name())
        .setSchema(new GoogleCloudDataplexV1Schema().setUserManaged(true))
        .setFormat(
            new GoogleCloudDataplexV1StorageFormat()
                .setMimeType(StorageFormat.PARQUET.getMimeType())
                .setCompressionFormat(CompressionFormat.COMPRESSION_FORMAT_UNSPECIFIED.name()));
  }

  private static GoogleCloudDataplexV1Schema cloneAndApplyHiveStyle(
      GoogleCloudDataplexV1Schema schema) {
    return cloneAndApplyHiveStyle(schema, null, null);
  }

  private static GoogleCloudDataplexV1Schema cloneAndApplyHiveStyle(
      GoogleCloudDataplexV1Schema schema,
      String originalPartitionKey,
      String renameToPartitionKey) {
    GoogleCloudDataplexV1Schema result = schema.clone();
    DataplexUtils.applyHiveStyle(
        result, originalPartitionKey, renameToPartitionKey, FieldType.STRING);
    return result;
  }

  private static GoogleCloudDataplexV1SchemaSchemaField dataplexField(
      String name, String type, String mode) {
    return new GoogleCloudDataplexV1SchemaSchemaField().setName(name).setType(type).setMode(mode);
  }

  private static GoogleCloudDataplexV1SchemaPartitionField dataplexPartitionField(
      String name, String type) {
    return new GoogleCloudDataplexV1SchemaPartitionField().setName(name).setType(type);
  }
}
