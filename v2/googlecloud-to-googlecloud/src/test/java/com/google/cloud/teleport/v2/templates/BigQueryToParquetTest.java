/*
 * Copyright (C) 2019 Google LLC
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
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import com.google.cloud.teleport.v2.templates.BigQueryToParquet.BigQueryToParquetOptions;
import com.google.cloud.teleport.v2.templates.BigQueryToParquet.RecordSchemaAligner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.it.gcp.artifacts.utils.ParquetTestUtil;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.BigQueryServerStream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigQueryToParquet}. */
@RunWith(JUnit4.class)
public class BigQueryToParquetTest {

  private static final String PROJECT = "fantasmic-999999";
  private static final String DATASET = "great_data";
  private static final String TABLE = "table";
  private static final String TABLE_SPEC = PROJECT + ":" + DATASET + "." + TABLE;

  private static final List<CreateReadSessionRequest> CAPTURED_REQUESTS =
      new CopyOnWriteArrayList<>();

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();

  private FakeDatasetService fakeDatasetService;
  private TableSchema tableSchema;

  @Before
  public void setUp() throws Exception {
    CAPTURED_REQUESTS.clear();
    FakeDatasetService.setUp();
    fakeDatasetService = new FakeDatasetService();
    fakeDatasetService.createDataset(PROJECT, DATASET, "", "", null);

    tableSchema =
        new TableSchema()
            .setFields(
                new ArrayList<>(
                    Arrays.asList(
                        new TableFieldSchema()
                            .setName("device_id")
                            .setType("STRING")
                            .setMode("NULLABLE"),
                        new TableFieldSchema()
                            .setName("temperature_c")
                            .setType("FLOAT64")
                            .setMode("NULLABLE"),
                        new TableFieldSchema()
                            .setName("sample_time")
                            .setType("TIMESTAMP")
                            .setMode("NULLABLE"),
                        new TableFieldSchema()
                            .setName("sample_date")
                            .setType("DATE")
                            .setMode("NULLABLE"),
                        new TableFieldSchema()
                            .setName("amount")
                            .setType("NUMERIC")
                            .setMode("NULLABLE"),
                        new TableFieldSchema()
                            .setName("metadata")
                            .setType("RECORD")
                            .setMode("NULLABLE")
                            .setFields(
                                new ArrayList<>(
                                    Arrays.asList(
                                        new TableFieldSchema()
                                            .setName("location")
                                            .setType("STRING")
                                            .setMode("NULLABLE"),
                                        new TableFieldSchema()
                                            .setName("firmware_version")
                                            .setType("INT64")
                                            .setMode("REQUIRED")))),
                        new TableFieldSchema()
                            .setName("tags")
                            .setType("RECORD")
                            .setMode("REPEATED")
                            .setFields(
                                new ArrayList<>(
                                    Arrays.asList(
                                        new TableFieldSchema()
                                            .setName("key")
                                            .setType("STRING")
                                            .setMode("REQUIRED"),
                                        new TableFieldSchema()
                                            .setName("value")
                                            .setType("STRING")
                                            .setMode("NULLABLE")))))));

    fakeDatasetService.createTable(
        new Table()
            .setTableReference(
                new TableReference().setProjectId(PROJECT).setDatasetId(DATASET).setTableId(TABLE))
            .setNumBytes(1024L)
            .setSchema(tableSchema));
  }

  /** Test {@link BigQueryToParquet#getTableSchema} throws on invalid table reference. */
  @Test
  public void testGetTableSchemaBadTable() {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.setTableRef("fantasmic-999999;great_data.table");
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    assertThrows(
        IllegalArgumentException.class,
        () -> BigQueryToParquet.getTableSchema(services, options, Collections.emptyList()));
  }

  /** Test {@link BigQueryToParquet#getTableSchema} throws when the table does not exist. */
  @Test
  public void testGetTableSchemaMissingTable() {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.setTableRef(PROJECT + ":" + DATASET + ".missing_table");
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> BigQueryToParquet.getTableSchema(services, options, Collections.emptyList()));
    assertThat(ex).hasMessageThat().contains("not found");
  }

  /** Test full table schema conversion preserves Avro logical types. */
  @Test
  public void testGetTableSchemaFullTable() {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.setTableRef(TABLE_SPEC);
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    Schema schema = BigQueryToParquet.getTableSchema(services, options, Collections.emptyList());

    assertThat(schema.getFields()).hasSize(7);
    assertThat(unwrapNullable(schema.getField("device_id").schema()).getType())
        .isEqualTo(Schema.Type.STRING);
    assertThat(unwrapNullable(schema.getField("temperature_c").schema()).getType())
        .isEqualTo(Schema.Type.DOUBLE);
    assertThat(unwrapNullable(schema.getField("sample_time").schema()).getLogicalType())
        .isEqualTo(LogicalTypes.timestampMicros());
    assertThat(unwrapNullable(schema.getField("sample_date").schema()).getLogicalType())
        .isEqualTo(LogicalTypes.date());
    assertThat(unwrapNullable(schema.getField("amount").schema()).getLogicalType())
        .isEqualTo(LogicalTypes.decimal(38, 9));
    assertThat(unwrapNullable(schema.getField("metadata").schema()).getType())
        .isEqualTo(Schema.Type.RECORD);
    assertThat(schema.getField("tags").schema().getType()).isEqualTo(Schema.Type.ARRAY);
  }

  /** Test tableRef without project ID falls back to BigQueryOptions project / bigQueryProject. */
  @Test
  public void testGetTableSchemaDefaultProjectFallback() {
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    BigQueryToParquetOptions optionsWithProject =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    optionsWithProject.setTableRef(DATASET + "." + TABLE);
    optionsWithProject.as(BigQueryOptions.class).setProject(PROJECT);
    Schema schema =
        BigQueryToParquet.getTableSchema(services, optionsWithProject, Collections.emptyList());
    assertThat(schema.getFields()).hasSize(7);

    BigQueryToParquetOptions optionsWithBqProject =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    optionsWithBqProject.setTableRef(DATASET + "." + TABLE);
    optionsWithBqProject.as(BigQueryOptions.class).setProject("other-project");
    optionsWithBqProject.as(BigQueryOptions.class).setBigQueryProject(PROJECT);
    Schema schemaFromBqProject =
        BigQueryToParquet.getTableSchema(services, optionsWithBqProject, Collections.emptyList());
    assertThat(schemaFromBqProject.getFields()).hasSize(7);
  }

  /** Test schema trimming with top-level, nested, case-insensitive, and reordered fields. */
  @Test
  public void testGetTableSchemaSelectedFields() {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.setTableRef(TABLE_SPEC);
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    List<String> selectedFields =
        BigQueryToParquet.parseSelectedFields(" metadata.location , SAMPLE_TIME, device_id ");
    assertThat(selectedFields).containsExactly("metadata.location", "SAMPLE_TIME", "device_id");

    Schema schema = BigQueryToParquet.getTableSchema(services, options, selectedFields);
    assertThat(schema.getFields()).hasSize(3);
    assertThat(schema.getFields().get(0).name()).isEqualTo("device_id");
    assertThat(schema.getFields().get(1).name()).isEqualTo("sample_time");
    assertThat(schema.getFields().get(2).name()).isEqualTo("metadata");

    Schema metadataSchema = unwrapNullable(schema.getField("metadata").schema());
    assertThat(metadataSchema.getFields()).hasSize(1);
    assertThat(metadataSchema.getFields().get(0).name()).isEqualTo("location");
  }

  /** Test unknown selected fields throw IllegalArgumentException. */
  @Test
  public void testGetTableSchemaUnknownFieldThrows() {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.setTableRef(TABLE_SPEC);
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            BigQueryToParquet.getTableSchema(
                services, options, ImmutableList.of("device_id", "nonexistent_col")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BigQueryToParquet.getTableSchema(
                services, options, ImmutableList.of("metadata.nonexistent_sub")));
  }

  /**
   * Test {@link RecordSchemaAligner} aligns Storage API Avro records (with different record
   * names/namespaces and field ordering) so {@link AvroCoder} can encode/decode nullable and
   * repeated structs cleanly.
   */
  @Test
  public void testRecordSchemaAlignerRoundTripWithAvroCoder() throws Exception {
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.setTableRef(TABLE_SPEC);
    BigQueryServices services = new FakeBigQueryServices().withDatasetService(fakeDatasetService);
    Schema targetSchema =
        BigQueryToParquet.getTableSchema(services, options, Collections.emptyList());

    // Build a Storage-API-style Avro schema with "__root__" root name, no namespaces, and reversed
    // field order.
    Schema storageSchema = buildStorageApiStyleAvroSchema(/* reverseFields= */ true);
    GenericRecord storageRecord = buildStorageApiRecord(storageSchema, "dev-1", 21.5, true);

    RecordSchemaAligner aligner = new RecordSchemaAligner(targetSchema);
    GenericRecord aligned = aligner.apply(new SchemaAndRecord(storageRecord, tableSchema));

    // Verify AvroCoder encodes and decodes without UnresolvedUnionException.
    AvroCoder<GenericRecord> coder = AvroCoder.of(targetSchema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(aligned, out);
    GenericRecord decoded = coder.decode(new ByteArrayInputStream(out.toByteArray()));

    assertThat(decoded.get("device_id").toString()).isEqualTo("dev-1");
    assertThat(decoded.get("temperature_c")).isEqualTo(21.5);
    GenericRecord decodedMeta = (GenericRecord) decoded.get("metadata");
    assertThat(decodedMeta.get("location").toString()).isEqualTo("us-central1");
    assertThat(decodedMeta.get("firmware_version")).isEqualTo(42L);
    @SuppressWarnings("unchecked")
    List<GenericRecord> decodedTags = (List<GenericRecord>) decoded.get("tags");
    assertThat(decodedTags).hasSize(1);
    assertThat(decodedTags.get(0).get("key").toString()).isEqualTo("env");
    assertThat(decodedTags.get(0).get("value").toString()).isEqualTo("prod");

    // Also verify null nullable struct round-trips cleanly.
    GenericRecord nullMetaRecord = buildStorageApiRecord(storageSchema, "dev-2", 19.0, false);
    GenericRecord alignedNullMeta = aligner.apply(new SchemaAndRecord(nullMetaRecord, tableSchema));
    ByteArrayOutputStream out2 = new ByteArrayOutputStream();
    coder.encode(alignedNullMeta, out2);
    GenericRecord decodedNullMeta = coder.decode(new ByteArrayInputStream(out2.toByteArray()));
    assertThat(decodedNullMeta.get("device_id").toString()).isEqualTo("dev-2");
    assertThat(decodedNullMeta.get("metadata")).isNull();
  }

  /** End-to-end pipeline test writing Parquet files using Beam's GA Storage Read v1 client. */
  @Test
  public void testEndToEndPipelineFullTable() throws Exception {
    Schema storageSchema = buildStorageApiStyleAvroSchema(/* reverseFields= */ false);
    List<GenericRecord> inputRecords =
        ImmutableList.of(
            buildStorageApiRecord(storageSchema, "dev-1", 22.5, true),
            buildStorageApiRecord(storageSchema, "dev-2", 18.0, false));

    FakeStorageClient fakeStorageClient = new FakeStorageClient(storageSchema, inputRecords);
    BigQueryServices services =
        new FakeBigQueryServices()
            .withDatasetService(fakeDatasetService)
            .withStorageClient(fakeStorageClient);

    File outputDir = tmp.newFolder("parquet-full");
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.as(BigQueryOptions.class).setProject(PROJECT);
    options.setTableRef(TABLE_SPEC);
    options.setBucket(outputDir.getAbsolutePath());
    options.setNumShards(1);

    BigQueryToParquet.run(options, services).waitUntilFinish();

    // Verify BigQueryIO issued the CreateReadSession request via the GA v1 StorageClient.
    assertThat(CAPTURED_REQUESTS).hasSize(1);
    CreateReadSessionRequest req = CAPTURED_REQUESTS.get(0);
    assertThat(req.getReadSession().getTable())
        .isEqualTo("projects/" + PROJECT + "/datasets/" + DATASET + "/tables/" + TABLE);
    assertThat(req.getReadSession().getDataFormat()).isEqualTo(DataFormat.AVRO);

    List<GenericRecord> writtenRecords = readAllParquetRecords(outputDir);
    assertThat(writtenRecords).hasSize(2);
    GenericRecord dev1 = null;
    GenericRecord dev2 = null;
    for (GenericRecord rec : writtenRecords) {
      String id = rec.get("device_id").toString();
      if ("dev-1".equals(id)) {
        dev1 = rec;
      } else if ("dev-2".equals(id)) {
        dev2 = rec;
      }
    }
    assertThat(dev1).isNotNull();
    assertThat(dev2).isNotNull();

    assertThat(dev1.get("temperature_c")).isEqualTo(22.5);
    assertThat(dev1.get("sample_time")).isNotNull();
    assertThat(dev1.get("sample_date")).isNotNull();
    assertThat(dev1.get("amount")).isNotNull();
    GenericRecord dev1Meta = (GenericRecord) dev1.get("metadata");
    assertThat(dev1Meta).isNotNull();
    assertThat(dev1Meta.get("location").toString()).isEqualTo("us-central1");
    assertThat(dev1Meta.get("firmware_version")).isEqualTo(42L);
    @SuppressWarnings("unchecked")
    List<GenericRecord> dev1Tags = (List<GenericRecord>) dev1.get("tags");
    assertThat(dev1Tags).hasSize(1);
    assertThat(dev1Tags.get(0).get("key").toString()).isEqualTo("env");
    assertThat(dev1Tags.get(0).get("value").toString()).isEqualTo("prod");

    assertThat(dev2.get("temperature_c")).isEqualTo(18.0);
    assertThat(dev2.get("metadata")).isNull();
  }

  /** End-to-end pipeline test with --fields (reordered + whitespace) and --rowRestriction. */
  @Test
  public void testEndToEndPipelineWithSelectedFieldsAndRowRestriction() throws Exception {
    // Storage API returns selected fields in a different order than the table schema.
    Schema storageMetadataSchema =
        SchemaBuilder.record("__root___metadata").fields().optionalString("location").endRecord();
    Schema storageProjectedSchema =
        SchemaBuilder.record("__root__")
            .fields()
            .name("metadata")
            .type(SchemaBuilder.unionOf().nullType().and().type(storageMetadataSchema).endUnion())
            .withDefault(null)
            .optionalString("device_id")
            .endRecord();

    GenericRecord metaRec = new GenericData.Record(storageMetadataSchema);
    metaRec.put("location", "us-east1");
    GenericRecord row = new GenericData.Record(storageProjectedSchema);
    row.put("metadata", metaRec);
    row.put("device_id", "dev-9");

    FakeStorageClient fakeStorageClient =
        new FakeStorageClient(storageProjectedSchema, ImmutableList.of(row));
    BigQueryServices services =
        new FakeBigQueryServices()
            .withDatasetService(fakeDatasetService)
            .withStorageClient(fakeStorageClient);

    File outputDir = tmp.newFolder("parquet-projected");
    BigQueryToParquetOptions options =
        PipelineOptionsFactory.create().as(BigQueryToParquetOptions.class);
    options.as(BigQueryOptions.class).setProject(PROJECT);
    options.setTableRef(TABLE_SPEC);
    options.setBucket(outputDir.getAbsolutePath());
    options.setNumShards(1);
    options.setFields(" metadata.location , device_id ");
    options.setRowRestriction("device_id = 'dev-9'");

    BigQueryToParquet.run(options, services).waitUntilFinish();

    assertThat(CAPTURED_REQUESTS).hasSize(1);
    CreateReadSessionRequest req = CAPTURED_REQUESTS.get(0);
    assertThat(req.getReadSession().getReadOptions().getSelectedFieldsList())
        .containsExactly("metadata.location", "device_id")
        .inOrder();
    assertThat(req.getReadSession().getReadOptions().getRowRestriction())
        .isEqualTo("device_id = 'dev-9'");

    List<GenericRecord> writtenRecords = readAllParquetRecords(outputDir);
    assertThat(writtenRecords).hasSize(1);
    GenericRecord result = writtenRecords.get(0);
    assertThat(result.getSchema().getFields()).hasSize(2);
    assertThat(result.get("device_id").toString()).isEqualTo("dev-9");
    GenericRecord resultMeta = (GenericRecord) result.get("metadata");
    assertThat(resultMeta.get("location").toString()).isEqualTo("us-east1");
  }

  private static Schema unwrapNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      for (Schema branch : schema.getTypes()) {
        if (branch.getType() != Schema.Type.NULL) {
          return branch;
        }
      }
    }
    return schema;
  }

  private static Schema buildStorageApiStyleAvroSchema(boolean reverseFields) {
    Schema timestampMicros =
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimalType = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));

    Schema metadataRecord =
        SchemaBuilder.record("__root___metadata")
            .fields()
            .optionalString("location")
            .requiredLong("firmware_version")
            .endRecord();

    Schema tagRecord =
        SchemaBuilder.record("__root___tags")
            .fields()
            .requiredString("key")
            .optionalString("value")
            .endRecord();

    List<Schema.Field> fields =
        new ArrayList<>(
            Arrays.asList(
                new Schema.Field(
                    "device_id",
                    SchemaBuilder.unionOf().nullType().and().stringType().endUnion(),
                    null,
                    Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field(
                    "temperature_c",
                    SchemaBuilder.unionOf().nullType().and().doubleType().endUnion(),
                    null,
                    Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field(
                    "sample_time",
                    SchemaBuilder.unionOf().nullType().and().type(timestampMicros).endUnion(),
                    null,
                    Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field(
                    "sample_date",
                    SchemaBuilder.unionOf().nullType().and().type(dateType).endUnion(),
                    null,
                    Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field(
                    "amount",
                    SchemaBuilder.unionOf().nullType().and().type(decimalType).endUnion(),
                    null,
                    Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field(
                    "metadata",
                    SchemaBuilder.unionOf().nullType().and().type(metadataRecord).endUnion(),
                    null,
                    Schema.Field.NULL_DEFAULT_VALUE),
                new Schema.Field("tags", Schema.createArray(tagRecord), null, null)));
    if (reverseFields) {
      Collections.reverse(fields);
    }
    return Schema.createRecord("__root__", null, null, false, fields);
  }

  private static GenericRecord buildStorageApiRecord(
      Schema storageSchema, String deviceId, double tempC, boolean includeMetadata) {
    GenericRecord record = new GenericData.Record(storageSchema);
    record.put("device_id", deviceId);
    record.put("temperature_c", tempC);
    Instant ts = Instant.parse("2025-03-06T15:00:00Z");
    record.put("sample_time", ts.getEpochSecond() * 1_000_000L + ts.getNano() / 1_000L);
    record.put("sample_date", (int) LocalDate.of(2025, 3, 6).toEpochDay());

    Schema decimalSchema = unwrapNullable(storageSchema.getField("amount").schema());
    ByteBuffer encodedAmount =
        new Conversions.DecimalConversion()
            .toBytes(
                new BigDecimal("123.450000000"), decimalSchema, decimalSchema.getLogicalType());
    record.put("amount", encodedAmount);

    if (includeMetadata) {
      Schema metadataSchema = unwrapNullable(storageSchema.getField("metadata").schema());
      GenericRecord metadata = new GenericData.Record(metadataSchema);
      metadata.put("location", "us-central1");
      metadata.put("firmware_version", 42L);
      record.put("metadata", metadata);
    } else {
      record.put("metadata", null);
    }

    Schema tagsArraySchema = storageSchema.getField("tags").schema();
    GenericRecord tag = new GenericData.Record(tagsArraySchema.getElementType());
    tag.put("key", "env");
    tag.put("value", "prod");
    record.put("tags", ImmutableList.of(tag));
    return record;
  }

  private static List<GenericRecord> readAllParquetRecords(File outputDir) throws IOException {
    File[] files = outputDir.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertThat(files).isNotNull();
    assertThat(files.length).isGreaterThan(0);

    List<GenericRecord> records = new ArrayList<>();
    for (File file : files) {
      records.addAll(ParquetTestUtil.readRecords(Files.readAllBytes(file.toPath())));
    }
    return records;
  }

  private static class FakeStorageClient implements StorageClient, Serializable {
    private final String avroSchemaJson;
    private final byte[] serializedRows;
    private final int rowCount;

    FakeStorageClient(Schema schema, List<GenericRecord> records) throws IOException {
      this.avroSchemaJson = schema.toString();
      this.rowCount = records.size();
      GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      for (GenericRecord record : records) {
        writer.write(record, encoder);
      }
      encoder.flush();
      this.serializedRows = out.toByteArray();
    }

    @Override
    public ReadSession createReadSession(CreateReadSessionRequest request) {
      CAPTURED_REQUESTS.add(request);
      return ReadSession.newBuilder()
          .setName("projects/test/locations/us/sessions/test-session")
          .setDataFormat(DataFormat.AVRO)
          .setAvroSchema(AvroSchema.newBuilder().setSchema(avroSchemaJson))
          .addStreams(ReadStream.newBuilder().setName("test-stream-0"))
          .build();
    }

    @Override
    public BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request) {
      return readRows(request, "");
    }

    @Override
    public BigQueryServerStream<ReadRowsResponse> readRows(
        ReadRowsRequest request, String fullTableName) {
      ReadRowsResponse response =
          ReadRowsResponse.newBuilder()
              .setAvroRows(
                  AvroRows.newBuilder()
                      .setSerializedBinaryRows(ByteString.copyFrom(serializedRows))
                      .setRowCount(rowCount))
              .setRowCount(rowCount)
              .build();
      return new FakeBigQueryServerStream<>(ImmutableList.of(response));
    }

    @Override
    public SplitReadStreamResponse splitReadStream(SplitReadStreamRequest request) {
      return SplitReadStreamResponse.getDefaultInstance();
    }

    @Override
    public SplitReadStreamResponse splitReadStream(
        SplitReadStreamRequest request, String fullTableName) {
      return SplitReadStreamResponse.getDefaultInstance();
    }

    @Override
    public void close() {}
  }
}
