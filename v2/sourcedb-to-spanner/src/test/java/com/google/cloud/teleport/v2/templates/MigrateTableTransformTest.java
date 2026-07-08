/*
 * Copyright (C) 2024 Google LLC
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.reader.ReaderImpl;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.reader.io.schema.typemapping.UnifiedTypeMapper;
import com.google.cloud.teleport.v2.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link MigrateTableTransform}. */
@RunWith(JUnit4.class)
public class MigrateTableTransformTest {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  /** Tests that metric names are correctly generated, optionally including the shard ID. */
  @Test
  public void testGetMetricName() {
    assertThat(MigrateTableTransform.getMetricName(null))
        .isEqualTo(MigrateTableTransform.GCS_RECORDS_WRITTEN);
    assertThat(MigrateTableTransform.getMetricName(""))
        .isEqualTo(MigrateTableTransform.GCS_RECORDS_WRITTEN);
    assertThat(MigrateTableTransform.getMetricName("shard1"))
        .isEqualTo(MigrateTableTransform.GCS_RECORDS_WRITTEN + "_shard1");
  }

  /** Tests the default file naming logic for AVRO exports to GCS, including shard information. */
  @Test
  public void testAvroFileNaming() {
    AvroDestination dest = AvroDestination.of("shard1", "table1", "{}");
    MigrateTableTransform.AvroFileNaming naming = new MigrateTableTransform.AvroFileNaming(dest);

    String filename =
        naming.getFilename(
            GlobalWindow.INSTANCE, PaneInfo.NO_FIRING, 1, 0, Compression.UNCOMPRESSED);

    assertThat(filename).startsWith("table1/shard1/");
    assertThat(filename).endsWith(".avro");
  }

  /** Tests the file naming logic when no shard ID is provided. */
  @Test
  public void testAvroFileNaming_NoShardId() {
    AvroDestination dest = AvroDestination.of(null, "table1", "{}");
    MigrateTableTransform.AvroFileNaming naming = new MigrateTableTransform.AvroFileNaming(dest);

    String filename =
        naming.getFilename(
            GlobalWindow.INSTANCE, PaneInfo.NO_FIRING, 1, 0, Compression.UNCOMPRESSED);

    assertThat(filename).startsWith("table1/");
    assertThat(filename).doesNotContain("null");
    assertThat(filename).endsWith(".avro");
  }

  /**
   * Tests the {@link MigrateTableTransform#expand} method to ensure it correctly constructs the
   * pipeline when GCS output and DLQ directories are specified.
   */
  @Test
  public void testExpand_ShouldExerciseBranches() {
    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect("MYSQL");
    options.setGcsOutputDirectory("gs://test/avro");
    options.setOutputDirectory("gs://test/output");
    options.setBatchSizeForSpannerMutations(100L);

    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-db");
    Ddl ddl = mock(Ddl.class);
    ISchemaMapper schemaMapper = mock(ISchemaMapper.class);
    ReaderImpl reader = mock(ReaderImpl.class);
    ReaderTransform readerTransform = mock(ReaderTransform.class);

    when(reader.getReaderTransform()).thenReturn(readerTransform);

    TupleTag<SourceRow> sourceRowTag = new TupleTag<SourceRow>("row") {};
    when(readerTransform.sourceRowTag()).thenReturn(sourceRowTag);

    PTransform<PBegin, PCollectionTuple> readTransform =
        new PTransform<PBegin, PCollectionTuple>() {
          @Override
          public PCollectionTuple expand(PBegin input) {
            PCollection<SourceRow> sourceRows =
                input.apply(
                    Create.empty(org.apache.beam.sdk.values.TypeDescriptor.of(SourceRow.class)));
            return PCollectionTuple.of(sourceRowTag, sourceRows);
          }
        };
    when(readerTransform.readTransform()).thenReturn(readTransform);

    MigrateTableTransform migrateTableTransform =
        new MigrateTableTransform(options, spannerConfig, ddl, schemaMapper, reader);

    // Call expand manually to exercise construction logic.
    // This avoids the need to mock execution-time dependencies like SpannerWriter.
    Pipeline p = Pipeline.create();
    migrateTableTransform.expand(PBegin.in(p));
  }

  /**
   * Tests the writeToGCS method end-to-end to ensure that records are serialized correctly using
   * the wrapper schema in a non-sharded flow, where shardId is null.
   */
  @Test
  public void testWriteToGCSForNonShardedFlow() throws Exception {
    runGcsWriteTest(null, "table1");
  }

  /**
   * Tests the writeToGCS method end-to-end to ensure that records are serialized correctly using
   * the wrapper schema in a sharded flow, where shardId is present.
   */
  @Test
  public void testWriteToGCSForShardedFlow() throws Exception {
    runGcsWriteTest("shard1", "table1/shard1");
  }

  private void runGcsWriteTest(String shardId, String expectedRelativeSubDir) throws Exception {
    TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    final String testTable = "table1";
    final long testReadTime = 1712751118L;
    var schemaRef = SchemaTestUtils.generateSchemaReference("public", "mydb");
    var schema =
        SourceTableSchema.builder(UnifiedTypeMapper.MapperType.MYSQL)
            .setTableName(testTable)
            .addSourceColumnNameToSourceColumnType(
                "id", new SourceColumnType("INTEGER", new Long[] {}, null))
            .addSourceColumnNameToSourceColumnType(
                "firstName", new SourceColumnType("varchar", new Long[] {20L}, null))
            .build();
    SourceRow sourceRow =
        SourceRow.builder(schemaRef, schema, shardId, testReadTime)
            .setField("id", 123)
            .setField("firstName", "abc")
            .build();

    PCollection<SourceRow> sourceRows = pipeline.apply(Create.of(sourceRow));
    String tempDirPath = tempFolder.getRoot().getAbsolutePath();

    SourceDbToSpannerOptions options = PipelineOptionsFactory.as(SourceDbToSpannerOptions.class);
    options.setSourceDbDialect("MYSQL");
    options.setGcsOutputDirectory(tempDirPath);

    SpannerConfig mockSpannerConfig = mock(SpannerConfig.class);
    Ddl mockDdl = mock(Ddl.class);
    ISchemaMapper mockSchemaMapper = mock(ISchemaMapper.class);
    ReaderImpl mockReader = mock(ReaderImpl.class);

    MigrateTableTransform migrateTableTransform =
        new MigrateTableTransform(
            options, mockSpannerConfig, mockDdl, mockSchemaMapper, mockReader);

    migrateTableTransform.writeToGCS(sourceRows, tempDirPath);

    // Run the pipeline to execute the Avro write logic on actual data
    pipeline.run().waitUntilFinish();

    // Verify Avro File was written and matches wrapper schema
    java.io.File avroSubDir = new java.io.File(tempDirPath, expectedRelativeSubDir);
    java.io.File[] avroFiles = avroSubDir.listFiles((dir, name) -> name.endsWith(".avro"));
    assertThat(avroFiles).isNotNull();
    assertThat(avroFiles).hasLength(1);
    java.io.File avroFile = avroFiles[0];

    try (DataFileReader<GenericRecord> fileReader =
        new DataFileReader<>(avroFile, new GenericDatumReader<>())) {
      assertThat(fileReader.hasNext()).isTrue();
      GenericRecord record = fileReader.next();

      assertThat(record.get("tableName").toString()).isEqualTo("table1");
      if (shardId == null) {
        assertThat(record.get("shardId")).isNull();
      } else {
        assertThat(record.get("shardId").toString()).isEqualTo(shardId);
      }

      GenericRecord payloadRecord = (GenericRecord) record.get("payload");
      assertThat(payloadRecord.get("id")).isEqualTo(123);
      assertThat(payloadRecord.get("firstName").toString()).isEqualTo("abc");

      assertThat(fileReader.hasNext()).isFalse();
    }
  }
}
