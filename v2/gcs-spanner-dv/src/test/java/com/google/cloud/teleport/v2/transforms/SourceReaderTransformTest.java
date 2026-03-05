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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SourceReaderTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final transient TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testReadAndMapAvroRecords() throws IOException {
    // 1. Create Dummy Avro File
    File subDir = tempFolder.newFolder("data");
    createAvroFile(new File(subDir, "data.avro"), "SpannerTable", "123");

    // 2. Prepare Dependencies
    // Construct a real Ddl object
    Ddl ddl =
        Ddl.builder()
            .createTable("SpannerTable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("name")
            .string()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    // Create Ddl View
    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 3. Run Pipeline
    String inputPath = tempFolder.getRoot().getAbsolutePath();
    // FileIO in beam support a variety of paths dynamically, such as GCS, S3 and TempFolder
    // This allows us to pass a tempFolder into the same transform that accepts a GCS path
    SourceReaderTransform transform =
        new SourceReaderTransform(inputPath, ddlView, IdentityMapper::new);

    PCollection<ComparisonRecord> output = pipeline.apply(transform);

    // 4. Verify
    PAssert.that(output)
        .satisfies(
            records -> {
              ComparisonRecord rec = records.iterator().next();
              if (!rec.getTableName().equals("SpannerTable")) {
                throw new AssertionError("Expected SpannerTable, got " + rec.getTableName());
              }
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithNoMatchingFiles() {
    // 1. Setup Ddl (same as above)
    // Create Ddl
    Ddl ddl =
        Ddl.builder()
            .createTable("SpannerTable")
            .column("id")
            .int64()
            .endColumn()
            .column("name")
            .string()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 2. Run Pipeline with input path that has no avro files
    String inputPath = tempFolder.getRoot().getAbsolutePath();
    SourceReaderTransform transform =
        new SourceReaderTransform(inputPath, ddlView, IdentityMapper::new);

    pipeline.apply(transform);

    // AvroIO throws a RuntimeException when no files are found matching the pattern
    // if withHintMatchesManyFiles is used (which uses match() internally).
    RuntimeException e = assertThrows(RuntimeException.class, () -> pipeline.run());
    assertTrue(e.getMessage().contains("No files matched spec"));
  }

  @Test
  public void testInvalidTable() throws IOException {
    // 1. Setup Ddl
    Ddl ddl =
        Ddl.builder()
            .createTable("SpannerTable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("name")
            .string()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 2. Create Avro file with invalid table name
    File subDir = tempFolder.newFolder("invalid_data");
    createAvroFile(new File(subDir, "invalid.avro"), "InvalidTable", "123");

    // 3. Run Pipeline
    String inputPath = tempFolder.getRoot().getAbsolutePath();
    SourceReaderTransform transform =
        new SourceReaderTransform(inputPath, ddlView, IdentityMapper::new);

    pipeline.apply(transform);

    // Expect RuntimeException from SourceHashFn -> ComparisonRecordMapper
    RuntimeException e = assertThrows(RuntimeException.class, () -> pipeline.run());
    assertTrue(
        e.getMessage()
            .contains(
                "Error mapping GenericRecord to ComparisonRecord: Spanner table not found for source: 'InvalidTable'"));
  }

  @Test
  public void testReadRecursively() throws IOException {
    // 1. Setup Ddl
    Ddl ddl =
        Ddl.builder()
            .createTable("SpannerTable")
            .column("id")
            .int64()
            .notNull()
            .endColumn()
            .column("name")
            .string()
            .endColumn()
            .primaryKey()
            .asc("id")
            .end()
            .endTable()
            .build();

    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 2. Create Avro files in root and subdirectory
    createAvroFile(new File(tempFolder.getRoot(), "root.avro"), "SpannerTable", "1");
    File subDir = tempFolder.newFolder("subdir");
    createAvroFile(new File(subDir, "sub.avro"), "SpannerTable", "2");

    // 3. Run Pipeline
    String inputPath = tempFolder.getRoot().getAbsolutePath();
    SourceReaderTransform transform =
        new SourceReaderTransform(inputPath, ddlView, IdentityMapper::new);

    PCollection<ComparisonRecord> output = pipeline.apply(transform);

    // 4. Verify
    PAssert.that(output)
        .satisfies(
            records -> {
              int count = 0;
              for (ComparisonRecord rec : records) {
                count++;
                if (!rec.getTableName().equals("SpannerTable")) {
                  throw new AssertionError("Expected SpannerTable, got " + rec.getTableName());
                }
              }
              if (count != 2) {
                throw new AssertionError("Expected 2 records, got " + count);
              }
              return null;
            });

    pipeline.run();
  }

  private void createAvroFile(File file, String tableName, String id) throws IOException {
    Schema payloadSchema =
        SchemaBuilder.record("Payload")
            .fields()
            .requiredString("id")
            .requiredString("name")
            .endRecord();

    Schema schema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .requiredString("tableName")
            .requiredString("shardId")
            .name("payload")
            .type(payloadSchema)
            .noDefault()
            .endRecord();

    DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
      dataFileWriter.create(schema, file);

      GenericRecord payload = new GenericData.Record(payloadSchema);
      payload.put("id", id);
      payload.put("name", "Test Name " + id);

      GenericRecord record = new GenericData.Record(schema);
      record.put("tableName", tableName);
      record.put("shardId", "shard" + id);
      record.put("payload", payload);

      dataFileWriter.append(record);
    }
  }
}
