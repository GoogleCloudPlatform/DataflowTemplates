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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.config.TableConfiguration;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for shard-aware GCS file pattern generation in {@link SourceReaderTransform}. */
public class SourceReaderTransformShardFilterTest implements Serializable {

  private static final String ROOT = "gs://b/d";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final transient TemporaryFolder tempFolder = new TemporaryFolder();

  private static TableConfiguration config(String tables, String shardIds) {
    GCSSpannerDVOptions options = PipelineOptionsFactory.create().as(GCSSpannerDVOptions.class);
    options.setGcsInputDirectory(null);
    if (tables != null) {
      options.setTables(tables);
    }
    if (shardIds != null) {
      options.setShardIds(shardIds);
    }
    return TableConfiguration.parseFromOptions(options);
  }

  /** Asserts the exact pattern set, ignoring order but not duplicates. */
  private static void assertPatterns(List<String> actual, String... expected) {
    assertEquals("pattern count, got " + actual, expected.length, actual.size());
    assertEquals(new HashSet<>(Arrays.asList(expected)), new HashSet<>(actual));
  }

  // G1 (H7 guard)
  @Test
  public void testNoTablesNoShards() {
    List<String> patterns = SourceReaderTransform.getFilePatterns(ROOT, config(null, null));

    assertEquals(Arrays.asList("gs://b/d/**.avro"), patterns);
  }

  // G2 (H7 guard)
  @Test
  public void testTablesNoShards() {
    List<String> patterns = SourceReaderTransform.getFilePatterns(ROOT, config("T1,T2", null));

    assertPatterns(patterns, "gs://b/d/T1/**.avro", "gs://b/d/T2/**.avro");
  }

  // G3
  @Test
  public void testShardsOnly() {
    List<String> patterns = SourceReaderTransform.getFilePatterns(ROOT, config(null, "s1,s2"));

    assertPatterns(patterns, "gs://b/d/*/s1/**.avro", "gs://b/d/*/s2/**.avro");
  }

  // G4
  @Test
  public void testTablesAndShards() {
    List<String> patterns = SourceReaderTransform.getFilePatterns(ROOT, config("T1,T2", "s1,s2"));

    assertPatterns(
        patterns,
        "gs://b/d/T1/s1/**.avro",
        "gs://b/d/T1/s2/**.avro",
        "gs://b/d/T2/s1/**.avro",
        "gs://b/d/T2/s2/**.avro");
  }

  // G5: the trailing '/' on the root is stripped, as today's code does for G1/G2.
  @Test
  public void testShardsWithTrailingSlashRoot() {
    List<String> shardsOnly =
        SourceReaderTransform.getFilePatterns(ROOT + "/", config(null, "s1,s2"));
    List<String> tablesAndShards =
        SourceReaderTransform.getFilePatterns(ROOT + "/", config("T1", "s1,s2"));

    assertPatterns(shardsOnly, "gs://b/d/*/s1/**.avro", "gs://b/d/*/s2/**.avro");
    assertPatterns(tablesAndShards, "gs://b/d/T1/s1/**.avro", "gs://b/d/T1/s2/**.avro");
    for (String pattern : concat(shardsOnly, tablesAndShards)) {
      assertFalse("double slash in " + pattern, pattern.substring("gs://".length()).contains("//"));
    }
  }

  // G6
  @Test
  public void testShardPatternCannotMatchLongerShardId() {
    List<String> patterns = SourceReaderTransform.getFilePatterns(ROOT, config(null, "shard_1"));

    assertEquals(1, patterns.size());
    assertTrue(patterns.get(0).contains("/shard_1/"));
    assertEquals("gs://b/d/*/shard_1/**.avro", patterns.get(0));
  }

  // G3 (behavioural): only the selected shard's directories are read, across tables.
  @Test
  public void testReadWithShardFilterReadsOnlySelectedShardDirectories() throws IOException {
    Ddl ddl =
        Ddl.builder()
            .createTable("T")
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
            .createTable("U")
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

    createAvroFile(new File(tempFolder.newFolder("T", "s1"), "a.avro"), "T", "s1", "1");
    createAvroFile(new File(tempFolder.newFolder("T", "s2"), "b.avro"), "T", "s2", "2");
    createAvroFile(new File(tempFolder.newFolder("U", "s1"), "c.avro"), "U", "s1", "3");

    SourceReaderTransform transform =
        new SourceReaderTransform(
            tempFolder.getRoot().getAbsolutePath(),
            ddlView,
            IdentityMapper::new,
            null,
            config(null, "s1"));

    PCollection<ComparisonRecord> output = pipeline.apply(transform);

    PAssert.that(output)
        .satisfies(
            records -> {
              int count = 0;
              Set<String> tables = new HashSet<>();
              for (ComparisonRecord rec : records) {
                count++;
                tables.add(rec.getTableName());
                if (!"s1".equals(rec.getShardId())) {
                  throw new AssertionError("Expected shard s1, got " + rec.getShardId());
                }
              }
              if (count != 2) {
                throw new AssertionError("Expected exactly 2 records, got " + count);
              }
              if (!tables.equals(new HashSet<>(Arrays.asList("T", "U")))) {
                throw new AssertionError("Expected tables T and U, got " + tables);
              }
              return null;
            });

    pipeline.run();
  }

  private static List<String> concat(List<String> a, List<String> b) {
    List<String> all = new ArrayList<>(a);
    all.addAll(b);
    return all;
  }

  private void createAvroFile(File file, String tableName, String shardId, String id)
      throws IOException {
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

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(schema, file);

      GenericRecord payload = new GenericData.Record(payloadSchema);
      payload.put("id", id);
      payload.put("name", "Test Name " + id);

      GenericRecord record = new GenericData.Record(schema);
      record.put("tableName", tableName);
      record.put("shardId", shardId);
      record.put("payload", payload);

      dataFileWriter.append(record);
    }
  }
}
