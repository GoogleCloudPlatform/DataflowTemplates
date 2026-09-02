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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.config.ValidationTableConfig;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

public class SpannerReaderTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReadAndMapRecords() {
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

    // 2. Mock Spanner Data
    Struct struct1 =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("name")
            .to("name1")
            .set("__tableName__")
            .to("SpannerTable")
            .build();

    Struct struct2 =
        Struct.newBuilder()
            .set("id")
            .to(2L)
            .set("name")
            .to("name2")
            .set("__tableName__")
            .to("SpannerTable")
            .build();

    // 3. Create Transform with overridden readFromSpanner
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerReaderTransform transform =
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new, ValidationTableConfig.empty()) {
          @Override
          protected PTransform<PCollection<ReadOperation>, PCollection<Struct>> readFromSpanner() {
            return new PTransform<PCollection<ReadOperation>, PCollection<Struct>>() {
              @Override
              public PCollection<Struct> expand(PCollection<ReadOperation> input) {
                return input.getPipeline().apply("MockRead", Create.of(struct1, struct2));
              }
            };
          }
        };

    // 4. Run Pipeline
    PCollection<ComparisonRecord> output = pipeline.apply(transform);

    // 5. Verify
    PAssert.that(output)
        .satisfies(
            records -> {
              int count = 0;
              for (ComparisonRecord rec : records) {
                count++;
                assertEquals("SpannerTable", rec.getTableName());
                assertNotNull(rec.getPrimaryKeyColumns());
                assertNotNull(rec.getHash());
              }
              assertEquals(2, count);
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testReadWithEmptyDdl() {
    // 1. Setup Empty Ddl
    Ddl ddl = Ddl.builder().build();

    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 2. Create Transform with overridden readFromSpanner
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerReaderTransform transform =
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new, ValidationTableConfig.empty()) {
          @Override
          protected PTransform<@NotNull PCollection<ReadOperation>, @NotNull PCollection<Struct>>
              readFromSpanner() {
            return new PTransform<>() {
              @Override
              public @NotNull PCollection<Struct> expand(
                  @NotNull PCollection<ReadOperation> input) {
                Struct dummy = Struct.newBuilder().set("id").to(1L).build();
                return input
                    .getPipeline()
                    .apply("MockRead", Create.of(dummy))
                    .apply(
                        "FilterDummy",
                        Filter.by((SerializableFunction<Struct, Boolean>) input1 -> false));
              }
            };
          }
        };

    // 3. Run Pipeline
    PCollection<ComparisonRecord> output = pipeline.apply(transform);

    // 4. Verify
    PAssert.that(output).empty();

    pipeline.run();
  }

  @Test
  public void testReadWithNullFields() {
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

    // struct with a value set as NULL
    Struct struct1 =
        Struct.newBuilder()
            .set("id")
            .to(1L)
            .set("name")
            .to((String) null)
            .set("__tableName__")
            .to("SpannerTable")
            .build();

    // 3. Create Transform
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerReaderTransform transform =
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new, ValidationTableConfig.empty()) {
          @Override
          protected PTransform<@NotNull PCollection<ReadOperation>, @NotNull PCollection<Struct>>
              readFromSpanner() {
            return new PTransform<>() {
              @Override
              public @NotNull PCollection<Struct> expand(
                  @NotNull PCollection<ReadOperation> input) {
                return input.getPipeline().apply("MockRead", Create.of(struct1));
              }
            };
          }
        };

    // 4. Run Pipeline
    PCollection<ComparisonRecord> output = pipeline.apply(transform);

    // 5. Verify
    PAssert.that(output)
        .satisfies(
            records -> {
              ComparisonRecord rec = records.iterator().next();
              assertNotNull(rec);
              assertEquals("SpannerTable", rec.getTableName());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testOriginalReadFromSpanner() {
    Ddl ddl = Ddl.builder().build();
    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL_Ref", Create.of(ddl)).apply(View.asSingleton());
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");

    SpannerReaderTransform transform =
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new, ValidationTableConfig.empty());

    assertNotNull(transform.readFromSpanner());
    pipeline.run();
  }

  @Test
  public void testReadWithTableConfigFiltersTables() {
    // 1. Setup Ddl with two tables
    Ddl ddl =
        Ddl.builder()
            .createTable("AllowedTable")
            .column("id").int64().notNull().endColumn()
            .primaryKey().asc("id").end()
            .endTable()
            .createTable("SkippedTable")
            .column("id").int64().notNull().endColumn()
            .primaryKey().asc("id").end()
            .endTable()
            .build();

    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 2. Setup ValidationTableConfig with only one table
    GCSSpannerDV.Options options = PipelineOptionsFactory.as(GCSSpannerDV.Options.class);
    options.setTables("AllowedTable");
    ValidationTableConfig tableConfig = ValidationTableConfig.parseFromOptions(options);

    // 3. Create Transform with overridden readFromSpanner to intercept and assert ReadOperations
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerReaderTransform transform =
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new, tableConfig) {
          @Override
          protected PTransform<@NotNull PCollection<ReadOperation>, @NotNull PCollection<Struct>>
              readFromSpanner() {
            return new PTransform<>() {
              @Override
              public @NotNull PCollection<Struct> expand(
                  @NotNull PCollection<ReadOperation> input) {
                // Assert that the pipeline only generated a ReadOperation for "AllowedTable"
                PAssert.that(input).satisfies(
                    ops -> {
                      int count = 0;
                      for (ReadOperation op : ops) {
                        count++;
                        assertTrue(
                            "Expected ReadOperation for AllowedTable but got: " + op.getQuery().getSql(),
                            op.getQuery().getSql().contains("AllowedTable"));
                      }
                      assertEquals(1, count);
                      return null;
                    });

                // Return an empty PCollection of Structs to safely complete the pipeline
                return input.getPipeline().apply("MockEmptyRead", Create.empty(org.apache.beam.sdk.values.TypeDescriptor.of(Struct.class)));
              }
            };
          }
        };

    // 4. Run Pipeline (PAssert runs during pipeline execution)
    pipeline.apply(transform);
    pipeline.run();
  }

  @Test
  public void testReadWithTableConfigAndSchemaMapperFiltersTables() {
    // 1. Setup Ddl with two tables (using Spanner names)
    Ddl ddl =
        Ddl.builder()
            .createTable("spanner_mapped_table")
            .column("id").int64().notNull().endColumn()
            .primaryKey().asc("id").end()
            .endTable()
            .createTable("skipped_table")
            .column("id").int64().notNull().endColumn()
            .primaryKey().asc("id").end()
            .endTable()
            .build();

    PCollectionView<Ddl> ddlView =
        pipeline.apply("CreateDDL", Create.of(ddl)).apply(View.asSingleton());

    // 2. Setup ValidationTableConfig with the Source name
    GCSSpannerDV.Options options = PipelineOptionsFactory.as(GCSSpannerDV.Options.class);
    options.setTables("source_mapped_table");
    ValidationTableConfig tableConfig = ValidationTableConfig.parseFromOptions(options);

    // 3. Create a Serializable SchemaMapper stub to translate spanner_mapped_table -> source_mapped_table
    com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper stubMapper = 
        new com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper(ddl) {
          @Override
          public String getSourceTableName(String namespace, String spannerTableName) {
            if ("spanner_mapped_table".equals(spannerTableName)) return "source_mapped_table";
            return super.getSourceTableName(namespace, spannerTableName);
          }
    };

    // 4. Create Transform with overridden readFromSpanner
    SpannerConfig spannerConfig = SpannerConfig.create().withProjectId("test-project");
    SpannerReaderTransform transform =
        new SpannerReaderTransform(spannerConfig, ddlView, (d) -> stubMapper, tableConfig) {
          @Override
          protected PTransform<@NotNull PCollection<ReadOperation>, @NotNull PCollection<Struct>>
              readFromSpanner() {
            return new PTransform<>() {
              @Override
              public @NotNull PCollection<Struct> expand(
                  @NotNull PCollection<ReadOperation> input) {
                // Assert that the pipeline correctly translated the spanner name and generated one ReadOperation
                PAssert.that(input).satisfies(
                    ops -> {
                      int count = 0;
                      for (ReadOperation op : ops) {
                        count++;
                        assertTrue(
                            "Expected ReadOperation for spanner_mapped_table but got: " + op.getQuery().getSql(),
                            op.getQuery().getSql().contains("spanner_mapped_table"));
                      }
                      assertEquals(1, count);
                      return null;
                    });

                // Return an empty PCollection of Structs
                return input.getPipeline().apply("MockEmptyRead2", Create.empty(org.apache.beam.sdk.values.TypeDescriptor.of(Struct.class)));
              }
            };
          }
        };

    // 5. Run Pipeline
    pipeline.apply(transform);
    pipeline.run();
  }
}
