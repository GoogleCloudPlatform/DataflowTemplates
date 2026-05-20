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

import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
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
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new) {
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
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new) {
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
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new) {
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
        new SpannerReaderTransform(spannerConfig, ddlView, IdentityMapper::new);

    assertNotNull(transform.readFromSpanner());
    pipeline.run();
  }
}
