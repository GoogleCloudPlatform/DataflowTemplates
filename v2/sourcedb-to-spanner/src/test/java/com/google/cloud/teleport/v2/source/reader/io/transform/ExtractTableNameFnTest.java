/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link ExtractTableNameFn}. */
@RunWith(JUnit4.class)
public class ExtractTableNameFnTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testExtractTableNameFn_simpleName() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("db").build());
    SourceTableSchema tableSchema = SchemaTestUtils.generateTestTableSchema("table1");
    SourceRow row =
        SourceRow.builder(schemaRef, tableSchema, null, 1L)
            .setField("firstName", "abc")
            .setField("lastName", "def")
            .build();

    PCollection<String> output =
        pipeline.apply(Create.of(row)).apply(ParDo.of(new ExtractTableNameFn()));

    PAssert.that(output).containsInAnyOrder("\"table1\"");
    pipeline.run();
  }

  @Test
  public void testExtractTableNameFn_withQuotes() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("db").build());
    SourceTableSchema tableSchema = SchemaTestUtils.generateTestTableSchema("table\"with\"quotes");
    SourceRow row =
        SourceRow.builder(schemaRef, tableSchema, null, 1L)
            .setField("firstName", "abc")
            .setField("lastName", "def")
            .build();

    PCollection<String> output =
        pipeline.apply(Create.of(row)).apply(ParDo.of(new ExtractTableNameFn()));

    // Delimit logic: table"with"quotes -> "table""with""quotes"
    PAssert.that(output).containsInAnyOrder("\"table\"\"with\"\"quotes\"");
    pipeline.run();
  }
}
