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
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link GroupCompletionDoFn}. */
@RunWith(JUnit4.class)
public class GroupCompletionDoFnTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGroupCompletionDoFn_matchingTable() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("db").build());
    SourceTableReference ref1 =
        SourceTableReference.builder()
            .setSourceSchemaReference(schemaRef)
            .setSourceTableName("table1")
            .setSourceTableSchemaUUID("u1")
            .build();
    SourceTableReference ref2 =
        SourceTableReference.builder()
            .setSourceSchemaReference(schemaRef)
            .setSourceTableName("table2")
            .setSourceTableSchemaUUID("u2")
            .build();

    ImmutableList<SourceTableReference> tableReferences = ImmutableList.of(ref1, ref2);
    GroupCompletionDoFn fn = new GroupCompletionDoFn(tableReferences);

    PCollection<SourceTableReference> output =
        pipeline.apply(Create.of(KV.of("table1", 100L))).apply(ParDo.of(fn));

    PAssert.that(output).containsInAnyOrder(ref1.toBuilder().setRecordCount(100L).build());
    pipeline.run();
  }

  @Test
  public void testGroupCompletionDoFn_missingTable() {
    SourceSchemaReference schemaRef =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("db").build());
    SourceTableReference ref1 =
        SourceTableReference.builder()
            .setSourceSchemaReference(schemaRef)
            .setSourceTableName("table1")
            .setSourceTableSchemaUUID("u1")
            .build();

    ImmutableList<SourceTableReference> tableReferences = ImmutableList.of(ref1);
    GroupCompletionDoFn fn = new GroupCompletionDoFn(tableReferences);

    PCollection<SourceTableReference> output =
        pipeline.apply(Create.of(KV.of("unknownTable", 100L))).apply(ParDo.of(fn));

    PAssert.that(output).empty();
    pipeline.run();
  }
}
