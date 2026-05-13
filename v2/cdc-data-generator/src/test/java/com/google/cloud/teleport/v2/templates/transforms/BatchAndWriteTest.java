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
package com.google.cloud.teleport.v2.templates.transforms;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.templates.CdcDataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Comprehensive unit tests for {@link BatchAndWrite}. */
@RunWith(JUnit4.class)
public class BatchAndWriteTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBatchAndWriteExpansion_spannerSink() {
    DataGeneratorSchema schema = DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("CreateSchema", Create.of(schema)).apply("AsSingleton", View.asSingleton());

    Schema rowSchema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(rowSchema).addValues(123L).build();
    GeneratedRecord record = GeneratedRecord.create("SampleTable", row);

    PCollection<KV<Integer, GeneratedRecord>> input =
        pipeline.apply("CreateInput", Create.of(KV.of(0, record)));

    BatchAndWrite transform =
        new BatchAndWrite(SinkType.SPANNER, "{}", 100, 10, 5000, 10000, schemaView);

    PCollection<String> output = input.apply("BatchAndWriteTransformSpanner", transform);
    assertEquals(StringUtf8Coder.of(), output.getCoder());

    // Run the pipeline to verify valid expansion and setup
    pipeline.run();
  }
}
