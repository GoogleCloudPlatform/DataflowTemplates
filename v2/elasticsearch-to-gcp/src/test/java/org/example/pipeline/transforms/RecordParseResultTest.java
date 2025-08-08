/*
 * Copyright (C) 2024 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.example.pipeline.transforms;

import com.google.common.collect.Streams;
import java.math.BigDecimal;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.example.pipeline.Types;
import org.example.pipeline.json.JsonSchema;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** */
@RunWith(JUnit4.class)
public class RecordParseResultTest {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  private static final List<String> JSON_DATA =
      List.of(
          "{\"propstr\":\"value\",\"prop.num\":5.0,\"proparr\":[\"some other value\"]}",
          "{\"propstr\":\"value2\",\"prop.num\":4.0,\"proparr\":[[\"some other value 2\"]]}",
          "{\"propstr\":\"value3\",\"prop.num\":null,\"proparr\":[null]}");

  @Test
  public void testParsingDefault() {
    var expectedAvroSchemaStr =
        SchemaBuilder.record(JsonSchema.TOP_LEVEL_NAME)
            .namespace(JsonSchema.TOP_LEVEL_NAMESPACE)
            .doc(JsonSchema.DEFAULT_RECORD_DOC_PREFIX + JsonSchema.TOP_LEVEL_NAME)
            .fields()
            .name("proparr")
            .type()
            .array()
            .items()
            .array()
            .items()
            .stringType()
            .noDefault()
            .name("propstr")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .endUnion()
            .noDefault()
            .name("prop__num")
            .type()
            .unionOf()
            .nullType()
            .and()
            .type(LogicalTypes.decimal(77, 38).addToSchema(Schema.create(Schema.Type.BYTES)))
            .endUnion()
            .noDefault()
            .endRecord()
            .toString();

    var output = testPipeline.apply(Create.of(JSON_DATA)).apply(ParseRecordFromJSON.create());

    PAssert.that(output.getNotParsed()).empty();
    PAssert.that(output.getDetectedAvroSchema())
        .satisfies(
            detectedSchemaIterator -> {
              var detectedSchemaStr = detectedSchemaIterator.iterator().next();
              Assert.assertEquals(expectedAvroSchemaStr, detectedSchemaStr);
              return null;
            });
    PAssert.that(output.getRecords())
        .satisfies(
            records -> {
              var processedData =
                  Streams.stream(records)
                      .map(record -> Types.toGenericRecord(expectedAvroSchemaStr, record))
                      .toList();
              var expectedData =
                  List.of(
                      new GenericRecordBuilder(new Schema.Parser().parse(expectedAvroSchemaStr))
                          .set("propstr", "value")
                          .set("prop__num", new BigDecimal(5.0))
                          .set("proparr", List.of(List.of("some other value")))
                          .build(),
                      new GenericRecordBuilder(new Schema.Parser().parse(expectedAvroSchemaStr))
                          .set("propstr", "value2")
                          .set("prop__num", new BigDecimal(4.0))
                          .set("proparr", List.of(List.of("some other value 2")))
                          .build(),
                      new GenericRecordBuilder(new Schema.Parser().parse(expectedAvroSchemaStr))
                          .set("propstr", "value3")
                          .set("prop__num", null)
                          .set("proparr", List.of())
                          .build());
              MatcherAssert.assertThat(
                  processedData, Matchers.containsInAnyOrder(expectedData.toArray()));

              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testParsingFlatten() {
    var testPipelineFlatten =
        TestPipeline.create(
            PipelineOptionsFactory.fromArgs(
                    "--multiArraySchemaResolution=FLATTEN", "--targetParallelism=1")
                .as(ParseRecordFromJSON.ParseJSONsOptions.class));
    var expectedAvroSchemaStr =
        SchemaBuilder.record(JsonSchema.TOP_LEVEL_NAME)
            .namespace(JsonSchema.TOP_LEVEL_NAMESPACE)
            .doc(JsonSchema.DEFAULT_RECORD_DOC_PREFIX + JsonSchema.TOP_LEVEL_NAME)
            .fields()
            .name("proparr")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .name("propstr")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .endUnion()
            .noDefault()
            .name("prop__num")
            .type()
            .unionOf()
            .nullType()
            .and()
            .type(LogicalTypes.decimal(77, 38).addToSchema(Schema.create(Schema.Type.BYTES)))
            .endUnion()
            .noDefault()
            .endRecord()
            .toString();

    var output =
        testPipelineFlatten.apply(Create.of(JSON_DATA)).apply(ParseRecordFromJSON.create());

    PAssert.that(output.getNotParsed()).empty();
    PAssert.that(output.getDetectedAvroSchema())
        .satisfies(
            detectedSchemaIterator -> {
              var detectedSchemaStr = detectedSchemaIterator.iterator().next();
              Assert.assertEquals(expectedAvroSchemaStr, detectedSchemaStr);
              return null;
            });

    testPipelineFlatten.run();
  }
}