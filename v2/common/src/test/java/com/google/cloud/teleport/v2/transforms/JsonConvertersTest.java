/*
 * Copyright (C) 2021 Google LLC
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

import com.google.cloud.teleport.v2.transforms.JsonConverters.StringToGenericRecordFn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Test cases for the {@link JsonConverters} class. Inspired by {@link CsvConvertersTest}. */
public class JsonConvertersTest {

  private static final String JSON_RESOURCES_DIR = "JsonConvertersTest/";

  private static final String JSON_RECORDS_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "json_records.json").getPath();

  private static final String ONE_FIELD_RECORD_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "one_field_record.json").getPath();

  private static final String ONE_NULL_FIELD_EXPLICIT_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "one_null_field_explicit.json").getPath();

  private static final String ONE_NULL_FIELD_IMPLICIT_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "one_null_field_implicit.json").getPath();

  private static final String ONE_NOT_NULL_FIELD_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "one_not_null_field.json").getPath();

  private static final String NUMERIC_FIELDS_RECORD_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "numeric_fields_record.json").getPath();

  private static final String NESTED_FIELDS_RECORD_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "nested_fields_record.json").getPath();

  private static final String ARRAY_OF_RECORDS_RECORD_FILE_PATH =
      Resources.getResource(JSON_RESOURCES_DIR + "array_of_records.json").getPath();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Tests {@link JsonConverters.ReadJson} reads a Json correctly. */
  @Test
  public void testReadJson() throws IOException {
    ImmutableSet<String> expected =
        ImmutableSet.copyOf(Files.readAllLines(new File(JSON_RECORDS_FILE_PATH).toPath()));

    // Build pipeline
    PCollection<String> readJsonOut =
        pipeline.apply(
            "TestReadJson",
            JsonConverters.ReadJson.newBuilder().setInputFileSpec(JSON_RECORDS_FILE_PATH).build());

    PAssert.that(readJsonOut)
        .satisfies(
            collection -> {
              assertEquals(expected, ImmutableSet.copyOf(collection));
              return null;
            });

    //  Execute pipeline
    pipeline.run();
  }

  /**
   * Tests if {@link JsonConverters.StringToGenericRecordFn} creates a proper GenericRecord of a
   * simple 1 field JSON records.
   */
  @Test
  public void testOneFieldJsonRecordToGenericRecord() {
    String serializedSchema =
        "{"
            + "\"name\": \"Schema\","
            + "\"type\": \"record\","
            + "\"fields\": ["
            + "   {\"name\": \"x\", \"type\": \"boolean\"}"
            + "]"
            + "}";
    Schema schema = new Parser().parse(serializedSchema);

    GenericRecord expected = new Record(schema);
    expected.put("x", true);

    PCollection<GenericRecord> pCollection =
        pipeline
            .apply(
                "TestReadJson",
                JsonConverters.ReadJson.newBuilder()
                    .setInputFileSpec(ONE_FIELD_RECORD_FILE_PATH)
                    .build())
            .apply(
                "ConvertStringToGenericRecord",
                ParDo.of(new StringToGenericRecordFn(serializedSchema)))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

    PAssert.that(pCollection).containsInAnyOrder(expected);

    pipeline.run();
  }

  /**
   * Tests if {@link JsonConverters.StringToGenericRecordFn} creates a proper GenericRecord of a
   * simple numeric fields JSON records.
   */
  @Test
  public void testNumericFieldsJsonRecordToGenericRecord() {
    String serializedSchema =
        "{"
            + "\"name\": \"Schema\","
            + "\"type\": \"record\","
            + "\"fields\": ["
            + "   {\"name\": \"i\", \"type\": \"int\"},"
            + "   {\"name\": \"l\", \"type\": \"long\"},"
            + "   {\"name\": \"f\", \"type\": \"float\"},"
            + "   {\"name\": \"d\", \"type\": \"double\"}"
            + "]"
            + "}";
    Schema schema = new Parser().parse(serializedSchema);

    GenericRecord expected = new Record(schema);
    expected.put("i", 42);
    expected.put("l", 42L);
    expected.put("f", 42F);
    expected.put("d", 42D);

    PCollection<GenericRecord> pCollection =
        pipeline
            .apply(
                "TestReadJson",
                JsonConverters.ReadJson.newBuilder()
                    .setInputFileSpec(NUMERIC_FIELDS_RECORD_FILE_PATH)
                    .build())
            .apply(
                "ConvertStringToGenericRecord",
                ParDo.of(new StringToGenericRecordFn(serializedSchema)))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

    PAssert.that(pCollection).containsInAnyOrder(expected);

    pipeline.run();
  }

  /**
   * Tests if {@link JsonConverters.StringToGenericRecordFn} creates a proper GenericRecord of a
   * JSON record with a nested record.
   */
  @Test
  public void testNestedFieldsJsonRecordToGenericRecord() {
    String serializedSchema =
        "{"
            + "\"name\": \"Schema\","
            + "\"type\": \"record\","
            + "\"fields\": ["
            + "   {\"name\": \"x\", \"type\": \"int\"},"
            + "   {\"name\": \"yz\", \"type\": {"
            + "       \"type\": \"record\", \"name\": \"yz.Record\", \"fields\": ["
            + "           {\"name\": \"y\", \"type\": \"int\"},"
            + "           {\"name\": \"z\", \"type\": \"int\"}"
            + "       ]"
            + "   }}"
            + "]"
            + "}";
    Schema schema = new Parser().parse(serializedSchema);

    GenericRecord expected = new Record(schema);
    expected.put("x", 3);
    GenericRecord yz = new Record(schema.getField("yz").schema());
    yz.put("y", 5);
    yz.put("z", 7);
    expected.put("yz", yz);

    PCollection<GenericRecord> pCollection =
        pipeline
            .apply(
                "TestReadJson",
                JsonConverters.ReadJson.newBuilder()
                    .setInputFileSpec(NESTED_FIELDS_RECORD_FILE_PATH)
                    .build())
            .apply(
                "ConvertStringToGenericRecord",
                ParDo.of(new StringToGenericRecordFn(serializedSchema)))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

    PAssert.that(pCollection).containsInAnyOrder(expected);

    pipeline.run();
  }

  /**
   * Tests if {@link JsonConverters.StringToGenericRecordFn} creates a proper GenericRecord of a
   * JSON record with an array of records.
   */
  @Test
  public void testArrayOfRecordsJsonRecordToGenericRecord() {
    String serializedSchema =
        "{"
            + "\"name\": \"Schema\","
            + "\"type\": \"record\","
            + "\"fields\": ["
            + "   {\"name\": \"x\", \"type\": \"int\"},"
            + "   {\"name\": \"yz\", \"type\": {\"type\": \"array\", \"items\": {"
            + "       \"type\": \"record\", \"name\": \"yz.Record\", \"fields\": ["
            + "           {\"name\": \"y\", \"type\": \"int\"},"
            + "           {\"name\": \"z\", \"type\": \"int\"}"
            + "       ]"
            + "   }}}"
            + "]"
            + "}";
    Schema schema = new Parser().parse(serializedSchema);

    GenericRecord expected = new Record(schema);
    expected.put("x", 3);
    GenericRecord yz1 = new Record(schema.getField("yz").schema().getElementType());
    yz1.put("y", 5);
    yz1.put("z", 7);
    GenericRecord yz2 = new Record(schema.getField("yz").schema().getElementType());
    yz2.put("y", 55);
    yz2.put("z", 77);
    expected.put(
        "yz", new GenericData.Array<>(schema.getField("yz").schema(), ImmutableList.of(yz1, yz2)));

    PCollection<GenericRecord> pCollection =
        pipeline
            .apply(
                "TestReadJson",
                JsonConverters.ReadJson.newBuilder()
                    .setInputFileSpec(ARRAY_OF_RECORDS_RECORD_FILE_PATH)
                    .build())
            .apply(
                "ConvertStringToGenericRecord",
                ParDo.of(new StringToGenericRecordFn(serializedSchema)))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

    PAssert.that(pCollection).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testOneNullableFieldExplicitJsonExplicitAvro() {
    testOneNullableFieldJsonRecordToGenericRecord(
        ONE_NULL_FIELD_EXPLICIT_FILE_PATH, r -> r.put("x", null));
  }

  @Test
  public void testOneNullableFieldExplicitJsonImplicitAvro() {
    testOneNullableFieldJsonRecordToGenericRecord(ONE_NULL_FIELD_EXPLICIT_FILE_PATH, r -> {});
  }

  @Test
  public void testOneNullableFieldImplicitJsonExplicitAvro() {
    testOneNullableFieldJsonRecordToGenericRecord(
        ONE_NULL_FIELD_IMPLICIT_FILE_PATH, r -> r.put("x", null));
  }

  @Test
  public void testOneNullableFieldImplicitJsonImplicitAvro() {
    testOneNullableFieldJsonRecordToGenericRecord(ONE_NULL_FIELD_IMPLICIT_FILE_PATH, r -> {});
  }

  @Test
  public void testOneNullableFieldNonNullValue() {
    testOneNullableFieldJsonRecordToGenericRecord(
        ONE_NOT_NULL_FIELD_FILE_PATH, r -> r.put("x", 42L));
  }

  private void testOneNullableFieldJsonRecordToGenericRecord(
      String inputFile, Consumer<GenericRecord> genericRecordSetter) {
    String serializedSchema =
        "{"
            + "\"name\": \"Schema\","
            + "\"type\": \"record\","
            + "\"fields\": ["
            + "   {\"name\": \"x\", \"type\": [\"null\", \"long\"]}"
            + "]"
            + "}";
    Schema schema = new Parser().parse(serializedSchema);
    GenericRecord expected = new Record(schema);
    genericRecordSetter.accept(expected);

    PCollection<GenericRecord> pCollection =
        pipeline
            .apply(
                "TestReadJson",
                JsonConverters.ReadJson.newBuilder().setInputFileSpec(inputFile).build())
            .apply(
                "ConvertStringToGenericRecord",
                ParDo.of(new StringToGenericRecordFn(serializedSchema)))
            .setCoder(AvroCoder.of(GenericRecord.class, schema));

    PAssert.that(pCollection).containsInAnyOrder(expected);

    pipeline.run();
  }
}
