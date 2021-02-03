/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.templates;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.io.GcsIO;
import com.google.cloud.teleport.v2.transforms.io.GcsIO.FORMAT;
import com.google.cloud.teleport.v2.utils.RowToCsv;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test class for {@link ProtegrityDataTokenization}.
 */
public class ProtegrityDataTokenizationTest {

  final static String testSchema = "{\"fields\":[{\"mode\":\"REQUIRED\",\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"mode\":\"REQUIRED\",\"name\":\"FieldName2\",\"type\":\"STRING\"}]}";
  final static String avroTestSchema =
      "{ \"namespace\": \"com.google.cloud.teleport.v2.templates\", \n"
          + "\t\"type\": \"record\", \n"
          + "\t\"name\": \"typeName\", \n"
          + "\t\"fields\": [ \n"
          + "\t  {\"name\": \"FieldName1\",\"type\": \"string\"}, \n"
          + "\t  {\"name\": \"FieldName2\", \"type\": \"string\"}\n"
          + "\t] \n"
          + "}";
  String[] testFields = {"TestValue1", "TestValue2"};

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "./";

  private static final String CSV_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.csv").getPath();

  private static final String JSON_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.txt").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "schema.txt").getPath();

  private static final String AVRO_OUTPUT_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testOutputAvro.avro").getPath();

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  private static class LogIt extends DoFn<GenericRecord, GenericRecord> {

    @ProcessElement
    public void processElement(@Element GenericRecord in, OutputReceiver<GenericRecord> out) {
      System.out.println(in.toString());
      out.output(in);
    }
  }

  @Test
  public void testGetBeamSchema() {
    System.out.println(CSV_FILE_PATH);
    Schema expectedSchema =
        Schema.builder()
            .addField("FieldName1", FieldType.STRING)
            .addField("FieldName2", FieldType.STRING)
            .build();
    SchemasUtils schemasUtils = new SchemasUtils(testSchema);
    Assert.assertEquals(expectedSchema, schemasUtils.getBeamSchema());
  }

  @Test
  public void testGetBigQuerySchema() {
    SchemasUtils schemasUtils = new SchemasUtils(testSchema);
    Assert.assertEquals(testSchema, schemasUtils.getBigQuerySchema().toString());
  }

  @Test
  public void testRowToCSV() {
    Schema beamSchema = new SchemasUtils(testSchema).getBeamSchema();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValues(new ArrayList<>(Arrays.asList(testFields))).build();
    String csvResult = new RowToCsv(";").getCsvFromRow(row);
    Assert.assertEquals(String.join(";", testFields), csvResult);
  }

  @Test
  public void testFileSystemIOReadCSV() throws IOException {
    PCollection<String> jsons = fileSystemIORead(CSV_FILE_PATH, FORMAT.CSV);
    assertField(jsons);
    testPipeline.run();
  }

  @Test
  public void testFileSystemIOReadJSON() throws IOException {
    PCollection<String> jsons = fileSystemIORead(JSON_FILE_PATH, FORMAT.JSON);
    assertField(jsons);
    testPipeline.run();
  }

  @Test
  public void testJsonToRow() throws IOException {
    PCollection<String> jsons = fileSystemIORead(JSON_FILE_PATH, FORMAT.JSON);
    SchemasUtils testSchemaUtils = new SchemasUtils(SCHEMA_FILE_PATH, StandardCharsets.UTF_8);
    JsonToRow.ParseResult rows =
        jsons.apply(
            "JsonToRow",
            JsonToRow.withExceptionReporting(testSchemaUtils.getBeamSchema())
                .withExtendedErrorInfo());
    PAssert.that(rows.getResults())
        .satisfies(
            x -> {
              LinkedList<Row> beamRows = Lists.newLinkedList(x);
              assertThat(beamRows, hasSize(3));
              beamRows.forEach(
                  row -> {
                    List<Object> fieldValues = row.getValues();
                    for (Object element : fieldValues) {
                      assertThat((String) element, startsWith("FieldValue"));
                    }
                  });
              return null;
            });
    testPipeline.run();
  }

  @Test
  public void testRowToAvro() {
    Schema beamSchema = new SchemasUtils(testSchema).getBeamSchema();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValues(new ArrayList<>(Arrays.asList(testFields))).build();
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);
    GenericRecord genericRecord = AvroUtils.toGenericRecord(row, avroSchema);

    testPipeline
        .apply("Create",
            Create.of(genericRecord).withCoder(AvroCoder.of(GenericRecord.class, avroSchema)))
        .apply("printIt", ParDo.of(new LogIt()))
        .apply(AvroIO.writeGenericRecords(avroSchema)
            .to("/home/daria/out.avro"));

    testPipeline.run();
  }

  private PCollection<String> fileSystemIORead(
      String inputGcsFilePattern, FORMAT inputGcsFileFormat) throws IOException {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.create().as(ProtegrityDataTokenizationOptions.class);
    options.setDataSchemaGcsPath(SCHEMA_FILE_PATH);
    options.setInputGcsFilePattern(inputGcsFilePattern);
    options.setInputGcsFileFormat(inputGcsFileFormat);
    if (inputGcsFileFormat == FORMAT.CSV) {
      options.setCsvContainsHeaders(Boolean.FALSE);
    }

    SchemasUtils testSchemaUtils =
        new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);

    CoderRegistry coderRegistry = testPipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        RowCoder.of(testSchemaUtils.getBeamSchema()).getEncodedTypeDescriptor(),
        RowCoder.of(testSchemaUtils.getBeamSchema()));
    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder =
        FailsafeElementCoder.of(
            RowCoder.of(testSchemaUtils.getBeamSchema()),
            RowCoder.of(testSchemaUtils.getBeamSchema()));
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    return new GcsIO(options).read(testPipeline, testSchemaUtils.getJsonBeamSchema());
  }

  private void assertField(PCollection<String> jsons) {
    PAssert.that(jsons)
        .satisfies(
            x -> {
              LinkedList<String> rows = Lists.newLinkedList(x);
              assertThat(rows, hasSize(3));
              rows.forEach(
                  row -> {
                    assertThat(row, startsWith("{\"Field1\":"));
                  });
              return null;
            });
  }
}
