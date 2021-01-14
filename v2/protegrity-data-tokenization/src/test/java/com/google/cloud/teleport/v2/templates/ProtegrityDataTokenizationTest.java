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

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.transforms.io.GcsIO;
import com.google.cloud.teleport.v2.transforms.io.GcsIO.FORMAT;
import com.google.cloud.teleport.v2.utils.RowToCsv;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test class for {@link ProtegrityDataTokenization}.
 */
public class ProtegrityDataTokenizationTest {

  final static String testSchema = "{\"fields\":[{\"mode\":\"REQUIRED\",\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"mode\":\"REQUIRED\",\"name\":\"FieldName2\",\"type\":\"STRING\"}]}";
  String[] fields = {"TestValue1", "TestValue2"};

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "./";

  private static final String SCV_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.csv").getPath();

  private static final String JSON_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "schema").getPath();

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  private static class LogIt extends DoFn<String, String> {

    @ProcessElement
    public void process(ProcessContext context) {
      System.out.println(context.element());
      context.output(context.element());
    }
  }

  @Test
  public void testGetBeamSchema() {
    Schema expectedSchema = Schema.builder()
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
  public void testNullRowToCSV() {
    Schema beamSchema = Schema.builder()
        .addNullableField("FieldString", Schema.FieldType.STRING).build();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValue(null).build();
    System.out.println(row);
    String csv = new RowToCsv(",").getCsvFromRow(row);
    Assert.assertEquals("null", csv);
  }

  @Test
  public void testRowToCSV() {
    Schema beamSchema = new SchemasUtils(testSchema).getBeamSchema();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder
        .addValues(new ArrayList<>(Arrays.asList(fields)))
        .build();
    String csvResult = new RowToCsv(";").getCsvFromRow(row);
    Assert.assertEquals(String.join(";", fields), csvResult);
  }

  @Test
  public void testGcsIOReadCSV() {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.create().as(ProtegrityDataTokenizationOptions.class);
    options.setDataSchemaGcsPath(SCHEMA_FILE_PATH);
    options.setInputGcsFilePattern(SCV_FILE_PATH);
    options.setInputGcsFileFormat(FORMAT.CSV);
    options.setCsvContainsHeaders(Boolean.FALSE);

    SchemasUtils testSchema = setSchemaAndCoder(options);

    PCollection<String> jsons = new GcsIO(options)
        .read(testPipeline, testSchema.getJsonBeamSchema());

    PAssert.that(jsons).satisfies(x -> {
      LinkedList<String> rows = Lists.newLinkedList(x);
      assertThat(rows, hasSize(3));
      rows.forEach(
          row -> {
            assertThat(
                row,
                startsWith("{\"Field1\":"));
          });
      return null;
    });
    testPipeline.run();
  }

  @Test
  public void testGcsIOReadJSON() {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.create().as(ProtegrityDataTokenizationOptions.class);
    options.setDataSchemaGcsPath(SCHEMA_FILE_PATH);
    options.setInputGcsFilePattern(JSON_FILE_PATH);
    options.setInputGcsFileFormat(FORMAT.JSON);

    SchemasUtils testSchema = setSchemaAndCoder(options);

    PCollection<String> jsons = new GcsIO(options)
        .read(testPipeline, testSchema.getJsonBeamSchema());

    PAssert.that(jsons).satisfies(x -> {
      LinkedList<String> rows = Lists.newLinkedList(x);
      assertThat(rows, hasSize(3));
      rows.forEach(
          row -> {
            assertThat(
                row,
                startsWith("{\"Field1\":"));
          });
      return null;
    });

    testPipeline.run();
  }

  private SchemasUtils setSchemaAndCoder(ProtegrityDataTokenizationOptions options) {
    SchemasUtils testSchema = null;
    try {
      testSchema = new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert testSchema != null;

    CoderRegistry coderRegistry = testPipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry
        .registerCoderForType(RowCoder.of(testSchema.getBeamSchema()).getEncodedTypeDescriptor(),
            RowCoder.of(testSchema.getBeamSchema()));
    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder = FailsafeElementCoder.of(
        RowCoder.of(testSchema.getBeamSchema()),
        RowCoder.of(testSchema.getBeamSchema())
    );
    coderRegistry
        .registerCoderForType(coder.getEncodedTypeDescriptor(), coder);
    return testSchema;
  }
}
