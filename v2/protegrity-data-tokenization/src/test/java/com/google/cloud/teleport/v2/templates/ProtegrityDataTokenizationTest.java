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

import static com.google.cloud.teleport.v2.transforms.BeamRowConverters.getCsvFromRow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.io.GoogleCloudStorageIO;
import com.google.cloud.teleport.v2.options.ProtegrityDataTokenizationOptions;
import com.google.cloud.teleport.v2.utils.BeamSchemaUtils;
import com.google.cloud.teleport.v2.utils.BeamSchemaUtils.SchemaParseException;
import com.google.cloud.teleport.v2.utils.IoFormats;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
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

  final String testSchema = "[{\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"name\":\"FieldName2\",\"type\":\"STRING\"}]";
  final String[] fields = {"TestValue1", "TestValue2"};

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  private static final String RESOURCES_DIR = "./";

  private static final String CSV_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.csv").getPath();

  private static final String JSON_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.txt").getPath();

  private static final String AVRO_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "testInput.avro").getPath();

  private static final String SCHEMA_FILE_PATH =
      Resources.getResource(RESOURCES_DIR + "schema.json").getPath();

  private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));

  @Test
  public void testGetBeamSchema() throws SchemaParseException, IOException {
    System.out.println(CSV_FILE_PATH);
    Schema expectedSchema =
        Schema.builder()
            .addField("FieldName1", FieldType.STRING)
            .addField("FieldName2", FieldType.STRING)
            .build();
    Schema beamSchema = BeamSchemaUtils.fromJson(testSchema);
    Assert.assertEquals(expectedSchema, beamSchema);
  }

  @Test
  public void testGetBigQuerySchema() throws SchemaParseException, IOException {
    final String testBigQuerySchema = "{\"fields\":[{\"mode\":\"REQUIRED\",\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"mode\":\"REQUIRED\",\"name\":\"FieldName2\",\"type\":\"STRING\"}]}";
    Schema beamSchema = BeamSchemaUtils.fromJson(testSchema);
    TableSchema tableSchema = BigQueryHelpers.fromJsonString(testBigQuerySchema, TableSchema.class);
    Assert.assertEquals(tableSchema,
        BeamSchemaUtils.beamSchemaToBigQuerySchema(beamSchema));
  }

  @Test
  public void testRowToCSV() throws SchemaParseException, IOException {
    Schema beamSchema = BeamSchemaUtils.fromJson(testSchema);
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValues(new ArrayList<>(Arrays.asList(fields))).build();
    String csvResult = getCsvFromRow(row, ";", "null");
    Assert.assertEquals(String.join(";", fields), csvResult);
  }

  @Test
  public void testRowToCSVWithNull() throws SchemaParseException, IOException {
    final String nullableTestSchema = "[{\"nullable\":false,\"name\":\"FieldName1\",\"type\":\"STRING\"},{\"nullable\":true,\"name\":\"FieldName2\",\"type\":\"STRING\"}]}";
    final String expectedCsv = "TestValueOne;null";

    List<Object> values = Lists.newArrayList("TestValueOne", null);

    Schema beamSchema = BeamSchemaUtils.fromJson(nullableTestSchema);
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValues(values).build();
    String csvResult = getCsvFromRow(row, ";", "null");
    Assert.assertEquals(expectedCsv, csvResult);
  }

  @Test
  public void testFileSystemIOReadCSV() throws IOException, SchemaParseException {
    PCollection<Row> rows = fileSystemIORead(CSV_FILE_PATH, IoFormats.CSV);

    assertField(rows);
    testPipeline.run();
  }

  @Test
  public void testFileSystemIOReadJSON() throws IOException, SchemaParseException {
    PCollection<Row> rows = fileSystemIORead(JSON_FILE_PATH, IoFormats.JSON);
    assertField(rows);
    testPipeline.run();
  }


  @Test
  public void testFileSystemIOReadAVRO() throws IOException, SchemaParseException {
    PCollection<Row> rows = fileSystemIORead(AVRO_FILE_PATH, IoFormats.AVRO);
    assertField(rows);
    testPipeline.run();
  }

  @Test
  public void testJsonToRow() throws IOException, SchemaParseException {
    PCollection<Row> rows = fileSystemIORead(JSON_FILE_PATH, IoFormats.JSON);
    PAssert.that(rows)
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

  private PCollection<Row> fileSystemIORead(
      String inputGcsFilePattern, IoFormats inputGcsFileFormat)
      throws IOException, SchemaParseException {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.create().as(ProtegrityDataTokenizationOptions.class);
    options.setDataSchemaGcsPath(SCHEMA_FILE_PATH);
    options.setInputGcsFilePattern(inputGcsFilePattern);
    options.setInputGcsFileFormat(inputGcsFileFormat);
    if (inputGcsFileFormat == IoFormats.CSV) {
      options.setCsvContainsHeaders(Boolean.FALSE);
    }

    Schema beamSchema = BeamSchemaUtils
        .fromJson(new String(Files.readAllBytes(Paths.get(options.getDataSchemaGcsPath())),
            StandardCharsets.UTF_8));

    CoderRegistry coderRegistry = testPipeline.getCoderRegistry();
    coderRegistry.registerCoderForType(
        FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor(), FAILSAFE_ELEMENT_CODER);
    coderRegistry.registerCoderForType(
        RowCoder.of(beamSchema).getEncodedTypeDescriptor(),
        RowCoder.of(beamSchema));
    /*
     * Row/Row Coder for FailsafeElement.
     */
    FailsafeElementCoder<Row, Row> coder =
        FailsafeElementCoder.of(
            RowCoder.of(beamSchema),
            RowCoder.of(beamSchema));
    coderRegistry.registerCoderForType(coder.getEncodedTypeDescriptor(), coder);

    GoogleCloudStorageIO.Read read = GoogleCloudStorageIO
        .read(options, beamSchema);

    switch (options.getInputGcsFileFormat()) {
      case CSV:
        return testPipeline.apply(read.csv(options));
      case JSON:
        return testPipeline.apply(read.json());
      case AVRO:
        return testPipeline.apply(read.avro());
      default:
        throw new IllegalStateException(
            "No valid format for input data is provided. Please, choose JSON, CSV or AVRO.");
    }
  }

  private void assertField(PCollection<Row> rows) {

    PAssert.that(rows)
        .satisfies(
            x -> {
              LinkedList<Row> items = Lists.newLinkedList(x);
              assertThat(items, hasSize(3));
              items.forEach(
                  item -> {
                    assertNotNull(item.getSchema());
                    assertThat(item.getSchema().getFields(), hasSize(3));
                    assertThat(item.getSchema().getField(0).getName(), equalTo("Field1"));

                    assertThat(item.getValues(), hasSize(3));
                  });
              return null;
            });
  }
}
