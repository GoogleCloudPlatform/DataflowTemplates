/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Tests import of Avro files. This requires an active GCP project with a Spanner instance. Hence
 * this test can only be run locally with a project set up using 'gcloud config'.
 */
@Category(IntegrationTest.class)
public class ImportFromAvroTest {
  @Rule public final TestPipeline importPipeline = TestPipeline.create();
  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();
  @Rule public final SpannerServerResource spannerServer = new SpannerServerResource();

  private final String dbName = "importdbtest";

  @Before
  public void setup() {
    // Just to make sure an old database is not left over.
    spannerServer.dropDatabase(dbName);
  }

  @After
  public void tearDown() {
    spannerServer.dropDatabase(dbName);
  }

  @Test
  public void booleans() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("booleans");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Boolean columns.
        .optionalBoolean("optional_boolean")
        .requiredBoolean("required_boolean")
        .optionalString("optional_string_boolean")
        .requiredString("required_string_boolean");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_boolean`                      BOOL,"
            + "`required_boolean`                      BOOL NOT NULL,"
            + "`optional_string_boolean`               BOOL,"
            + "`required_string_boolean`               BOOL NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("required_boolean", true)
                .set("optional_boolean", false)
                .set("required_string_boolean", "FALSE")
                .set("optional_string_boolean", "TRUE")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("required_boolean", false)
                .set("optional_boolean", true)
                .set("required_string_boolean", "true")
                .set("optional_string_boolean", "f")
                .build()));
  }

  @Test
  public void integers() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("integers");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalInt("optional_int")
        .requiredInt("required_int")
        .requiredLong("required_long")
        .optionalLong("optional_long")
        .optionalString("optional_string_long")
        .requiredString("required_string_long");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_int`                          INT64,"
            + "`required_int`                          INT64 NOT NULL,"
            + "`optional_long`                         INT64,"
            + "`required_long`                         INT64 NOT NULL,"
            + "`optional_string_long`                  INT64,"
            + "`required_string_long`                  INT64 NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_int", 1)
                .set("optional_long", 2L)
                .set("required_long", 3L)
                .set("required_int", 4)
                .set("optional_string_long", "1000")
                .set("required_string_long", "5000")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_int", 10)
                .set("optional_long", 20L)
                .set("required_long", 30L)
                .set("required_int", 40)
                .set("optional_string_long", "10000")
                .set("required_string_long", "50000")
                .build()));
  }

  @Test
  public void floats() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("floats");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalInt("optional_int")
        .requiredInt("required_int")
        .requiredLong("required_long")
        .optionalLong("optional_long")
        // Floating columns
        .optionalFloat("optional_float")
        .requiredFloat("required_float")
        .requiredDouble("required_double")
        .optionalDouble("optional_double")
        .optionalString("optional_string_double")
        .requiredString("required_string_double");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_int`                          FLOAT64,"
            + "`required_int`                          FLOAT64 NOT NULL,"
            + "`optional_long`                         FLOAT64,"
            + "`required_long`                         FLOAT64 NOT NULL,"
            + "`optional_float`                        FLOAT64,"
            + "`required_float`                        FLOAT64 NOT NULL,"
            + "`optional_double`                       FLOAT64,"
            + "`required_double`                       FLOAT64 NOT NULL,"
            + "`optional_string_double`                FLOAT64,"
            + "`required_string_double`                FLOAT64 NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_int", 1)
                .set("required_int", 4)
                .set("optional_long", 2L)
                .set("required_long", 3L)
                .set("optional_float", 2.3f)
                .set("required_float", 3.4f)
                .set("optional_double", 2.5)
                .set("required_double", 3.6)
                .set("optional_string_double", "100.30")
                .set("required_string_double", "0.1e-3")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_int", 10)
                .set("required_int", 40)
                .set("optional_long", 20L)
                .set("required_long", 30L)
                .set("optional_float", 2.03f)
                .set("required_float", 3.14f)
                .set("optional_double", 2.05)
                .set("required_double", 3.16)
                .set("optional_string_double", "100.301")
                .set("required_string_double", "1.1e-3")
                .build()));
  }

  @Test
  public void strings() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("strings");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalInt("optional_int")
        .requiredInt("required_int")
        .requiredLong("required_long")
        .optionalLong("optional_long")
        // Floating columns
        .optionalFloat("optional_float")
        .requiredFloat("required_float")
        .requiredDouble("required_double")
        .optionalDouble("optional_double")
        // String columns
        .optionalString("optional_string")
        .requiredString("required_string");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_int`                          STRING(10),"
            + "`required_int`                          STRING(MAX) NOT NULL,"
            + "`optional_long`                         STRING(MAX),"
            + "`required_long`                         STRING(MAX) NOT NULL,"
            + "`optional_float`                        STRING(MAX),"
            + "`required_float`                        STRING(MAX) NOT NULL,"
            + "`optional_double`                       STRING(MAX),"
            + "`required_double`                       STRING(MAX) NOT NULL,"
            + "`optional_string`                       STRING(MAX),"
            + "`required_string`                       STRING(30) NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_int", 1)
                .set("required_int", 4)
                .set("optional_long", 2L)
                .set("required_long", 3L)
                .set("optional_float", 2.3f)
                .set("required_float", 3.4f)
                .set("optional_double", 2.5)
                .set("required_double", 3.6)
                .set("optional_string", "ONE STRING")
                .set("required_string", "TWO STRING")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_int", 10)
                .set("required_int", 40)
                .set("optional_long", 20L)
                .set("required_long", 30L)
                .set("optional_float", 2.03f)
                .set("required_float", 3.14f)
                .set("optional_double", 2.05)
                .set("required_double", 3.16)
                .set("optional_string", null)
                .set("required_string", "THE STRING")
                .build()));
  }

  @Test
  public void timestamps() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("timestamps");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Long columns.
        .requiredLong("required_long")
        .optionalLong("optional_long")
        // String columns
        .optionalString("optional_string")
        .requiredString("required_string");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_long`                         TIMESTAMP,"
            + "`required_long`                         TIMESTAMP NOT NULL,"
            + "`optional_string`                       TIMESTAMP,"
            + "`required_string`                       TIMESTAMP NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_long", 5000000L)
                .set("required_long", 6000000L)
                .set("optional_string", "2018-06-06T21:00:35.312000000Z")
                .set("required_string", "2018-06-06T21:00:35.312000000Z")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_long", 500000330L)
                .set("required_long", 6000020000L)
                .set("optional_string", "2017-06-06T21:00:35.312000000Z")
                .set("required_string", "2017-06-06T21:00:35.312000000Z")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 3L)
                .set("optional_long", null)
                .set("required_long", 6000020000L)
                .set("optional_string", null)
                .set("required_string", "0001-01-01T00:00:00Z")
                .build()));
  }

  @Test
  public void dates() throws Exception {
    // Unfortunately Avro SchemaBuilder has a limitation of not allowing nullable LogicalTypes.
    Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema schema =
        SchemaBuilder.record("dates")
            .fields()
            // Primary key.
            .requiredLong("id")
            // String columns
            .optionalString("optional_string")
            .requiredString("required_string")
            .name("required_int")
            .type(dateType)
            .noDefault()
            .endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_string`                       DATE,"
            + "`required_string`                       DATE NOT NULL,"
            + "`required_int`                          DATE NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_string", "2018-03-04")
                .set("required_string", "2018-04-04")
                .set("required_int", 2)
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_string", null)
                .set("required_string", "2018-01-02")
                .set("required_int", 3)
                .build()));
  }

  // TODO: enable this test once generated columns are supported.
  // @Test
  public void generatedColumns() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("generatedColumns");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalLong("optional_generated")
        .requiredLong("required_generated");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_generated`                    INT64 AS (`id`) STORED,"
            + "`required_generated`                    INT64 NOT NULL AS (`id`) STORED,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_generated", 1L)
                .set("required_generated", 1L)
                .build()));
  }

  private void runTest(Schema avroSchema, String spannerSchema, Iterable<GenericRecord> records)
      throws Exception {
    // Create the Avro file to be imported.
    String fileName = "avroFile.avro";
    ExportProtos.Export exportProto =
        ExportProtos.Export.newBuilder()
            .addTables(
                ExportProtos.Export.Table.newBuilder()
                    .setName("AvroTable")
                    .addDataFiles(fileName)
                    .build())
            .addDatabaseOptions(
                ExportProtos.Export.DatabaseOption.newBuilder()
                    .setOptionName("version_retention_period")
                    .setOptionValue("\"4d\"")
                    .build())
            .build();
    JsonFormat.printer().print(exportProto);

    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    File avroFile = tmpDir.newFile(fileName);
    try (DataFileWriter<GenericRecord> fileWriter =
        new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);

      for (GenericRecord r : records) {
        fileWriter.append(r);
      }
      fileWriter.flush();
    }
    // Create the target database.
    spannerServer.createDatabase(dbName, Collections.singleton(spannerSchema));
    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    assertThat(ddl.databaseOptions().size(), is(1));
    ExportProtos.Export.DatabaseOption dbOption = ddl.databaseOptions().get(0);
    assertThat(dbOption.getOptionName(), is("version_retention_period"));
    assertThat(dbOption.getOptionValue(), is("4d"));
  }
}
