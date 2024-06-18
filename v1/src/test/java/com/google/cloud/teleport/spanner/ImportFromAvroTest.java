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

import static org.hamcrest.Matchers.equalToCompressingWhiteSpace;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.teleport.spanner.common.NumericUtils;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.ddl.InformationSchemaScanner;
import com.google.cloud.teleport.spanner.proto.ExportProtos;
import com.google.cloud.teleport.spanner.proto.ExportProtos.ProtoDialect;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  public void pgBooleans() throws Exception {
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
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_boolean\"                      boolean,"
            + "\"required_boolean\"                      boolean NOT NULL,"
            + "\"optional_string_boolean\"               boolean,"
            + "\"required_string_boolean\"               boolean NOT NULL,"
            + " PRIMARY KEY (\"id\"))";

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
                .build()),
        Dialect.POSTGRESQL);
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
  public void pgIntegers() throws Exception {
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
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_int\"                          bigint,"
            + "\"required_int\"                          bigint NOT NULL,"
            + "\"optional_long\"                         bigint,"
            + "\"required_long\"                         bigint NOT NULL,"
            + "\"optional_string_long\"                  bigint,"
            + "\"required_string_long\"                  bigint NOT NULL,"
            + " PRIMARY KEY (\"id\"))";

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
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
  public void float64s() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("float64s");
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
  public void pgFloat64s() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("float64s");
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
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_int\"                          double precision,"
            + "\"required_int\"                          double precision NOT NULL,"
            + "\"optional_long\"                         double precision,"
            + "\"required_long\"                         double precision NOT NULL,"
            + "\"optional_float\"                        double precision,"
            + "\"required_float\"                        double precision NOT NULL,"
            + "\"optional_double\"                       double precision,"
            + "\"required_double\"                       double precision NOT NULL,"
            + "\"optional_string_double\"                double precision,"
            + "\"required_string_double\"                double precision NOT NULL,"
            + " PRIMARY KEY (\"id\"))";

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
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
  public void float32s() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("float32s");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalInt("optional_int")
        .requiredInt("required_int")
        // Floating columns
        .optionalFloat("optional_float")
        .requiredFloat("required_float")
        .optionalString("optional_string_float")
        .requiredString("required_string_float");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                    INT64 NOT NULL,"
            + "`optional_int`                          FLOAT32,"
            + "`required_int`                          FLOAT32 NOT NULL,"
            + "`optional_float`                        FLOAT32,"
            + "`required_float`                        FLOAT32 NOT NULL,"
            + "`optional_string_float`                 FLOAT32,"
            + "`required_string_float`                 FLOAT32 NOT NULL,"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_int", 1)
                .set("required_int", 4)
                .set("optional_float", 2.3f)
                .set("required_float", 3.4f)
                .set("optional_string_float", "100.30")
                .set("required_string_float", "0.1e-3")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_int", 10)
                .set("required_int", 40)
                .set("optional_float", 2.03f)
                .set("required_float", 3.14f)
                .set("optional_string_float", "100.301")
                .set("required_string_float", "1.1e-3")
                .build()));
  }

  @Test
  public void pgFloat32s() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("float32s");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalInt("optional_int")
        .requiredInt("required_int")
        // Floating columns
        .optionalFloat("optional_float")
        .requiredFloat("required_float")
        .optionalString("optional_string_float")
        .requiredString("required_string_float");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_int\"                          real,"
            + "\"required_int\"                          real NOT NULL,"
            + "\"optional_float\"                        real,"
            + "\"required_float\"                        real NOT NULL,"
            + "\"optional_string_float\"                 real,"
            + "\"required_string_float\"                 real NOT NULL,"
            + " PRIMARY KEY (\"id\"))";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_int", 1)
                .set("required_int", 4)
                .set("optional_float", 2.3f)
                .set("required_float", 3.4f)
                .set("optional_string_float", "100.30")
                .set("required_string_float", "0.1e-3")
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_int", 10)
                .set("required_int", 40)
                .set("optional_float", 2.03f)
                .set("required_float", 3.14f)
                .set("optional_string_float", "100.301")
                .set("required_string_float", "1.1e-3")
                .build()),
        Dialect.POSTGRESQL);
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
  public void pgStrings() throws Exception {
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
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_int\"                          character varying(10),"
            + "\"required_int\"                          character varying NOT NULL,"
            + "\"optional_long\"                         character varying,"
            + "\"required_long\"                         character varying NOT NULL,"
            + "\"optional_float\"                        character varying,"
            + "\"required_float\"                        character varying NOT NULL,"
            + "\"optional_double\"                       character varying,"
            + "\"required_double\"                       character varying NOT NULL,"
            + "\"optional_string\"                       character varying,"
            + "\"required_string\"                       character varying(30) NOT NULL,"
            + " PRIMARY KEY (\"id\"))";

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
                .build()),
        Dialect.POSTGRESQL);
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
  public void pgTimestamptzs() throws Exception {
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
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_long\"                         timestamp with time zone,"
            + "\"required_long\"                         timestamp with time zone NOT NULL,"
            + "\"optional_string\"                       timestamp with time zone,"
            + "\"required_string\"                       timestamp with time zone NOT NULL,"
            + " PRIMARY KEY (\"id\"))";

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
                .build()),
        Dialect.POSTGRESQL);
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

  @Test
  public void pgDates() throws Exception {
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
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_string\"                       date,"
            + "\"required_string\"                       date NOT NULL,"
            + "\"required_int\"                          date NOT NULL,"
            + "PRIMARY KEY (\"id\")) ";

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
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
  public void pgNumerics() throws Exception {
    // Unfortunately Avro SchemaBuilder has a limitation of not allowing nullable LogicalTypes.
    Schema numericType =
        LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
            .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema =
        SchemaBuilder.record("numerics")
            .fields()
            // Primary key.
            .requiredLong("id")
            // Bytes columns
            .optionalBytes("optional_bytes")
            .requiredBytes("required_bytes")
            .name("required_decimal")
            .type(numericType)
            .noDefault()
            .endRecord();
    String spannerSchema =
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"optional_bytes\"                        numeric,"
            + "\"required_bytes\"                        numeric NOT NULL,"
            + "\"required_decimal\"                      numeric NOT NULL,"
            + "PRIMARY KEY (\"id\")) ";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_bytes", ByteBuffer.wrap(NumericUtils.pgStringToBytes("NaN")))
                .set(
                    "required_bytes",
                    ByteBuffer.wrap(NumericUtils.pgStringToBytes("81313218843.0000000")))
                .set(
                    "required_decimal",
                    ByteBuffer.wrap(NumericUtils.pgStringToBytes("10.0000000010")))
                .build(),
            new GenericRecordBuilder(schema)
                .set("id", 2L)
                .set("optional_bytes", null)
                .set(
                    "required_bytes",
                    ByteBuffer.wrap(NumericUtils.pgStringToBytes("-0.218311815433216841313811548")))
                .set(
                    "required_decimal",
                    ByteBuffer.wrap(NumericUtils.pgStringToBytes("-711384543184123843132.483124")))
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
  public void pgNumericArray() throws Exception {
    // Unfortunately Avro SchemaBuilder has a limitation of not allowing nullable LogicalTypes.
    Schema numericType =
        LogicalTypes.decimal(NumericUtils.PG_MAX_PRECISION, NumericUtils.PG_MAX_SCALE)
            .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema numericArrayType =
        SchemaBuilder.builder().array().items().type(wrapAsNullable(numericType));

    Schema schema =
        SchemaBuilder.record("numerics")
            .fields()
            // Primary key.
            .requiredLong("id")
            .name("required_numeric_arr")
            .type(numericArrayType)
            .noDefault()
            .endRecord();
    String spannerSchema =
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                                    bigint NOT NULL,"
            + "\"required_numeric_arr\"                  numeric[] NOT NULL,"
            + "PRIMARY KEY (\"id\")) ";

    StringBuilder maxPgNumeric = new StringBuilder();
    StringBuilder minPgNumeric = new StringBuilder("-");
    for (int i = 0; i < NumericUtils.PG_MAX_PRECISION - NumericUtils.PG_MAX_SCALE; i++) {
      maxPgNumeric.append("9");
      minPgNumeric.append("9");
    }
    maxPgNumeric.append(".");
    minPgNumeric.append(".");
    for (int i = 0; i < NumericUtils.PG_MAX_SCALE; i++) {
      maxPgNumeric.append("9");
      minPgNumeric.append("9");
    }

    String[] arrValues = {
      null,
      "NaN",
      null,
      maxPgNumeric.toString(),
      minPgNumeric.toString(),
      "-9305028.140032",
      "-25398514232141142.012479",
      "1999999999.1246"
    };
    List<ByteBuffer> pgNumericArr =
        Stream.of(arrValues)
            .map(x -> x == null ? null : ByteBuffer.wrap(NumericUtils.pgStringToBytes(x)))
            .collect(Collectors.toList());

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("required_numeric_arr", pgNumericArr)
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
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

  @Test
  public void pgGeneratedColumns() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("generatedColumns");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalLong("optional_generated")
        .requiredLong("required_generated")
        .optionalLong("optional_generated_2");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                          bigint NOT NULL,"
            + "\"optional_generated\"          bigint GENERATED ALWAYS AS (\"id\") STORED,"
            + "\"required_generated\"          bigint NOT NULL GENERATED ALWAYS AS (\"id\") STORED,"
            + "\"optional_generated_2\"        bigint NOT NULL GENERATED ALWAYS AS (\"id\") STORED,"
            + "PRIMARY KEY (\"id\"))";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_generated", 1L)
                .set("required_generated", 1L)
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
  public void defaultColumns() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("defaultColumns");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalLong("optional_default")
        .requiredLong("required_default")
        .optionalLong("optional_default_2");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE `AvroTable` ("
            + "`id`                                  INT64 NOT NULL,"
            + "`optional_default`                    INT64 DEFAULT (1),"
            + "`required_default`                    INT64 NOT NULL DEFAULT (2),"
            + "`optional_default_2`                  INT64 DEFAULT (3),"
            + ") PRIMARY KEY (`id`)";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_default", 1L)
                .set("required_default", 1L)
                .build()));
  }

  @Test
  public void pgDefaultColumns() throws Exception {
    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("defaultColumns");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler
        // Primary key.
        .requiredLong("id")
        // Integer columns.
        .optionalLong("optional_default")
        .requiredLong("required_default")
        .optionalLong("optional_default_2");
    Schema schema = fieldAssembler.endRecord();
    String spannerSchema =
        "CREATE TABLE \"AvroTable\" ("
            + "\"id\"                            bigint NOT NULL,"
            + "\"optional_default\"              bigint DEFAULT '1'::bigint,"
            + "\"required_default\"              bigint DEFAULT '2'::bigint,"
            + "\"optional_default_2\"            bigint DEFAULT '3'::bigint,"
            + "PRIMARY KEY (\"id\"))";

    runTest(
        schema,
        spannerSchema,
        Arrays.asList(
            new GenericRecordBuilder(schema)
                .set("id", 1L)
                .set("optional_default", 1L)
                .set("required_default", 1L)
                .build()),
        Dialect.POSTGRESQL);
  }

  @Test
  public void models() throws Exception {
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put(
        "ModelAll.avro",
        SchemaBuilder.record("Iris")
            .prop("spannerEntity", "Model")
            .prop("spannerRemote", "true")
            .prop(
                "spannerOption_0",
                "endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/endpoints/4608339105032437760\"")
            .fields()
            // Input columns.
            .name("Input")
            .type()
            .record("Iris_Input")
            .fields()
            .name("f1")
            .prop("sqlType", "FLOAT64")
            .type()
            .booleanType()
            .noDefault()
            .name("f2")
            .prop("sqlType", "FLOAT64")
            .type()
            .booleanType()
            .noDefault()
            .name("f3")
            .prop("sqlType", "FLOAT64")
            .type()
            .booleanType()
            .noDefault()
            .name("f4")
            .prop("sqlType", "FLOAT64")
            .type()
            .booleanType()
            .noDefault()
            .endRecord()
            .noDefault()
            // Output columns.
            .name("Output")
            .type()
            .record("Iris_Output")
            .fields()
            .name("classes")
            .prop("sqlType", "ARRAY<STRING(MAX)>")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .name("scores")
            .prop("sqlType", "ARRAY<FLOAT64>")
            .type()
            .array()
            .items()
            .longType()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord());
    avroFiles.put(
        "ModelStruct.avro",
        SchemaBuilder.record("TextEmbeddingGecko")
            .prop("spannerEntity", "Model")
            .prop("spannerRemote", "true")
            .prop(
                "spannerOption_0",
                "endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/publishers/google/models/textembedding-gecko\"")
            .fields()
            // Input columns.
            .name("Input")
            .type()
            .record("TextEmbeddingGecko_Input")
            .fields()
            .name("content")
            .prop("sqlType", "STRING(MAX)")
            .type()
            .stringType()
            .noDefault()
            .endRecord()
            .noDefault()
            // Output columns.
            .name("Output")
            .type()
            .record("TextEmbeddingGecko_Output")
            .fields()
            .name("embeddings")
            .prop(
                "sqlType",
                "STRUCT<statistics STRUCT<truncated BOOL, token_count FLOAT64>, values ARRAY<FLOAT64>>")
            .type()
            .record("struct_ModelStruct_output_0")
            .fields()
            .name("statistics")
            .type()
            .record("struct_ModelStruct_output_0_1")
            .fields()
            .name("truncated")
            .type()
            .booleanType()
            .noDefault()
            .name("token_count")
            .type()
            .doubleType()
            .noDefault()
            .endRecord()
            .noDefault()
            .name("values")
            .type()
            .array()
            .items()
            .unionOf()
            .nullType()
            .and()
            .doubleType()
            .endUnion()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord()
            .noDefault()
            .endRecord());

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addTables(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.
    String spannerSchema =
        "CREATE TABLE `T` ("
            + "`id` INT64 NOT NULL,"
            + "`c1` BOOL,"
            + "`c2` INT64,"
            + ") PRIMARY KEY (`id`)";
    spannerServer.createDatabase(dbName, Collections.singleton(spannerSchema));

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `T`"
                + " ( `id` INT64 NOT NULL, `c1` BOOL, `c2` INT64, )"
                + " PRIMARY KEY (`id` ASC)"
                + " CREATE MODEL `Iris`"
                + " INPUT ( `f1` FLOAT64, `f2` FLOAT64, `f3` FLOAT64, `f4` FLOAT64, )"
                + " OUTPUT ( `classes` ARRAY<STRING(MAX)>, `scores` ARRAY<FLOAT64>, )"
                + " REMOTE OPTIONS (endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/endpoints/4608339105032437760\")"
                + " CREATE MODEL `TextEmbeddingGecko`"
                + " INPUT ( `content` STRING(MAX), )"
                + " OUTPUT ( `embeddings` STRUCT<statistics STRUCT<truncated BOOL, token_count FLOAT64>, values ARRAY<FLOAT64>>, )"
                + " REMOTE OPTIONS (endpoint=\"//aiplatform.googleapis.com/projects/span-cloud-testing/locations/us-central1/publishers/google/models/textembedding-gecko\")"));
  }

  @Test
  public void changeStreams() throws Exception {
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put(
        "ChangeStreamAll.avro",
        SchemaBuilder.record("ChangeStreamAll")
            .prop("spannerChangeStreamForClause", "FOR ALL")
            .prop("spannerOption_0", "retention_period=\"7d\"")
            .prop("spannerOption_1", "value_capture_type=\"OLD_AND_NEW_VALUES\"")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamEmpty.avro",
        SchemaBuilder.record("ChangeStreamEmpty")
            .prop("spannerChangeStreamForClause", "")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamTable.avro",
        SchemaBuilder.record("ChangeStreamTable")
            .prop("spannerChangeStreamForClause", "FOR T")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamColumns.avro",
        SchemaBuilder.record("ChangeStreamColumns")
            .prop("spannerChangeStreamForClause", "FOR T(c1, c2)")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamKeyOnly.avro",
        SchemaBuilder.record("ChangeStreamKeyOnly")
            .prop("spannerChangeStreamForClause", "FOR T()")
            .fields()
            .endRecord());

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addChangeStreams(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.
    String spannerSchema =
        "CREATE TABLE `T` ("
            + "`id` INT64 NOT NULL,"
            + "`c1` BOOL,"
            + "`c2` INT64,"
            + ") PRIMARY KEY (`id`)";
    spannerServer.createDatabase(dbName, Collections.singleton(spannerSchema));

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE `T` ("
                + " `id`                                    INT64 NOT NULL,"
                + " `c1`                                    BOOL,"
                + " `c2`                                    INT64,"
                + " ) PRIMARY KEY (`id` ASC)"
                + " CREATE CHANGE STREAM `ChangeStreamAll`"
                + " FOR ALL"
                + " OPTIONS (retention_period=\"7d\", value_capture_type=\"OLD_AND_NEW_VALUES\")"
                + " CREATE CHANGE STREAM `ChangeStreamColumns`"
                + " FOR `T`(`c1`, `c2`)"
                + " CREATE CHANGE STREAM `ChangeStreamEmpty`"
                + " CREATE CHANGE STREAM `ChangeStreamKeyOnly`"
                + " FOR `T`()"
                + " CREATE CHANGE STREAM `ChangeStreamTable`"
                + " FOR `T`"));
  }

  // TODO: Enable the test once change streams are supported in PG.
  // @Test
  public void pgChangeStreams() throws Exception {
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put(
        "ChangeStreamAll.avro",
        SchemaBuilder.record("ChangeStreamAll")
            .prop("spannerChangeStreamForClause", "FOR ALL")
            .prop("spannerOption_0", "retention_period='7d'")
            .prop("spannerOption_1", "value_capture_type='OLD_AND_NEW_VALUES'")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamEmpty.avro",
        SchemaBuilder.record("ChangeStreamEmpty")
            .prop("spannerChangeStreamForClause", "")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamTable.avro",
        SchemaBuilder.record("ChangeStreamTable")
            .prop("spannerChangeStreamForClause", "FOR \"T\"")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamColumns.avro",
        SchemaBuilder.record("ChangeStreamColumns")
            .prop("spannerChangeStreamForClause", "FOR \"T\"(\"c1\", \"c2\")")
            .fields()
            .endRecord());
    avroFiles.put(
        "ChangeStreamKeyOnly.avro",
        SchemaBuilder.record("ChangeStreamKeyOnly")
            .prop("spannerChangeStreamForClause", "FOR \"T\"()")
            .fields()
            .endRecord());

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    exportProtoBuilder.setDialect(ProtoDialect.POSTGRESQL);
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addChangeStreams(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.
    String spannerSchema =
        "CREATE TABLE \"T\" ("
            + "\"id\" bigint NOT NULL,"
            + "\"c1\" boolean,"
            + "\"c2\" bigint,"
            + " PRIMARY KEY (\"id\"))";
    spannerServer.createPgDatabase(dbName, Collections.singleton(spannerSchema));

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx, Dialect.POSTGRESQL).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE TABLE \"T\" ("
                + " \"id\"                                    bigint NOT NULL,"
                + " \"c1\"                                    boolean,"
                + " \"c2\"                                    bigint,"
                + " PRIMARY KEY (\"id\")"
                + " )"
                + " CREATE CHANGE STREAM \"ChangeStreamAll\""
                + " FOR ALL"
                + " WITH (retention_period='7d', value_capture_type='OLD_AND_NEW_VALUES')"
                + " CREATE CHANGE STREAM \"ChangeStreamColumns\""
                + " FOR \"T\"(\"c1\", \"c2\")"
                + " CREATE CHANGE STREAM \"ChangeStreamEmpty\""
                + " CREATE CHANGE STREAM \"ChangeStreamKeyOnly\""
                + " FOR \"T\"()"
                + " CREATE CHANGE STREAM \"ChangeStreamTable\""
                + " FOR \"T\""));
  }

  @Test
  public void sequences() throws Exception {
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put(
        "Sequence1.avro",
        SchemaBuilder.record("Sequence1")
            .prop("sequenceOption_0", "sequence_kind=\"bit_reversed_positive\"")
            .prop("sequenceOption_1", "skip_range_min=10")
            .prop("sequenceOption_2", "skip_range_max=10000")
            .prop("sequenceOption_3", "start_with_counter=99")
            .fields()
            .endRecord());

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addSequences(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.
    String sequenceDef =
        "CREATE SEQUENCE `Sequence2`"
            + " OPTIONS (sequence_kind=\"bit_reversed_positive\", "
            + " skip_range_min=0, skip_range_max=1000, start_with_counter=50)";
    String tableDef =
        "CREATE TABLE `T` ("
            + "`id` INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE Sequence2)),"
            + "`c1` BOOL,"
            + "`c2` INT64,"
            + ") PRIMARY KEY (`id`)";

    spannerServer.createDatabase(dbName, Arrays.asList(sequenceDef, tableDef));

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE SEQUENCE `Sequence1`\n\tOPTIONS "
                + "(sequence_kind=\"bit_reversed_positive\","
                + " skip_range_max=10000,"
                + " skip_range_min=10,"
                + " start_with_counter=99)\n"
                + "\nCREATE SEQUENCE `Sequence2`\n\tOPTIONS "
                + "(sequence_kind=\"bit_reversed_positive\","
                + " skip_range_max=1000,"
                + " skip_range_min=0,"
                + " start_with_counter=50)"
                + "CREATE TABLE `T` (\n\t"
                + "`id`                                    INT64 NOT NULL "
                + "DEFAULT  (GET_NEXT_SEQUENCE_VALUE(SEQUENCE Sequence2)),\n\t"
                + "`c1`                                    BOOL,\n\t"
                + "`c2`                                    INT64,\n) "
                + "PRIMARY KEY (`id` ASC)\n\n"));
  }

  @Test
  public void pgSequences() throws Exception {
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put(
        "Sequence1.avro",
        SchemaBuilder.record("PGSequence1")
            .prop("sequenceKind", "bit_reversed_positive")
            .prop("skipRangeMin", "10")
            .prop("skipRangeMax", "10000")
            .prop("counterStartValue", "99")
            .fields()
            .endRecord());

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    exportProtoBuilder.setDialect(ProtoDialect.POSTGRESQL);
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addSequences(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.
    String sequenceDef =
        "CREATE SEQUENCE \"PGSequence2\" BIT_REVERSED_POSITIVE"
            + " SKIP RANGE 0 1000 START COUNTER WITH 50";
    String tableDef =
        "CREATE TABLE \"T\" ("
            + "\"id\" bigint NOT NULL DEFAULT nextval('\"PGSequence2\"'),"
            + "\"c\" bigint,"
            + "PRIMARY KEY (\"id\"))";

    spannerServer.createPgDatabase(dbName, Arrays.asList(sequenceDef, tableDef));

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx, Dialect.POSTGRESQL).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE SEQUENCE \"PGSequence1\" BIT_REVERSED_POSITIVE"
                + " SKIP RANGE 10 10000 START COUNTER WITH 99"
                + "\nCREATE SEQUENCE \"PGSequence2\" BIT_REVERSED_POSITIVE"
                + " SKIP RANGE 0 1000 START COUNTER WITH 50"
                + "CREATE TABLE \"T\" ("
                + "\n\t\"id\"                                    bigint NOT NULL"
                + " DEFAULT nextval('\"PGSequence2\"'::text),\n\t"
                + "\"c\"                                     bigint,"
                + "\n\tPRIMARY KEY (\"id\")\n)\n\n"));
  }

  @Test
  public void protoDescriptors() throws Exception {

    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("T");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler.requiredLong("id").optionalBoolean("c1").optionalLong("c2");
    Schema tableSchema = fieldAssembler.endRecord();
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put("CreateTable.avro", tableSchema);

    FileDescriptorProto.Builder builder = FileDescriptorProto.newBuilder();
    builder
        .addMessageType(
            com.google.cloud.teleport.spanner.tests.TestMessage.getDescriptor().toProto())
        .addEnumType(com.google.cloud.teleport.spanner.tests.TestEnum.getDescriptor().toProto());
    FileDescriptorSet.Builder fileDescriptorSetBuilder = FileDescriptorSet.newBuilder();
    fileDescriptorSetBuilder.addFile(builder);
    ByteString protoDescriptorBytes = fileDescriptorSetBuilder.build().toByteString();

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    exportProtoBuilder.setProtoDescriptors(protoDescriptorBytes);
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addTables(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.

    spannerServer.createDatabase(dbName, Arrays.asList());

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();
  }

  @Test
  public void protoBundle() throws Exception {

    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("T");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler.requiredLong("id").optionalBoolean("c1").optionalLong("c2");
    Schema tableSchema = fieldAssembler.endRecord();
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put("CreateTable.avro", tableSchema);

    FileDescriptorSet.Builder fileDescriptorSetBuilder = FileDescriptorSet.newBuilder();
    fileDescriptorSetBuilder.addFile(
        com.google.cloud.teleport.spanner.tests.TestMessage.getDescriptor().getFile().toProto());
    ByteString protoDescriptorBytes = fileDescriptorSetBuilder.build().toByteString();
    ImmutableList<String> protoBundle =
        ImmutableList.of(
            "com.google.cloud.teleport.spanner.tests.TestMessage",
            "com.google.cloud.teleport.spanner.tests.TestEnum");

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    exportProtoBuilder.setProtoDescriptors(protoDescriptorBytes);
    exportProtoBuilder.addAllProtoBundle(protoBundle);
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addTables(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    // Create the target database.
    spannerServer.createDatabase(dbName, Arrays.asList());

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE PROTO BUNDLE ("
                + "\n\tcom.google.cloud.teleport.spanner.tests.TestMessage,"
                + " com.google.cloud.teleport.spanner.tests.TestEnum)"
                + "CREATE TABLE `T` (\n\t"
                + "`id`                                    INT64 NOT NULL, "
                + "\n\t`c1`                                    BOOL,\n\t"
                + "`c2`                                    INT64,\n) "
                + "PRIMARY KEY ()\n\n"));
  }

  @Test
  public void alterProtoBundle() throws Exception {

    SchemaBuilder.RecordBuilder<Schema> record = SchemaBuilder.record("T");
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler = record.fields();

    fieldAssembler.requiredLong("id").optionalBoolean("c1").optionalLong("c2");
    Schema tableSchema = fieldAssembler.endRecord();
    Map<String, Schema> avroFiles = new HashMap<>();
    avroFiles.put("CreateTable.avro", tableSchema);

    FileDescriptorSet.Builder fileDescriptorSetBuilder = FileDescriptorSet.newBuilder();
    fileDescriptorSetBuilder.addFile(
        com.google.cloud.teleport.spanner.tests.TestMessage.getDescriptor().getFile().toProto());
    ByteString protoDescriptorBytes = fileDescriptorSetBuilder.build().toByteString();
    ImmutableList<String> protoBundle =
        ImmutableList.of("com.google.cloud.teleport.spanner.tests.TestMessage");

    ExportProtos.Export.Builder exportProtoBuilder = ExportProtos.Export.newBuilder();
    exportProtoBuilder.setProtoDescriptors(protoDescriptorBytes);
    exportProtoBuilder.addAllProtoBundle(protoBundle);
    for (Entry<String, Schema> entry : avroFiles.entrySet()) {
      String fileName = entry.getKey();
      Schema schema = entry.getValue();
      exportProtoBuilder.addTables(
          ExportProtos.Export.Table.newBuilder()
              .setName(schema.getName())
              .addDataFiles(fileName)
              .build());
      // Create the Avro files to be imported.
      File avroFile = tmpDir.newFile(fileName);
      try (DataFileWriter<GenericRecord> fileWriter =
          new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
        fileWriter.create(schema, avroFile);
      }
    }

    // Create the database manifest file.
    ExportProtos.Export exportProto = exportProtoBuilder.build();
    File manifestFile = tmpDir.newFile("spanner-export.json");
    String manifestFileLocation = manifestFile.getParent();
    Files.write(
        manifestFile.toPath(),
        JsonFormat.printer().print(exportProto).getBytes(StandardCharsets.UTF_8));

    List<String> statements =
        Arrays.asList(
            "CREATE PROTO BUNDLE (" + "com.google.cloud.teleport.spanner.tests.TestEnum)");

    // Create the target database.
    spannerServer.createDatabase(dbName, statements, protoDescriptorBytes);

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx).scan();
    }
    assertThat(
        ddl.prettyPrint(),
        equalToCompressingWhiteSpace(
            "\nCREATE PROTO BUNDLE ("
                + "\n\tcom.google.cloud.teleport.spanner.tests.TestMessage,"
                + " com.google.cloud.teleport.spanner.tests.TestEnum)"
                + "CREATE TABLE `T` (\n\t"
                + "`id`                                    INT64 NOT NULL, "
                + "\n\t`c1`                                    BOOL,\n\t"
                + "`c2`                                    INT64,\n) "
                + "PRIMARY KEY ()\n\n"));
  }

  private void runTest(Schema avroSchema, String spannerSchema, Iterable<GenericRecord> records)
      throws Exception {
    runTest(avroSchema, spannerSchema, records, Dialect.GOOGLE_STANDARD_SQL);
  }

  private void runTest(
      Schema avroSchema, String spannerSchema, Iterable<GenericRecord> records, Dialect dialect)
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
                    .setOptionValue(dialect == Dialect.GOOGLE_STANDARD_SQL ? "\"4d\"" : "'4d'")
                    .build())
            .setDialect(ProtoDialect.valueOf(dialect.name()))
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
    switch (dialect) {
      case GOOGLE_STANDARD_SQL:
        spannerServer.createDatabase(dbName, Collections.singleton(spannerSchema));
        break;
      case POSTGRESQL:
        spannerServer.createPgDatabase(dbName, Collections.singleton(spannerSchema));
        break;
      default:
        throw new IllegalArgumentException("Unrecognized dialect: " + dialect);
    }

    // Run the import pipeline.
    importPipeline.apply(
        "Import",
        new ImportTransform(
            spannerServer.getSpannerConfig(dbName),
            ValueProvider.StaticValueProvider.of(manifestFileLocation),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(true),
            ValueProvider.StaticValueProvider.of(30)));
    PipelineResult importResult = importPipeline.run();
    importResult.waitUntilFinish();

    Ddl ddl;
    try (ReadOnlyTransaction ctx = spannerServer.getDbClient(dbName).readOnlyTransaction()) {
      ddl = new InformationSchemaScanner(ctx, dialect).scan();
    }
    assertThat(ddl.databaseOptions().size(), is(1));
    ExportProtos.Export.DatabaseOption dbOption = ddl.databaseOptions().get(0);
    assertThat(dbOption.getOptionName(), is("version_retention_period"));
    assertThat(dbOption.getOptionValue(), is("4d"));
  }

  private Schema wrapAsNullable(Schema avroType) {
    return SchemaBuilder.builder().unionOf().nullType().and().type(avroType).endUnion();
  }
}
