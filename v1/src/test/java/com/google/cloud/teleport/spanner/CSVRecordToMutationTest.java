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

import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.proto.TextImportProtos.ImportManifest.TableManifest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;

/** Tests for CSVRecordToMutation class. */
public final class CSVRecordToMutationTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static String testTableName = "TestTable";

  private final ValueProvider<Character> columnDelimiter = StaticValueProvider.of(',');
  private final ValueProvider<Character> fieldQualifier = StaticValueProvider.of('"');
  private final ValueProvider<Boolean> trailingDelimiter = StaticValueProvider.of(false);
  private final ValueProvider<Character> escape = StaticValueProvider.of(null);
  private final ValueProvider<String> nullString = StaticValueProvider.of(null);
  private final ValueProvider<String> dateFormat = StaticValueProvider.of(null);
  private final ValueProvider<String> timestampFormat = StaticValueProvider.of(null);
  private final ValueProvider<String> invalidOutputPath = StaticValueProvider.of(null);
  private final TupleTag<String> errorTag = new TupleTag<>();

  private final CSVFormat csvFormat =
      CSVFormat.newFormat(columnDelimiter.get())
          .withQuote(fieldQualifier.get())
          .withIgnoreEmptyLines(true)
          .withTrailingDelimiter(trailingDelimiter.get())
          .withEscape(escape.get())
          .withNullString(nullString.get());

  @Test
  public void parseRowToMutation() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());

    CSVRecord csvRecord =
        CSVParser.parse(
                "123,a string,`another"
                    + " string`,1.23,True,2019-01-01,2018-12-31T23:59:59Z,1567637083,aGk=,"
                    + "-439.25335679,`{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}`",
                csvFormat.withQuote('`').withTrailingDelimiter(true))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(123)
                .set("str_10_col")
                .to("a string")
                .set("str_max_col")
                .to("another string")
                .set("float_col")
                .to(1.23)
                .set("bool_col")
                .to(true)
                .set("date_col")
                .to(Value.date(Date.parseDate("2019-01-01")))
                .set("timestamp_col")
                .to(Value.timestamp(Timestamp.parseTimestamp("2018-12-31T23:59:59Z")))
                .set("timestamp_col_epoch")
                .to(Value.timestamp(Timestamp.ofTimeMicroseconds(1567637083)))
                .set("byte_col")
                .to(Value.bytes(ByteArray.fromBase64("aGk=")))
                .set("numeric_col")
                .to("-439.25335679")
                .set("json_col")
                .to("{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}")
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowToMutationNewlineInData() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());

    CSVRecord csvRecord =
        CSVParser.parse(
                "123,a string,`another\n"
                    + "string`,1.23,True,2019-01-01,2018-12-31T23:59:59Z,1567637083,aGk=,"
                    + "-439.25335679,`{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}`",
                csvFormat.withQuote('`').withTrailingDelimiter(true))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(123)
                .set("str_10_col")
                .to("a string")
                .set("str_max_col")
                .to("another\nstring")
                .set("float_col")
                .to(1.23)
                .set("bool_col")
                .to(true)
                .set("date_col")
                .to(Value.date(Date.parseDate("2019-01-01")))
                .set("timestamp_col")
                .to(Value.timestamp(Timestamp.parseTimestamp("2018-12-31T23:59:59Z")))
                .set("timestamp_col_epoch")
                .to(Value.timestamp(Timestamp.ofTimeMicroseconds(1567637083)))
                .set("byte_col")
                .to(Value.bytes(ByteArray.fromBase64("aGk=")))
                .set("numeric_col")
                .to("-439.25335679")
                .set("json_col")
                .to("{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}")
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowToMutationException2() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse(
                "123,a string,`another"
                    + " string`,1.23,NotBool,2019-01-01,2018-12-31T23:59:59Z,1567637083,aGk=,"
                    + "-439.25335679,`{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}`",
                csvFormat.withQuote('`').withTrailingDelimiter(true))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));
    // CSVRecordToMutation throws exception if it could not parse the data to corresponding type.
    // `NotBool` cannot be parsed to Boolean in this test case.
    assertThrows(PipelineExecutionException.class, () -> pipeline.run());
  }

  @Test
  public void parseRowToMutationDefaultDatetimeExtraParts() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse(
                "1,str,\"str with, commas,\",1.1,False,1910-01-01"
                    + " 00:00:00,2018-12-31T23:59:59.123Z,1567637083000,aGk=",
                csvFormat.withTrailingDelimiter(true))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(1)
                .set("str_10_col")
                .to("str")
                .set("str_max_col")
                .to("str with, commas,")
                .set("float_col")
                .to(1.1)
                .set("bool_col")
                .to(false)
                .set("date_col")
                .to(Value.date(Date.parseDate("1910-01-01")))
                .set("timestamp_col")
                .to(Value.timestamp(Timestamp.parseTimestamp("2018-12-31T23:59:59.123Z")))
                .set("timestamp_col_epoch")
                .to(Value.timestamp(Timestamp.ofTimeMicroseconds(1567637083000L)))
                .set("byte_col")
                .to(Value.bytes(ByteArray.fromBase64("aGk=")))
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowToMutationCustomizedDelimiterAndFieldQualifier() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());

    CSVRecord csvRecord =
        CSVParser.parse(
                "123|`str1 with |`|`\"str2\"+ \"'\"|`", csvFormat.withDelimiter('|').withQuote('`'))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(123)
                .set("str_10_col")
                .to("str1 with |")
                .set("str_max_col")
                .to("\"str2\"+ \"'\"|")
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowToMutationCustomizedDateFormat() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdlDateOnly())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord = CSVParser.parse("1/21/1998", csvFormat).getRecords().get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        StaticValueProvider.of("M/d/yyyy"),
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("date_col")
                .to(Value.date(Date.parseDate("1998-01-21")))
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowToMutationCustomizedTimestampFormat() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdlTimestampOnly())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse("Jan 21 1998 01:02:03.456+08:00", csvFormat).getRecords().get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        StaticValueProvider.of("MMM d yyyy HH:mm:ss.SSSVV"),
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("timestamp_col")
                .to(Value.timestamp(Timestamp.parseTimestamp("1998-01-20T17:02:03.456Z")))
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowToMutationCustomizedEmptyAndNullFields() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse(
                "123||\\NA||||||",
                csvFormat
                    .withDelimiter('|')
                    .withTrailingDelimiter(false)
                    .withEscape('`')
                    .withNullString("\\NA"))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(123)
                .set("str_10_col")
                .to("")
                .set("str_max_col")
                .to(Value.string(null))
                .set("float_col")
                .to(Value.float64(null))
                .set("bool_col")
                .to(Value.bool(null))
                .set("date_col")
                .to(Value.date(null))
                .set("timestamp_col")
                .to(Value.timestamp(null))
                .set("timestamp_col_epoch")
                .to(Value.timestamp(null))
                .set("byte_col")
                .to(Value.bytes(null))
                .build());

    pipeline.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void parseRowToMutationInvalidFormat() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse("123,a string,yet another string,1.23,True,99999/99/99", csvFormat)
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    pipeline.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void parseRowToMutationTooManyColumns() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse("123,a string,yet another string,1.23,True,,,,,,,", csvFormat)
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    pipeline.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void parseRowWithArrayColumn() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdlWithArray())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse("str, [a string in an array]", csvFormat).getRecords().get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));
    pipeline.run();
  }

  @Test
  public void parseRowWithPgArrayColumn() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestPgDdlWithArray())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord =
        CSVParser.parse("str, [a string in an array]", csvFormat).getRecords().get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));
    assertThrows(PipelineExecutionException.class, () -> pipeline.run());
  }

  @Test
  public void pgParseRowToMutation() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getPgTestDdl())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());

    CSVRecord csvRecord =
        CSVParser.parse(
                "123,a string,'another string',1.23,True,2018-12-31T23:59:59Z,1567637083"
                    + ",aGk=,-439.25335679,'{\"a\": null, \"b\": [true, false, 14.234"
                    + ", \"dsafaaf\"]}',1910-01-01,2017-10-28T12:59:59Z",
                csvFormat.withQuote('\'').withTrailingDelimiter(true))
            .getRecords()
            .get(0);
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));

    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(123)
                .set("str_10_col")
                .to("a string")
                .set("str_max_col")
                .to("another string")
                .set("float_col")
                .to(1.23)
                .set("bool_col")
                .to(true)
                .set("timestamp_col")
                .to(Value.timestamp(Timestamp.parseTimestamp("2018-12-31T23:59:59Z")))
                .set("timestamp_col_epoch")
                .to(Value.timestamp(Timestamp.ofTimeMicroseconds(1567637083)))
                .set("byte_col")
                .to(Value.bytes(ByteArray.fromBase64("aGk=")))
                .set("numeric_col")
                .to(Value.pgNumeric("-439.25335679"))
                .set("jsonb_col")
                .to("{\"a\": null, \"b\": [true, false, 14.234, \"dsafaaf\"]}")
                .set("date_col")
                .to(Value.date(Date.parseDate("1910-01-01")))
                .set("commit_timestamp_col")
                .to(Value.timestamp(Timestamp.parseTimestamp("2017-10-28T12:59:59Z")))
                .build());

    pipeline.run();
  }

  @Test
  public void parseRowWithDefaultColumnOmitted() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdlWithDefaultValue())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());

    CSVRecord csvRecord = CSVParser.parse("10,20", csvFormat).getRecords().get(0);
    // Omit the value of int_col3.
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    // Verify that int_col3 doesn't appear in the mutation column list.
    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col1")
                .to(10)
                .set("int_col2")
                .to(20)
                .build());
    pipeline.run();
  }

  @Test
  public void pgParseRowWithDefaultColumnOmitted() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getPgTestDdlWithDefaultValue())).apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord = CSVParser.parse("10,20", csvFormat).getRecords().get(0);
    // Omit the value of int_col3.
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    // Verify that int_col3 doesn't appear in the mutation column list.
    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col1")
                .to(10)
                .set("int_col2")
                .to(20)
                .build());
    pipeline.run();
  }

  @Test
  public void parseRowWithGeneratedPrimaryKey() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline
            .apply("ddl", Create.of(getTestDdlWithGeneratedPrimaryKey()))
            .apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());

    CSVRecord csvRecord = CSVParser.parse("10,20", csvFormat).getRecords().get(0);
    // Omit the value of gen_col.
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    // Verify that gen_col doesn't appear in the mutation column list.
    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(10)
                .set("val_col")
                .to(20)
                .build());
    pipeline.run();
  }

  @Test
  public void pgParseRowWithGeneratedPrimaryKey() throws Exception {
    PCollectionView<Ddl> ddlView =
        pipeline
            .apply("ddl", Create.of(getPgTestDdlWithGeneratedPrimaryKey()))
            .apply(View.asSingleton());
    PCollectionView<Map<String, List<TableManifest.Column>>> tableColumnsMapView =
        pipeline
            .apply(
                "tableColumnsMap",
                Create.<Map<String, List<TableManifest.Column>>>of(getEmptyTableColumnsMap())
                    .withCoder(
                        MapCoder.of(
                            StringUtf8Coder.of(),
                            ListCoder.of(ProtoCoder.of(TableManifest.Column.class)))))
            .apply("Map as view", View.asSingleton());
    CSVRecord csvRecord = CSVParser.parse("10,20", csvFormat).getRecords().get(0);
    // Omit the value of gen_col.
    PCollection<KV<String, CSVRecord>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, csvRecord))
                .withCoder(
                    KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CSVRecord.class))));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new CSVRecordToMutation(
                        ddlView,
                        tableColumnsMapView,
                        dateFormat,
                        timestampFormat,
                        invalidOutputPath,
                        errorTag))
                .withSideInputs(ddlView, tableColumnsMapView));

    // Verify that gen_col doesn't appear in the mutation column list.
    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(10)
                .set("val_col")
                .to(20)
                .build());
    pipeline.run();
  }

  private static Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable(testTableName)
            .column("int_col")
            .int64()
            .notNull()
            .endColumn()
            .column("str_10_col")
            .string()
            .size(10)
            .endColumn()
            .column("str_max_col")
            .type(Type.string())
            .max()
            .endColumn()
            .column("float_col")
            .float64()
            .endColumn()
            .column("bool_col")
            .bool()
            .endColumn()
            .column("date_col")
            .date()
            .endColumn()
            .column("timestamp_col")
            .timestamp()
            .endColumn()
            .column("timestamp_col_epoch")
            .timestamp()
            .endColumn()
            .column("byte_col")
            .bytes()
            .endColumn()
            .column("numeric_col")
            .numeric()
            .endColumn()
            .column("json_col")
            .json()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getPgTestDdl() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable(testTableName)
            .column("int_col")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("str_10_col")
            .pgVarchar()
            .size(10)
            .endColumn()
            .column("str_max_col")
            .type(Type.pgVarchar())
            .max()
            .endColumn()
            .column("float_col")
            .pgFloat8()
            .endColumn()
            .column("bool_col")
            .pgBool()
            .endColumn()
            .column("timestamp_col")
            .pgTimestamptz()
            .endColumn()
            .column("timestamp_col_epoch")
            .pgTimestamptz()
            .endColumn()
            .column("byte_col")
            .pgBytea()
            .endColumn()
            .column("numeric_col")
            .pgNumeric()
            .endColumn()
            .column("jsonb_col")
            .pgJsonb()
            .endColumn()
            .column("date_col")
            .pgDate()
            .endColumn()
            .column("commit_timestamp_col")
            .pgSpannerCommitTimestamp()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getTestDdlDateOnly() {
    Ddl ddl =
        Ddl.builder()
            .createTable(testTableName)
            .column("date_col")
            .date()
            .endColumn()
            .primaryKey()
            .asc("date_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getTestDdlTimestampOnly() {
    Ddl ddl =
        Ddl.builder()
            .createTable(testTableName)
            .column("timestamp_col")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("timestamp_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getTestDdlWithArray() {
    Ddl ddl =
        Ddl.builder()
            .createTable(testTableName)
            .column("str_col")
            .string()
            .max()
            .notNull()
            .endColumn()
            .column("arr_str_col")
            .type(Type.array(Type.string()))
            .max()
            .endColumn()
            .primaryKey()
            .asc("str_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getTestPgDdlWithArray() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable(testTableName)
            .column("str_col")
            .pgVarchar()
            .max()
            .notNull()
            .endColumn()
            .column("arr_str_col")
            .type(Type.pgArray(Type.pgVarchar()))
            .max()
            .endColumn()
            .primaryKey()
            .asc("str_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getTestDdlWithDefaultValue() {
    Ddl ddl =
        Ddl.builder()
            .createTable(testTableName)
            .column("int_col1")
            .int64()
            .notNull()
            .endColumn()
            .column("int_col2")
            .int64()
            .defaultExpression("2")
            .endColumn()
            .column("int_col3")
            .int64()
            .defaultExpression("3")
            .endColumn()
            .primaryKey()
            .asc("int_col1")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getPgTestDdlWithDefaultValue() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable(testTableName)
            .column("int_col1")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("int_col2")
            .pgInt8()
            .defaultExpression("2")
            .endColumn()
            .column("int_col3")
            .pgInt8()
            .defaultExpression("3")
            .endColumn()
            .primaryKey()
            .asc("int_col1")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getTestDdlWithGeneratedPrimaryKey() {
    Ddl ddl =
        Ddl.builder()
            .createTable(testTableName)
            .column("int_col")
            .int64()
            .notNull()
            .endColumn()
            .column("val_col")
            .int64()
            .endColumn()
            .column("gen_col")
            .int64()
            .generatedAs("MOD(int_col+1, 64)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .asc("gen_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Ddl getPgTestDdlWithGeneratedPrimaryKey() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable(testTableName)
            .column("int_col")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("val_col")
            .pgInt8()
            .endColumn()
            .column("gen_col")
            .pgInt8()
            .generatedAs("MOD(int_col+1, 64)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .asc("gen_col")
            .end()
            .endTable()
            .build();
    return ddl;
  }

  private static Map<String, List<TableManifest.Column>> getEmptyTableColumnsMap() {
    List<TableManifest.Column> columns = new ArrayList<>();
    HashMap<String, List<TableManifest.Column>> tableColumnsMap = new HashMap<>();
    tableColumnsMap.put(testTableName, columns);
    return tableColumnsMap;
  }
}
