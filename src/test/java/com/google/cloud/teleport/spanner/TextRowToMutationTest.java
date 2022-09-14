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
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
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
import org.junit.Rule;
import org.junit.Test;

/** Tests for TextRowToMutation class. */
public final class TextRowToMutationTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static String testTableName = "TestTable";

  private final ValueProvider<Character> columnDelimiter = StaticValueProvider.of(',');
  private final ValueProvider<Character> fieldQualifier = StaticValueProvider.of('"');
  private final ValueProvider<Boolean> trailingDelimiter = StaticValueProvider.of(false);
  private final ValueProvider<Character> escape = StaticValueProvider.of(null);
  private final ValueProvider<String> nullString = StaticValueProvider.of(null);
  private final ValueProvider<String> dateFormat = StaticValueProvider.of(null);
  private final ValueProvider<String> timestampFormat = StaticValueProvider.of(null);

  @Test
  public void parseRowToMutation() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input",
            Create.of(
                KV.of(
                    testTableName,
                    "123,a string,`another"
                        + " string`,1.23,True,2019-01-01,2018-12-31T23:59:59Z,1567637083,aGk=,"
                        + "-439.25335679,`{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}`")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('`'),
                        StaticValueProvider.of(true),
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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
  public void parseRowToMutationException1() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input", Create.of(KV.of(testTableName, "123,a string,another\n456,two,four")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('`'),
                        StaticValueProvider.of(true),
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
                .withSideInputs(ddlView, tableColumnsMapView));
    // TextRowToMutation throws exception if the input string contains multiple lines.
    assertThrows(PipelineExecutionException.class, () -> pipeline.run());
  }

  @Test
  public void parseRowToMutationEmptyLine() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply("input", Create.of(KV.of(testTableName, "")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('`'),
                        StaticValueProvider.of(true),
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations).empty();

    pipeline.run();
  }

  @Test
  public void parseRowToMutationException2() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input",
            Create.of(
                KV.of(
                    testTableName,
                    "123,a string,`another"
                        + " string`,1.23,NotBool,2019-01-01,2018-12-31T23:59:59Z,1567637083,aGk=,"
                        + "-439.25335679,`{\"a\":[1,null,true],\"b\":{\"a\":\"\\\"hello\\\"\"}}`")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('`'),
                        StaticValueProvider.of(true),
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
                .withSideInputs(ddlView, tableColumnsMapView));
    // TextRowToMutation throws exception if it could not parse the data to corresponding type.
    // `NotBool` cannot be parsed to Boolean in this test case.
    assertThrows(PipelineExecutionException.class, () -> pipeline.run());
  }

  @Test
  public void parseRowToMutationDefaultDatetimeExtraParts() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input",
            Create.of(
                KV.of(
                    testTableName,
                    "1,str,\"str with, commas,\",1.1,False,1910-01-01"
                        + " 00:00:00,2018-12-31T23:59:59.123Z,1567637083000,aGk=")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        fieldQualifier,
                        StaticValueProvider.of(true),
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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
  public void parseRowToMutationCustomizedDimiterAndFieldQulifier() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input", Create.of(KV.of(testTableName, "123|`str1 with |`|`\"str2\"+ \"'\"|`")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        StaticValueProvider.of('|'),
                        StaticValueProvider.of('`'),
                        trailingDelimiter,
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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
  public void parseRowToMutationCustomizedDateFormat() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply("input", Create.of(KV.of(testTableName, "1/21/1998")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        fieldQualifier,
                        trailingDelimiter,
                        escape,
                        nullString,
                        StaticValueProvider.of("M/d/yyyy"),
                        timestampFormat))
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
  public void parseRowToMutationCustomizedTimestampFormat() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply("input", Create.of(KV.of(testTableName, "Jan 21 1998 01:02:03.456+08:00")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        fieldQualifier,
                        trailingDelimiter,
                        escape,
                        nullString,
                        dateFormat,
                        StaticValueProvider.of("MMM d yyyy HH:mm:ss.SSSVV")))
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
  public void parseRowToMutationCustomizedEmptyAndNullFields() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply("input", Create.of(KV.of(testTableName, "123||\\NA||||||")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        StaticValueProvider.of('|'),
                        fieldQualifier,
                        StaticValueProvider.of(false),
                        StaticValueProvider.of('`'),
                        StaticValueProvider.of("\\NA"),
                        dateFormat,
                        timestampFormat))
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input",
            Create.of(
                KV.of(testTableName, "123,a string,yet another string,1.23,True,99999/99/99")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        fieldQualifier,
                        trailingDelimiter,
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input",
            Create.of(KV.of(testTableName, "123,a string,yet another string,1.23,True,,,,,,,")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('"'),
                        trailingDelimiter,
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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

    PCollection<KV<String, String>> input =
        pipeline.apply("input", Create.of(KV.of(testTableName, "str, [a string in an array]")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('"'),
                        trailingDelimiter,
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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

    PCollection<KV<String, String>> input =
        pipeline.apply("input", Create.of(KV.of(testTableName, "str, [a string in an array]")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('"'),
                        trailingDelimiter,
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
                .withSideInputs(ddlView, tableColumnsMapView));
    assertThrows(PipelineExecutionException.class, () -> pipeline.run());
  }

  @Test
  public void pgParseRowToMutation() {
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

    PCollection<KV<String, String>> input =
        pipeline.apply(
            "input",
            Create.of(
                KV.of(
                    testTableName,
                    "123,a string,'another string',1.23,True,2018-12-31T23:59:59Z,1567637083"
                        + ",aGk=,-439.25335679,'{\"a\": null, \"b\": [true, false, 14.234"
                        + ", \"dsafaaf\"]}',1910-01-01")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('\''),
                        StaticValueProvider.of(true),
                        escape,
                        nullString,
                        dateFormat,
                        timestampFormat))
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

  private static Map<String, List<TableManifest.Column>> getEmptyTableColumnsMap() {
    List<TableManifest.Column> columns = new ArrayList<>();
    HashMap<String, List<TableManifest.Column>> tableColumnsMap = new HashMap<>();
    tableColumnsMap.put(testTableName, columns);
    return tableColumnsMap;
  }
}
