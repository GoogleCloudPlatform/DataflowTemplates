/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.spanner;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.spanner.TextImportProtos.ImportManifest.TableManifest;
import com.google.cloud.teleport.spanner.ddl.Ddl;
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
  private final ValueProvider<Boolean> trailingDelimiter = StaticValueProvider.of(true);
  private final ValueProvider<Character> escape = StaticValueProvider.of('\\');
  private final ValueProvider<String> nullString = StaticValueProvider.of("\\N");

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
                    "123,a string,`another string`,1.23,True,2019-01-01,2018-12-31T23:59:59Z,")));
    PCollection<Mutation> mutations =
        input.apply(
            ParDo.of(
                    new TextRowToMutation(
                        ddlView,
                        tableColumnsMapView,
                        columnDelimiter,
                        StaticValueProvider.of('`'),
                        trailingDelimiter,
                        escape,
                        nullString))
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
            "input",
            Create.of(KV.of(testTableName, "123|`strin with \"`|`\"yet another one\"+ \"'\"`")));
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
                        nullString))
                .withSideInputs(ddlView, tableColumnsMapView));

    PAssert.that(mutations)
        .containsInAnyOrder(
            Mutation.newInsertOrUpdateBuilder(testTableName)
                .set("int_col")
                .to(123)
                .set("str_10_col")
                .to("strin with \"")
                .set("str_max_col")
                .to("\"yet another one\"+ \"'\"")
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
        pipeline.apply("input", Create.of(KV.of(testTableName, "123||\\NA||||")));
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
                        StaticValueProvider.of("\\NA")))
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
                        nullString))
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
                        nullString))
                .withSideInputs(ddlView, tableColumnsMapView));

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
            .primaryKey()
            .asc("int_col")
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
