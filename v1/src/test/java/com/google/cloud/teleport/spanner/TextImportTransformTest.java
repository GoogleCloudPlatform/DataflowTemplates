/*
 * Copyright (C) 2019 Google LLC
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.TextImportTransform.ReadImportManifest;
import com.google.cloud.teleport.spanner.TextImportTransform.ReadTableColumns;
import com.google.cloud.teleport.spanner.TextImportTransform.ResolveDataFiles;
import com.google.cloud.teleport.spanner.common.Type.Code;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import com.google.cloud.teleport.spanner.proto.TextImportProtos.ImportManifest;
import com.google.cloud.teleport.spanner.proto.TextImportProtos.ImportManifest.TableManifest.Column;
import com.google.common.collect.Lists;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

/** Tests for TextImportTransform class. */
public class TextImportTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void readImportManifest() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    Path f12 = Files.createTempFile("table1-file", "2");
    Path f13 = Files.createTempFile("table1-file", "3");
    Path f21 = Files.createTempFile("table2-file", "1");
    Path f22 = Files.createTempFile("table2-file", "2");
    String tempDir = f11.getParent().toString();

    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table1\","
                  + "\"file_patterns\":[\"%s\",\"%s\"]},"
                  + "{\"table_name\": \"table2\","
                  + "\"file_patterns\":[\"%s\"]}"
                  + "]}",
              f11.toString(), f12.toString(), f21.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    // Validates that only the file patterns specified in manifest will be returned.
    // E.g., f13 and f22 are not in the tableAndFiles result.
    PAssert.that(tableAndFiles)
        .containsInAnyOrder(
            KV.of("table1", f11.toString()),
            KV.of("table1", f12.toString()),
            KV.of("table2", f21.toString()));

    pipeline.run();
  }

  @Test
  public void readImportManifestUtfWithBOM() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    String tempDir = f11.getParent().toString();

    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "\uFEFF{\"tables\": ["
                  + "{\"table_name\": \"table1\","
                  + "\"file_patterns\":[\"%s\"]}"
                  + "]}",
              f11.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    PAssert.that(tableAndFiles).containsInAnyOrder(KV.of("table1", f11.toString()));

    pipeline.run();
  }

  @Test
  public void readImportManifestPartialMatching() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    Path f12 = Files.createTempFile("table1-file", "2");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("US-ASCII");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table1\","
                  + "\"file_patterns\":[\"%s\",\"%s\"]},"
                  + "{\"table_name\": \"table2\","
                  + "\"file_patterns\":[\"NOT_FOUND_FILE_PATTERN_\"]}"
                  + "]}",
              f11.toString(), f12.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollectionView<Dialect> dialectView =
        pipeline
            .apply("Dialect", Create.of(Dialect.GOOGLE_STANDARD_SQL))
            .apply("Dialect As PCollectionView", View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest, dialectView))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    PAssert.that(tableAndFiles)
        .containsInAnyOrder(KV.of("table1", f11.toString()), KV.of("table1", f12.toString()));
    try {
      pipeline.run();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test(expected = PipelineExecutionException.class)
  public void readImportManifestInvalidManifestFormat() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    Path f12 = Files.createTempFile("table1-file", "2");
    Path f13 = Files.createTempFile("table1-file", "3");
    String tempDir = f11.getParent().toString();

    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      // An invalid json string (missing the ending close "}").
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table1\","
                  + "\"file_patterns\":[\"%s\",\"%s\"]},"
                  + "{\"table_name\": \"table2\","
                  + "\"file_patterns\":[\"*\"]}"
                  + "]",
              f11.toString(), f12.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    pipeline.run();
  }

  @Test
  public void readImportManifestInvalidTable() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");

    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"NON_EXIST_TABLE\","
                  + "\"file_patterns\":[\"%s\"]}"
                  + "]}",
              f11.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    try {
      pipeline.run();
    } catch (PipelineExecutionException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              "java.lang.RuntimeException: Table NON_EXIST_TABLE not found in the database. "
                  + "Table must be pre-created in database"));
    }
  }

  @Test
  public void readImportManifestGeneratedColumn() throws Exception {
    Path f31 = Files.createTempFile("table3-file", "1");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table3\","
                  + "\"file_patterns\": [\"%s\"],"
                  + "\"columns\": [{\"column_name\": \"int_col\", \"type_name\": \"INT64\"}]}"
                  + "]}",
              f31.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    pipeline.run();
  }

  @Test
  public void readImportManifestColumnListMustBeProvidedForGeneratedColumn() throws Exception {
    Path f31 = Files.createTempFile("table3-file", "1");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table3\","
                  + "\"file_patterns\": [\"%s\"]}"
                  + "]}",
              f31.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));
    try {
      pipeline.run();
    } catch (PipelineExecutionException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              "java.lang.RuntimeException: DB table table3 has one or more generated columns. An"
                  + " explicit column list that excludes the generated columns must be provided in"
                  + " the manifest."));
    }
  }

  @Test
  public void readImportManifestGeneratedColumnCannotBeImported() throws Exception {
    Path f31 = Files.createTempFile("table3-file", "1");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table3\","
                  + "\"file_patterns\": [\"%s\"],"
                  + "\"columns\": [{\"column_name\": \"gen_col\", \"type_name\": \"INT64\"}]}"
                  + "]}",
              f31.toString());
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));
    try {
      pipeline.run();
    } catch (PipelineExecutionException e) {
      assertThat(
          e.getMessage(),
          equalTo(
              "java.lang.RuntimeException: Column gen_col in manifest is a generated column "
                  + "in DB table table3. Generated columns cannot be imported."));
    }
  }

  @Test
  public void readPgImportManifestTypeMustMatch() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": [{\"table_name\": \"table1\",\"file_patterns\": [\"%s\"],\"columns\":"
                  + " [{\"column_name\": \"int_col\", \"type_name\": \"bigint\"},"
                  + " {\"column_name\":\"str_10_col\", \"type_name\": \"character varying(10)\"},"
                  + " {\"column_name\":\"float_col\", \"type_name\": \"double precision\"},"
                  + " {\"column_name\":\"bool_col\", \"type_name\": \"boolean\"}, {\"column_name\":"
                  + " \"byte_col\", \"type_name\": \"bytea\"}, {\"column_name\": \"timestamp_col\","
                  + " \"type_name\":\"timestamp with time zone\"}, {\"column_name\":"
                  + " \"numeric_col\", \"type_name\": \"numeric\"}, {\"column_name\": \"date_col\","
                  + " \"type_name\": \"date\"}, {\"column_name\": \"commit_timestamp_col\","
                  + " \"type_name\": \"spanner.commit_timestamp\"}]}]}",
              f11.toString());
      writer.write(jsonString, 0, jsonString.length());
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getPgTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));
    pipeline.run();
  }

  @Test
  public void readPgImportManifestTypeMismatch() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(manifestFile, charset)) {
      String jsonString =
          String.format(
              "{\"tables\": ["
                  + "{\"table_name\": \"table1\","
                  + "\"file_patterns\": [\"%s\"],"
                  + "\"columns\": [{\"column_name\": \"int_col\", \"type_name\": \"text\"}]}"
                  + "]}",
              f11.toString());
      writer.write(jsonString, 0, jsonString.length());
    }

    ValueProvider<String> importManifest =
        ValueProvider.StaticValueProvider.of(manifestFile.toString());
    PCollectionView<Ddl> ddlView =
        pipeline.apply("ddl", Create.of(getPgTestDdl())).apply(View.asSingleton());

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    PipelineExecutionException thrown =
        assertThrows(PipelineExecutionException.class, () -> pipeline.run());
    assertThat(
        thrown.getMessage(),
        equalTo(
            "java.lang.RuntimeException: Mismatching type: Table table1 Column int_col [PG_INT8"
                + " from DB and text from manifest]"));
  }

  private static Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("table1")
            .column("int_col")
            .int64()
            .notNull()
            .endColumn()
            .column("str_10_col")
            .string()
            .size(10)
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
            .column("byte_col")
            .bytes()
            .endColumn()
            .column("timestamp_col")
            .timestamp()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .endTable()
            .createTable("table2")
            .column("int_col")
            .int64()
            .notNull()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .endTable()
            .createTable("table3")
            .column("int_col")
            .int64()
            .notNull()
            .endColumn()
            .column("gen_col")
            .int64()
            .generatedAs("(int_col)")
            .stored()
            .endColumn()
            .primaryKey()
            .asc("int_col")
            .end()
            .endTable()
            .createView("view1")
            .query("SELECT int_col FROM table1")
            .security(com.google.cloud.teleport.spanner.ddl.View.SqlSecurity.INVOKER)
            .endView()
            .createView("view2")
            .query("SELECT int_col FROM table2")
            .security(com.google.cloud.teleport.spanner.ddl.View.SqlSecurity.DEFINER)
            .endView()
            .build();
    return ddl;
  }

  private static Ddl getPgTestDdl() {
    Ddl ddl =
        Ddl.builder(Dialect.POSTGRESQL)
            .createTable("table1")
            .column("int_col")
            .pgInt8()
            .notNull()
            .endColumn()
            .column("str_10_col")
            .pgVarchar()
            .size(10)
            .endColumn()
            .column("float_col")
            .pgFloat8()
            .endColumn()
            .column("bool_col")
            .pgBool()
            .endColumn()
            .column("byte_col")
            .pgBytea()
            .endColumn()
            .column("timestamp_col")
            .pgTimestamptz()
            .endColumn()
            .column("numeric_col")
            .pgNumeric()
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
            .createView("view")
            .query("SELECT int_col FROM table1")
            .security(com.google.cloud.teleport.spanner.ddl.View.SqlSecurity.DEFINER)
            .endView()
            .build();
    return ddl;
  }

  @Test
  public void testParseSpannerDataType() {
    assertEquals(
        Code.STRING,
        ResolveDataFiles.parseSpannerDataType("STRING(MAX)", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.INT64, ResolveDataFiles.parseSpannerDataType("INT64", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.FLOAT64,
        ResolveDataFiles.parseSpannerDataType("FLOAT64", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.BOOL, ResolveDataFiles.parseSpannerDataType("BOOL", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.DATE, ResolveDataFiles.parseSpannerDataType("DATE", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.TIMESTAMP,
        ResolveDataFiles.parseSpannerDataType("TIMESTAMP", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.BYTES, ResolveDataFiles.parseSpannerDataType("BYTES", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.NUMERIC,
        ResolveDataFiles.parseSpannerDataType("NUMERIC", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.JSON, ResolveDataFiles.parseSpannerDataType("JSON", Dialect.GOOGLE_STANDARD_SQL));
    assertEquals(
        Code.PG_SPANNER_COMMIT_TIMESTAMP,
        ResolveDataFiles.parseSpannerDataType("spanner.commit_timestamp", Dialect.POSTGRESQL));
    assertThrows(
        IllegalArgumentException.class,
        () -> ResolveDataFiles.parseSpannerDataType("unknown", Dialect.GOOGLE_STANDARD_SQL));
  }

  @Test
  public void testReadTableColumns() throws Exception {
    ImportManifest importManifest1 =
        ImportManifest.newBuilder()
            .addTables(
                ImportManifest.TableManifest.newBuilder()
                    .setTableName("table1")
                    .addColumns(Column.newBuilder().setColumnName("col1").build())
                    .build())
            .build();

    PCollection<ImportManifest> input =
        pipeline.apply("ImportManifest", Create.of(importManifest1));
    PCollection<Map<String, List<Column>>> manifestColumns =
        input.apply("ReadTableColumns", new ReadTableColumns());

    PAssert.that(manifestColumns)
        .satisfies(
            input1 -> {
              LinkedList<Map<String, List<Column>>> manifestCols = Lists.newLinkedList(input1);
              assertEquals(1, manifestCols.size());
              for (Map<String, List<Column>> manifest : manifestCols) {
                assertEquals(1, manifest.get("table1").size());
                assertEquals("col1", manifest.get("table1").get(0).getColumnName());
              }
              return null;
            });
    pipeline.run();
  }
}
