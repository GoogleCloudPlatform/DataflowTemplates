/*
 * Copyright (C) 2019 Google Inc.
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

import com.google.cloud.teleport.spanner.TextImportTransform.ReadImportManifest;
import com.google.cloud.teleport.spanner.TextImportTransform.ResolveDataFiles;
import com.google.cloud.teleport.spanner.ddl.Ddl;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
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
  public void readImportManifestPartialMatching() throws Exception {
    Path f11 = Files.createTempFile("table1-file", "1");
    Path f12 = Files.createTempFile("table1-file", "2");
    Path manifestFile = Files.createTempFile("import-manifest", ".json");
    Charset charset = Charset.forName("UTF-8");
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

    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Read manifest file", new ReadImportManifest(importManifest))
            .apply("Resolve data files", new ResolveDataFiles(importManifest, ddlView));

    PAssert.that(tableAndFiles)
        .containsInAnyOrder(KV.of("table1", f11.toString()), KV.of("table1", f12.toString()));
    try {
      pipeline.run();
    } catch (Exception e) {
      System.out.print("  e........" + e.getMessage());
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

  private static Ddl getTestDdl() {
    Ddl ddl =
        Ddl.builder()
            .createTable("testTableName")
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
}
