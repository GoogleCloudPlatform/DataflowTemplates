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

import com.google.cloud.teleport.spanner.ExportProtos.TableManifest;
import com.google.cloud.teleport.spanner.ImportTransform.ValidateInputFiles;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for ImportTransform class. */
public class ImportTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void validateInputFiles() throws Exception {
    Path f1 = Files.createTempFile("table1-file", "1");
    Path f2 = Files.createTempFile("table1-file", "2");
    Path f3 = Files.createTempFile("table2-file", "1");

    // Create the expected manifest string.
    TableManifest.Builder builder = TableManifest.newBuilder();
    builder
        .addFilesBuilder()
        .setName(f1.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    builder
        .addFilesBuilder()
        .setName(f2.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    TableManifest manifest1 = builder.build();

    builder = TableManifest.newBuilder();
    builder
        .addFilesBuilder()
        .setName(f3.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    TableManifest manifest2 = builder.build();

    final Map<String, TableManifest> tablesAndManifests =
        ImmutableMap.of(
            "table1", manifest1,
            "table2", manifest2);
    ValueProvider<String> importDirectory =
        ValueProvider.StaticValueProvider.of(f1.getParent().toString());

    // Execute the transform.
    PCollection<KV<String, String>> tableAndFiles =
        pipeline
            .apply("Create", Create.of(tablesAndManifests))
            .apply(ParDo.of(new ValidateInputFiles(importDirectory)));

    PAssert.that(tableAndFiles)
        .containsInAnyOrder(
            KV.of("table1", f1.toString()),
            KV.of("table1", f2.toString()),
            KV.of("table2", f3.toString()));

    pipeline.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void validateInvalidInputFiles() throws Exception {
    Path f1 = Files.createTempFile("table1-file", "1");
    TableManifest.Builder builder = TableManifest.newBuilder();
    builder.addFilesBuilder().setName(f1.getFileName().toString()).setMd5("invalid checksum");
    TableManifest manifest1 = builder.build();
    ValueProvider<String> importDirectory =
        ValueProvider.StaticValueProvider.of(f1.getParent().toString());
    // Execute the transform.
    pipeline
        .apply("Create", Create.of(ImmutableMap.of("table1", manifest1)))
        .apply(ParDo.of(new ValidateInputFiles(importDirectory)));
    pipeline.run();
    // Pipeline should fail with an exception.
  }
}
