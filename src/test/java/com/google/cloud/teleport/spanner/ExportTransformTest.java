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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.spanner.ExportProtos.Export;
import com.google.cloud.teleport.spanner.ExportProtos.Export.Builder;
import com.google.cloud.teleport.spanner.ExportProtos.TableManifest;
import com.google.cloud.teleport.spanner.ExportTransform.BuildTableManifests;
import com.google.cloud.teleport.spanner.ExportTransform.CreateDatabaseManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/** Tests for ExportTransform. */
public class ExportTransformTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void buildTableManifests() throws  Exception {
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
    String manifest1 = JsonFormat.printer().print(builder.build());

    builder = TableManifest.newBuilder();
    builder
        .addFilesBuilder()
        .setName(f3.getFileName().toString())
        .setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    String manifest2 = JsonFormat.printer().print(builder.build());

    final Map<String, Iterable<String>> tablesAndFiles =
        ImmutableMap.of(
            "table1", ImmutableList.of(f1.toString(), f2.toString()),
            "table2", ImmutableList.of(f3.toString()));

    // Execute the transform.
    PCollection<KV<String, String>> tableManifests =
        pipeline
            .apply("Create", Create.of(tablesAndFiles))
            .apply(ParDo.of(new BuildTableManifests()));

    PAssert.that(tableManifests)
        .containsInAnyOrder(KV.of("table1", manifest1), KV.of("table2", manifest2));
    pipeline.run();
  }

  @Test
  public void buildDatabaseManifestFile() throws InvalidProtocolBufferException {
    Map<String, String> tablesAndManifests =
        ImmutableMap.of("table1", "table1 manifest", "table2", "table2 manifest");

    Export.Builder builder = Export.newBuilder();
    builder.addTablesBuilder().setName("table1").setManifestFile("table1-manifest.json");
    builder.addTablesBuilder().setName("table2").setManifestFile("table2-manifest.json");
    String expectedManifest = JsonFormat.printer().print(builder.build());

    PCollection<String> databaseManifest =
        pipeline
            .apply(Create.of(tablesAndManifests))
            .apply(Combine.globally(new CreateDatabaseManifest()));

    // The output JSON may contain the tables in any order, so a string comparison is not
    // sufficient. Have to convert the manifest string to a protobuf. Also for the checker function
    // to be serializable, it has to be written as a lambda.
    PAssert.thatSingleton(databaseManifest)
        .satisfies( // Checker function.
            (SerializableFunction<String, Void>)
                input -> {
                  Builder builder1 = Export.newBuilder();
                  try {
                    JsonFormat.parser().merge(input, builder1);
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                  Export manifestProto = builder1.build();
                  assertThat(manifestProto.getTablesCount(), is(2));
                  String table1Name = manifestProto.getTables(0).getName();
                  assertThat(table1Name, startsWith("table"));
                  assertThat(
                      manifestProto.getTables(0).getManifestFile(),
                      is(table1Name + "-manifest.json"));
                  return null;
                });

    pipeline.run();
  }
}
