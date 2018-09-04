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
import com.google.cloud.teleport.spanner.ExportTransform.BuildTableManifests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
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
    builder.addFilesBuilder().setName(f1.getFileName().toString()).setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    builder.addFilesBuilder().setName(f2.getFileName().toString()).setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    String manifest1 = JsonFormat.printer().print(builder.build());

    builder = TableManifest.newBuilder();
    builder.addFilesBuilder().setName(f3.getFileName().toString()).setMd5("1B2M2Y8AsgTpgAmY7PhCfg==");
    String manifest2 = JsonFormat.printer().print(builder.build());

    // Execute the transform.
    final Map<String, Iterable<String>> tablesAndFiles =
        ImmutableMap.of(
            "table1", ImmutableList.of(f1.toString(), f2.toString()),
            "table2", ImmutableList.of(f3.toString()));

    PCollection<KV<String, String>> tableManifests =
        pipeline
            .apply("Create", Create.of(tablesAndFiles))
            .apply(ParDo.of(new BuildTableManifests()));

    PAssert.that(tableManifests)
        .containsInAnyOrder(KV.of("table1", manifest1), KV.of("table2", manifest2));
    pipeline.run();
  }
}
