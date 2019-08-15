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

package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.templates.BulkCompressor.Compressor;
import com.google.cloud.teleport.util.TestUtils;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link BulkCompressor} class. */
@RunWith(JUnit4.class)
public class BulkCompressorTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String FILE_BASE_NAME = "bulk-compress-test.txt";
  private static final List<String> FILE_CONTENT =
      ImmutableList.of("Sad Trombone", "Reflected Optics", "Quiet Incline");

  private static Path tempFolderCompressedPath;
  private static Path tempFolderUncompressedPath;
  private static ResourceId textFile;

  @BeforeClass
  public static void setupClass() throws IOException {
    // Test folders
    tempFolderCompressedPath = tempFolder.newFolder("compressed").toPath();
    tempFolderUncompressedPath = tempFolder.newFolder("uncompressed").toPath();

    // Test file
    textFile =
        TestUtils.writeToFile(
            tempFolderUncompressedPath.resolve(FILE_BASE_NAME).toString(), FILE_CONTENT);
  }

  /** Tests the {@link BulkCompressor.Compressor} performs compression properly. */
  @Test
  public void testCompressFile() throws Exception {
    // Setup test
    final Compression compression = Compression.GZIP;

    final ValueProvider<String> outputDirectoryProvider =
        pipeline.newProvider(tempFolderCompressedPath.toString());

    final ValueProvider<Compression> compressionProvider = StaticValueProvider.of(compression);

    final Metadata metadata = FileSystems.matchSingleFileSpec(textFile.toString());

    // Execute the compressor
    PCollection<String> lines = pipeline
        .apply("Create File Input", Create.of(metadata))
        .apply("Compress", ParDo.of(new Compressor(outputDirectoryProvider, compressionProvider)))
        .apply("Read the Files", TextIO.readAll().withCompression(Compression.AUTO));

    // Test the result
    PAssert.that(lines).containsInAnyOrder(FILE_CONTENT);
    pipeline.run();
  }
}
