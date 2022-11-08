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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.templates.BulkDecompressor.DEADLETTER_TAG;
import static com.google.cloud.teleport.templates.BulkDecompressor.DECOMPRESS_MAIN_OUT_TAG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.templates.BulkDecompressor.Decompress;
import com.google.cloud.teleport.util.TestUtils;
import com.google.common.io.Files;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link BulkDecompressor} class. */
@RunWith(JUnit4.class)
public class BulkDecompressorTest {

  @Rule public ExpectedException exception = ExpectedException.none();
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TestPipeline validatorPipeline = TestPipeline.create();
  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String FILE_BASE_NAME = "decompressor-test";
  private static final List<String> FILE_CONTENT =
      Arrays.asList("Irritable eagle", "Optimistic jay", "Fanciful hawk");

  private static Path tempFolderOutputPath;
  private static ResourceId compressedFile;
  private static ResourceId wrongCompressionExtFile;
  private static ResourceId uncompressedFile;
  private static ResourceId unknownCompressionFile;

  @BeforeClass
  public static void setupClass() throws IOException {
    Path tempFolderRootPath = tempFolder.getRoot().toPath();
    tempFolderOutputPath = tempFolder.newFolder("output").toPath();

    // Test files
    compressedFile =
        TestUtils.writeToFile(
            tempFolderRootPath
                .resolve(FILE_BASE_NAME + Compression.GZIP.getSuggestedSuffix())
                .toString(),
            FILE_CONTENT,
            Compression.GZIP);

    wrongCompressionExtFile =
        TestUtils.writeToFile(
            tempFolderRootPath
                .resolve(FILE_BASE_NAME + Compression.DEFLATE.getSuggestedSuffix())
                .toString(),
            FILE_CONTENT,
            Compression.BZIP2);

    uncompressedFile =
        TestUtils.writeToFile(
            tempFolderRootPath
                .resolve(FILE_BASE_NAME + Compression.BZIP2.getSuggestedSuffix())
                .toString(),
            FILE_CONTENT,
            Compression.UNCOMPRESSED);

    unknownCompressionFile =
        TestUtils.writeToFile(
            tempFolderRootPath.resolve(FILE_BASE_NAME).toString(),
            FILE_CONTENT,
            Compression.UNCOMPRESSED);
  }

  /** Tests the {@link BulkDecompressor.Decompress} performs the decompression properly. */
  @Test
  public void testDecompressCompressedFile() throws Exception {
    // Arrange
    //
    final ValueProvider<String> outputDirectory =
        pipeline.newProvider(tempFolderOutputPath.toString());

    final Metadata compressedFile1Metadata =
        FileSystems.matchSingleFileSpec(compressedFile.toString());

    final Metadata compressedFile2Metadata =
        FileSystems.matchSingleFileSpec(wrongCompressionExtFile.toString());

    final String expectedOutputFilename = Files.getNameWithoutExtension(compressedFile.toString());

    final String expectedOutputFilePath =
        tempFolderOutputPath.resolve(expectedOutputFilename).normalize().toString();

    // Act
    //
    PCollectionTuple decompressOut =
        pipeline
            .apply("CreateWorkItems", Create.of(compressedFile1Metadata, compressedFile2Metadata))
            .apply(
                "Decompress",
                ParDo.of(new Decompress(outputDirectory))
                    .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    // Assert
    //
    PAssert.that(decompressOut.get(DECOMPRESS_MAIN_OUT_TAG))
        .containsInAnyOrder(expectedOutputFilePath);

    PAssert.that(decompressOut.get(DEADLETTER_TAG))
        .satisfies(
            collection -> {
              KV<String, String> kv = collection.iterator().next();
              assertThat(kv.getKey(), is(equalTo(compressedFile2Metadata.resourceId().toString())));
              assertThat(kv.getValue(), is(notNullValue()));
              return null;
            });

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Validate the uncompressed file written has the expected file content.
    PCollection<String> validatorOut =
        validatorPipeline.apply("ReadOutputFile", TextIO.read().from(expectedOutputFilePath));

    PAssert.that(validatorOut).containsInAnyOrder(FILE_CONTENT);

    validatorPipeline.run();
  }

  /** Tests the {@link BulkDecompressor.Decompress} when a matched file is uncompressed. */
  @Test
  public void testDecompressUncompressedFile() throws Exception {
    // Arrange
    //
    final ValueProvider<String> outputDirectory =
        pipeline.newProvider(tempFolderOutputPath.toString());

    final Metadata uncompressedFileMetadata =
        FileSystems.matchSingleFileSpec(uncompressedFile.toString());

    // Act
    //
    PCollectionTuple decompressOut =
        pipeline
            .apply("CreateWorkItems", Create.of(uncompressedFileMetadata))
            .apply(
                "Decompress",
                ParDo.of(new Decompress(outputDirectory))
                    .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    // Assert
    //
    PAssert.that(decompressOut.get(DECOMPRESS_MAIN_OUT_TAG)).empty();
    PAssert.that(decompressOut.get(DEADLETTER_TAG))
        .satisfies(
            collection -> {
              KV<String, String> kv = collection.iterator().next();
              assertThat(
                  kv.getKey(), is(equalTo(uncompressedFileMetadata.resourceId().toString())));
              assertThat(
                  kv.getValue(),
                  containsString(
                      String.format(
                          "The file resource %s is malformed or not in %s compressed format.",
                          uncompressedFile.toString(), Compression.BZIP2)));
              return null;
            });

    pipeline.run();
  }

  /**
   * Tests the {@link BulkDecompressor.Decompress} when a matched file does not match a known
   * compression.
   */
  @Test
  public void testDecompressUnknownCompressionFile() throws Exception {
    // Arrange
    //
    final ValueProvider<String> outputDirectory =
        pipeline.newProvider(tempFolderOutputPath.toString());

    final Metadata unknownCompressionFileMetadata =
        FileSystems.matchSingleFileSpec(unknownCompressionFile.toString());

    // Act
    //
    PCollectionTuple decompressOut =
        pipeline
            .apply("CreateWorkItems", Create.of(unknownCompressionFileMetadata))
            .apply(
                "Decompress",
                ParDo.of(new Decompress(outputDirectory))
                    .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    // Assert
    //
    PAssert.that(decompressOut.get(DECOMPRESS_MAIN_OUT_TAG)).empty();
    PAssert.that(decompressOut.get(DEADLETTER_TAG))
        .satisfies(
            collection -> {
              KV<String, String> kv = collection.iterator().next();
              assertThat(
                  kv.getKey(), is(equalTo(unknownCompressionFileMetadata.resourceId().toString())));
              assertThat(
                  kv.getValue(),
                  containsString(
                      String.format(
                          BulkDecompressor.UNCOMPRESSED_ERROR_MSG,
                          unknownCompressionFile.toString(),
                          BulkDecompressor.SUPPORTED_COMPRESSIONS)));
              return null;
            });

    pipeline.run();
  }
}
