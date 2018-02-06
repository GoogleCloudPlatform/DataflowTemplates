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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.teleport.templates.Decompressor.Decompress;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.CompressedSource.CompressionMode;
import org.apache.beam.sdk.io.FileBasedSink.CompressionType;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link Decompressor} class. */
@RunWith(JUnit4.class)
public class DecompressorTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String FILE_BASE_NAME = "decompressor-test";
  private static final List<String> FILE_CONTENT =
      Arrays.asList("Irritable eagle", "Optimistic jay", "Fanciful hawk");

  private static Path tempFolderRootPath;
  private static Path tempFolderOutputPath;
  private static ResourceId compressedFile1;
  private static ResourceId compressedFile2;
  private static ResourceId uncompressedFile;
  private static ResourceId unknownCompressionFile;

  @BeforeClass
  public static void setupClass() throws IOException {
    tempFolderRootPath = tempFolder.getRoot().toPath();
    tempFolderOutputPath = tempFolder.newFolder("output").toPath();

    // test files
    compressedFile1 = writeToFile(FILE_CONTENT, FILE_BASE_NAME + ".gz", CompressionType.GZIP);
    compressedFile2 = writeToFile(FILE_CONTENT, FILE_BASE_NAME + ".deflate", CompressionType.BZIP2);
    uncompressedFile =
        writeToFile(FILE_CONTENT, FILE_BASE_NAME + ".bz2", CompressionType.UNCOMPRESSED);
    unknownCompressionFile =
        writeToFile(FILE_CONTENT, FILE_BASE_NAME, CompressionType.UNCOMPRESSED);
  }

  /**
   * Tests the {@link Decompressor.FileLister} when the inputFilePattern resolves to a single file.
   */
  @Test
  public void testFileListerSingleFile() throws Exception {
    StaticValueProvider<String> inputFilePattern =
        StaticValueProvider.of(compressedFile1.toString());

    List<KV<ResourceId, CompressionMode>> results =
        DoFnTester.of(new Decompressor.FileLister()).processBundle(inputFilePattern);

    KV<ResourceId, CompressionMode> expected = KV.of(compressedFile1, CompressionMode.GZIP);

    assertThat(results.size(), is(equalTo(1)));
    assertThat(results, hasItems(expected));
  }

  /** Tests the {@link Decompressor.FileLister} when the inputFilePattern is a glob. */
  @SuppressWarnings("unchecked")
  @Test
  public void testFileListerGlob() throws Exception {
    String glob = tempFolderRootPath.toString() + File.separatorChar + "*.*";
    StaticValueProvider<String> inputFilePattern = StaticValueProvider.of(glob);

    List<KV<ResourceId, CompressionMode>> results =
        DoFnTester.of(new Decompressor.FileLister()).processBundle(inputFilePattern);

    assertThat(results.size(), is(equalTo(3)));
    assertThat(
        results,
        hasItems(
            KV.of(compressedFile1, CompressionMode.GZIP),
            KV.of(compressedFile2, CompressionMode.DEFLATE),
            KV.of(uncompressedFile, CompressionMode.BZIP2)));
  }

  /**
   * Tests the {@link Decompressor.FileLister} when the inputFilePattern does not match any files.
   */
  @Test
  public void testFileListerNoMatch() throws Exception {
    String glob = tempFolder.toString() + File.separatorChar + "*.gzip";
    StaticValueProvider<String> inputFilePattern = StaticValueProvider.of(glob);

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Failed to match any files with the pattern " + glob);

    DoFnTester.of(new Decompressor.FileLister()).processBundle(inputFilePattern);
  }

  /**
   * Tests the {@link Decompressor.FileLister} when a file matched by the pattern does not match any
   * compression mode.
   */
  @Test
  public void testFileListerUnknownCompression() throws Exception {
    StaticValueProvider<String> inputFilePattern =
        StaticValueProvider.of(unknownCompressionFile.toString());

    DoFnTester<ValueProvider<String>, KV<ResourceId, CompressionMode>> doFnTester =
        DoFnTester.of(new Decompressor.FileLister());

    List<KV<ResourceId, CompressionMode>> mainOutput = doFnTester.processBundle(inputFilePattern);

    List<KV<String, String>> deadletterOutput =
        doFnTester.takeOutputElements(Decompressor.DEADLETTER_TAG);

    assertThat(mainOutput.size(), is(equalTo(0)));
    assertThat(deadletterOutput.size(), is(equalTo(1)));

    KV<String, String> failure = deadletterOutput.get(0);
    assertThat(failure.getKey(), is(equalTo(inputFilePattern.get())));
    assertThat(
        failure.getValue(),
        is(
            equalTo(
                String.format(
                    "Skipping file %s because it did not match any compression mode (%s)",
                    unknownCompressionFile.getFilename(),
                    Arrays.toString(CompressionMode.values())))));
  }

  /** Tests the {@link Decompressor.Decompress} performs the decompression properly. */
  @Test
  public void testDecompressCompressedFile() throws Exception {
    StaticValueProvider<String> outputDirectory =
        StaticValueProvider.of(tempFolderOutputPath.toString());

    DoFnTester.of(new Decompress(outputDirectory))
        .processBundle(KV.of(compressedFile1, CompressionMode.GZIP));

    Path path = tempFolderOutputPath.resolve(FILE_BASE_NAME);
    List<String> lines = Files.readAllLines(path);

    assertThat(lines, is(equalTo(FILE_CONTENT)));
  }

  /** Tests the {@link Decompressor.Decompress} when a matched file is uncompressed. */
  @Test
  public void testDecompressUncompressedFile() throws Exception {
    StaticValueProvider<String> outputDirectory =
        StaticValueProvider.of(tempFolderOutputPath.toString());

    DoFnTester<KV<ResourceId, CompressionMode>, Void> doFnTester =
        DoFnTester.of(new Decompress(outputDirectory));

    List<Void> mainOutput =
        doFnTester.processBundle(KV.of(uncompressedFile, CompressionMode.BZIP2));

    List<KV<String, String>> deadletterOutput =
        doFnTester.takeOutputElements(Decompressor.DEADLETTER_TAG);

    assertThat(mainOutput.size(), is(equalTo(0)));
    assertThat(deadletterOutput.size(), is(equalTo(1)));

    KV<String, String> failure = deadletterOutput.get(0);
    assertThat(failure.getKey(), is(equalTo(uncompressedFile.toString())));
    assertThat(
        failure.getValue(),
        is(
            equalTo(
                String.format(
                    "The file resource %s is malformed or not in %s compressed format.",
                    uncompressedFile.toString(), CompressionMode.BZIP2))));
  }

  /** Tests the {@link Decompressor.Shuffle} transform. */
  @Test
  public void testShuffle() throws Exception {
    // Create a test pipeline
    Pipeline pipeline = TestPipeline.create();

    // Create the input PCollection
    KV<ResourceId, CompressionMode> kv1 = KV.of(compressedFile1, CompressionMode.GZIP);
    KV<ResourceId, CompressionMode> kv2 = KV.of(compressedFile2, CompressionMode.ZIP);

    PCollection<KV<ResourceId, CompressionMode>> input =
        pipeline.apply(
            Create.of(kv1, kv2)
                .withCoder(
                    KvCoder.of(
                        SerializableCoder.of(ResourceId.class),
                        SerializableCoder.of(CompressionMode.class))));

    // Apply the Shuffle transform
    PCollection<KV<ResourceId, CompressionMode>> output = input.apply(Decompressor.Shuffle.of());

    // Assert on the results
    PAssert.that(output).containsInAnyOrder(kv1, kv2);
  }

  //////////////////// Helpers ///////////////////////////

  /**
   * Helper to generate files for testing.
   *
   * @param lines The lines to write.
   * @param filename The filename of the file to write.
   * @param compression The compression type of the file.
   * @return The file written.
   * @throws IOException If an error occurs while creating or writing the file.
   */
  private static ResourceId writeToFile(
      List<String> lines, String filename, CompressionType compression) throws IOException {

    String filePath = tempFolderRootPath.resolve(filename).toString();
    String fileContents = String.join(System.lineSeparator(), lines);

    ResourceId resourceId = FileSystems.matchNewResource(filePath, false);

    String mimeType =
        compression == CompressionType.UNCOMPRESSED ? MimeTypes.TEXT : compression.getMimeType();

    // Write the file contents to the channel and close.
    try (ReadableByteChannel readChannel =
        Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
      try (WritableByteChannel writeChannel =
          compression.create(FileSystems.create(resourceId, mimeType))) {
        ByteStreams.copy(readChannel, writeChannel);
      }
    }

    return resourceId;
  }
}
