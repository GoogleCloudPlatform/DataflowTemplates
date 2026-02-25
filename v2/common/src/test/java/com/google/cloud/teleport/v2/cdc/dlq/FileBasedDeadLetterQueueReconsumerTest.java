/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.cdc.dlq;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the FileBasedDeadLetterQueueReconsumer transform and components. */
@RunWith(JUnit4.class)
public class FileBasedDeadLetterQueueReconsumerTest {

  private static final String[] JSON_FILE_CONTENTS_1 = {
    "{\"message\":{\"datasample1\":\"datasample1\"}, \"error_message\":\"errorsample3\"}",
    "{\"message\":{\"datasample2\":\"datasample2\"}, \"error_message\":\"errorsample3\"}",
    "{\"message\":{\"badcharacters\":\"abc îé def\"}, \"error_message\":\"errorsample3\"}"
  };

  private static final String[] JSON_RESULTS_1 = {
    "{\"datasample1\":\"datasample1\",\"_metadata_error\":\"errorsample3\","
        + "\"_metadata_retry_count\":1}",
    "{\"datasample2\":\"datasample2\",\"_metadata_error\":\"errorsample3\","
        + "\"_metadata_retry_count\":1}",
    "{\"badcharacters\":\"abc îé def\",\"_metadata_error\":\"errorsample3\","
        + "\"_metadata_retry_count\":1}"
  };

  static final Logger LOG = LoggerFactory.getLogger(FileBasedDeadLetterQueueReconsumerTest.class);

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public TestPipeline p = TestPipeline.create();

  private String createJsonFile(String filename, String[] fileLines) throws IOException {
    File f = folder.newFile(filename);
    OutputStream outputStream = new FileOutputStream(f.getAbsolutePath());
    OutputStreamWriter w = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);

    for (String line : fileLines) {
      w.write(line);
      w.write('\n');
    }
    w.close();
    return f.getAbsolutePath();
  }

  @Test
  public void testFilesAreConsumed() throws IOException {
    String fileName = createJsonFile("dlqFile1.json", JSON_FILE_CONTENTS_1);
    // Adding in a file that should not be consumed.
    folder.newFolder("data");
    createJsonFile("data/donotReadMe.json", JSON_FILE_CONTENTS_1);

    String folderPath = Paths.get(folder.getRoot().getAbsolutePath()).resolve("*").toString();
    PCollection<String> jsonData =
        p.apply(FileIO.match().filepattern(folderPath))
            .apply(FileBasedDeadLetterQueueReconsumer.moveAndConsumeMatches());
    PAssert.that(jsonData).containsInAnyOrder(JSON_RESULTS_1);
    p.run().waitUntilFinish();

    assertFalse(new File(fileName).exists());
  }

  @Test
  public void testAllFilesAreConsumed() throws IOException {
    TestStream<String> inputFiles =
        TestStream.create(StringUtf8Coder.of())
            .addElements(
                createJsonFile("dlqFile1.json", JSON_FILE_CONTENTS_1),
                createJsonFile("dlqFile2.json", JSON_FILE_CONTENTS_1))
            .addElements(createJsonFile("dlqFile3.json", JSON_FILE_CONTENTS_1))
            .advanceWatermarkToInfinity();

    PCollection<String> jsonData =
        p.apply(inputFiles)
            .apply(FileIO.matchAll())
            .apply(FileBasedDeadLetterQueueReconsumer.moveAndConsumeMatches());

    PAssert.that(jsonData)
        .containsInAnyOrder(
            Stream.of(JSON_RESULTS_1)
                .flatMap(line -> Stream.of(line, line, line))
                .collect(Collectors.toList()));

    p.run().waitUntilFinish();
  }

  @Test
  public void testReadData() throws IOException, FileNotFoundException {
    String jsonPath = createJsonFile("dlqFile3.json", JSON_FILE_CONTENTS_1);
    ResourceId resourceId = FileSystems.matchNewResource(jsonPath, false);
    String expected = Arrays.stream(JSON_FILE_CONTENTS_1).collect(Collectors.joining("\n"));

    String text =
        FileBasedDeadLetterQueueReconsumer.readFile(resourceId)
            .lines()
            .collect(Collectors.joining("\n"));

    assertThat(text).isEqualTo(expected);
  }
}
