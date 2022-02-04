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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.cdc.dlq.FileBasedDeadLetterQueueReconsumer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test the FileBasedDeadLetterQueueReconsumer transform and components. */
@RunWith(JUnit4.class)
public class DLQWriteTransformTest {

  private static final String JSON_ROW_CONTENT =
      "{\"message\":{\"badcharacters\":\"abc îé def\"}, \"error_message\":\"errorsample3\"}";

  private static final String[] JSON_RESULTS_1 = {
    "{\"datasample1\":\"datasample1\",\"_metadata_error\":\"errorsample3\","
        + "\"_metadata_retry_count\":1}",
    "{\"datasample2\":\"datasample2\",\"_metadata_error\":\"errorsample3\","
        + "\"_metadata_retry_count\":1}",
    "{\"badcharacters\":\"abc îé def\",\"_metadata_error\":\"errorsample3\","
        + "\"_metadata_retry_count\":1}"
  };

  static final Logger LOG = LoggerFactory.getLogger(DLQWriteTransformTest.class);

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testFilesAreWritten() throws IOException, FileNotFoundException {
    File dlqDir = folder.newFolder("dlq/");
    p.apply(Create.of(JSON_ROW_CONTENT).withCoder(StringUtf8Coder.of()))
        .apply(
            "Write To DLQ/Writer",
            DLQWriteTransform.WriteDLQ.newBuilder()
                .withDlqDirectory(dlqDir.getAbsolutePath())
                .withTmpDirectory(folder.newFolder(".temp/").getAbsolutePath())
                .build());
    p.run().waitUntilFinish();

    File[] files = dlqDir.listFiles();
    assertThat(files).isNotEmpty();

    ResourceId resourceId = FileSystems.matchNewResource(files[0].getAbsolutePath(), false);
    BufferedReader reader = FileBasedDeadLetterQueueReconsumer.readFile(resourceId);
    assertThat(reader.readLine()).isEqualTo(JSON_ROW_CONTENT);
  }
}
