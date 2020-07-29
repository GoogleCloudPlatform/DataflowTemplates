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

package com.google.cloud.teleport.v2.cdc.sources;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the scalable Datastream FileIO.
 *
 * Tests that require GCS access have been marked to Ignore,
 * but should be run when developing locally.
 */
@RunWith(JUnit4.class)
public class DataStreamIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamIOTest.class);

  public static final String BUCKET = "gs://ds-fileio-tests/";
  public static final String ROOT_PATH_WITH_DIRECTORIES = "path-with-directories/";
  public static final String ROOT_PATH_WITH_FILES = "path-with-files/";

  @Ignore
  @Test
  public void testFullContinuous() {
    TestStream<String> mainStream = TestStream.create(StringUtf8Coder.of())
        .addElements(BUCKET)
        .advanceProcessingTime(Duration.standardSeconds(10))
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    PCollection<String> directories = pipeline
        .apply(mainStream)
        .apply(new DataStreamIO());

    PAssert.that(directories).containsInAnyOrder(
        "gs://ds-fileio-tests/path-with-files/HR_JOBS/2020/07/14/11/03/",
        "gs://ds-fileio-tests/path-with-files/HR_JOBS/2020/07/14/12/16/");
    pipeline.run().waitUntilFinish();
  }

}
