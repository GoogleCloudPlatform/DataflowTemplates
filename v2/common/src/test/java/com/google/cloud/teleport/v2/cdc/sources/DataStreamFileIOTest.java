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

import com.google.cloud.teleport.v2.cdc.sources.DataStreamFileIO.FindChildrenDirectories;
import com.google.cloud.teleport.v2.cdc.sources.DataStreamFileIO.PerKeyHeartBeat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
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
public class DataStreamFileIOTest {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamFileIOTest.class);

  public static final String BUCKET = "gs://ds-fileio-tests/";
  public static final String ROOT_PATH_WITH_DIRECTORIES = "path-with-directories/";
  public static final String ROOT_PATH_WITH_FILES = "path-with-files/";

  @Test
  public void testHeartBeats() {
    TestStream<String> inputs = TestStream.create(StringUtf8Coder.of())
        .addElements("key1")
        .advanceProcessingTime(Duration.standardSeconds(10))
        .addElements("key2")
        .advanceProcessingTime(Duration.standardSeconds(10))
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    PCollection<String> values = pipeline.apply(inputs)
        .apply(WithKeys.of(elm -> elm))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(ParDo.of(new PerKeyHeartBeat(Duration.standardSeconds(10))));

    PAssert.that(values).containsInAnyOrder("key1", "key1", "key2");
  }

  @Ignore  // Need to ignore this test because it accesses GCS resources.
  @Test
  public void testFullContinuous() {
    TestStream<String> mainStream = TestStream.create(StringUtf8Coder.of())
        .addElements("")
        .advanceProcessingTime(Duration.standardSeconds(10))
        .advanceWatermarkToInfinity();

    Pipeline pipeline = Pipeline.create();

    PCollection<String> directories = pipeline
        .apply(mainStream)
        .apply(DataStreamFileIO.matchRootPathPeriodically(
            BUCKET + ROOT_PATH_WITH_FILES, Duration.standardSeconds(10)));

    PAssert.that(directories).containsInAnyOrder(
        "gs://ds-fileio-tests/path-with-files/HR_JOBS/2020/07/14/11/03/",
        "gs://ds-fileio-tests/path-with-files/HR_JOBS/2020/07/14/12/16/");
    pipeline.run().waitUntilFinish();
  }

  @Ignore  // Need to ignore this test because it accesses GCS resources.
  @Test
  public void testMatchDirectoriesOnce() {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> matches = pipeline
        .apply(Create.of(
            KV.of(BUCKET+ ROOT_PATH_WITH_DIRECTORIES, ""),
            KV.of(BUCKET+ ROOT_PATH_WITH_DIRECTORIES, "")))
        .apply(ParDo.of(new FindChildrenDirectories(1, 1)));

    PAssert.that(matches).containsInAnyOrder(
        BUCKET + ROOT_PATH_WITH_DIRECTORIES + "HR_JOBS/",
        BUCKET + ROOT_PATH_WITH_DIRECTORIES + "HR_SALARIES/"
    );
    pipeline.run().waitUntilFinish();
  }

  @Ignore  // Need to ignore this test because it accesses GCS resources.
  @Test
  public void testMatchDeeperDirectories() {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> matches = pipeline
        .apply(Create.of(KV.of(BUCKET+ ROOT_PATH_WITH_DIRECTORIES, "")))
        .apply(ParDo.of(new FindChildrenDirectories(1, 2)));

    PAssert.that(matches).containsInAnyOrder(
        BUCKET + ROOT_PATH_WITH_DIRECTORIES + "HR_JOBS/",
        BUCKET + ROOT_PATH_WITH_DIRECTORIES + "HR_JOBS/this_should_not_be_found/",
        BUCKET + ROOT_PATH_WITH_DIRECTORIES + "HR_SALARIES/"
    );
    pipeline.run().waitUntilFinish();
  }

  @Ignore  // Need to ignore this test because it accesses GCS resources.
  @Test
  public void testMatchOnlyDeeperDirectories() {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> matches = pipeline
        .apply(Create.of(KV.of(BUCKET+ ROOT_PATH_WITH_DIRECTORIES, "")))
        .apply(ParDo.of(new FindChildrenDirectories(1, 1)))
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(path -> KV.of(path, "")))
        .apply(ParDo.of(new FindChildrenDirectories(1, 1)));

    PAssert.that(matches).containsInAnyOrder(
        BUCKET + ROOT_PATH_WITH_DIRECTORIES + "HR_JOBS/this_should_not_be_found/"
    );
    pipeline.run().waitUntilFinish();
  }
}
