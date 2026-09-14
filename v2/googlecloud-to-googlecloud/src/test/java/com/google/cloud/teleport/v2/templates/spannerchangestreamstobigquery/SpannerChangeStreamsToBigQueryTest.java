/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import com.google.cloud.spanner.SpannerException;
import com.google.cloud.teleport.v2.options.SpannerChangeStreamsToBigQueryOptions;
import com.google.cloud.teleport.v2.spanner.SpannerTestHelper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerChangeStreamsToBigQuery}. */
@RunWith(JUnit4.class)
public final class SpannerChangeStreamsToBigQueryTest extends SpannerTestHelper {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  private static final String TEST_LABEL = "cs2bq";
  private static final String TEST_PROJECT = "span-cloud-testing";
  private static final String TEST_INSTANCE = "change-stream-test";
  private static final String TEST_TABLE = "Users";
  private static final String TEST_CHANGE_STREAM = "UsersStream";

  private static String fakeTempLocation;

  public SpannerChangeStreamsToBigQueryTest() {
    super(TEST_LABEL);
  }

  @Before
  public void setup() throws Exception {
    fakeTempLocation = tmpDir.newFolder("temporaryLocation").getAbsolutePath();
    super.setUp();
  }

  @After
  public void tearDown() throws NoSuchFieldException, IllegalAccessException {
    super.tearDown();
  }

  @Test
  public void testSpannerDirectedReadOptions() {
    mockGetDialect();

    exception.expect(SpannerException.class);
    SpannerChangeStreamsToBigQueryOptions options =
        PipelineOptionsFactory.create().as(SpannerChangeStreamsToBigQueryOptions.class);
    options.setSpannerProjectId(TEST_PROJECT);
    options.setSpannerInstanceId(TEST_INSTANCE);
    options.setSpannerDatabase(TEST_TABLE);
    options.setSpannerMetadataInstanceId(TEST_INSTANCE);
    options.setSpannerMetadataDatabase(TEST_TABLE);
    options.setSpannerChangeStreamName(TEST_CHANGE_STREAM);
    options.setBigQueryDataset("test-dataset");
    options.setTempLocation(fakeTempLocation);
    options.setSpannerDirectedReadOptions(
        "{\"includeReplicas\":{\"replicaSelections\":[{\"location\":\"us-central1\",\"type\":\"READ_ONLY\"}]}}");

    SpannerChangeStreamsToBigQuery.run(options);
  }
}
