/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.writer;

import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

public class SpannerWriterTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCreateSpannerWriter() {
    SpannerConfig conf =
        SpannerConfig.create().withProjectId("p1").withInstanceId("instance").withDatabaseId("db1");
    SpannerWriter writer = new SpannerWriter(conf);
    assertNotNull(writer.getSpannerWrite());
  }

  @Test(expected = NullPointerException.class)
  public void testCreateSpannerWriter_NullPointerException() {
    SpannerWriter writer = new SpannerWriter(null);
    assertNotNull(writer.getSpannerWrite());
  }
}
