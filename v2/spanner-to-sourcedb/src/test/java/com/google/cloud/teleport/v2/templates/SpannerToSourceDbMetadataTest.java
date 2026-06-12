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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Utility tests to boost coverage for {@link SpannerToSourceDb}. */
@RunWith(JUnit4.class)
public class SpannerToSourceDbMetadataTest {

  @Test
  public void testCalculateConnectionPoolSizePerWorker() {
    assertEquals(5, SpannerToSourceDb.calculateConnectionPoolSizePerWorker(50L, 10));
    assertEquals(1, SpannerToSourceDb.calculateConnectionPoolSizePerWorker(10L, 10));
    
    assertThrows(IllegalArgumentException.class, () -> 
        SpannerToSourceDb.calculateConnectionPoolSizePerWorker(5L, 10));
  }

  @Test
  public void testBuildDlqManager() {
    SpannerToSourceDb.Options options = PipelineOptionsFactory.as(SpannerToSourceDb.Options.class);
    options.setDeadLetterQueueDirectory("gs://test/dlq");
    options.setTempLocation("gs://test/temp/");
    
    assertNotNull(SpannerToSourceDb.buildDlqManager(options));
    assertEquals("gs://test/dlq", options.getDeadLetterQueueDirectory());

    // Test default path
    options.setDeadLetterQueueDirectory("");
    SpannerToSourceDb.buildDlqManager(options);
    assertEquals("gs://test/temp/dlq/", options.getDeadLetterQueueDirectory());
  }

  @Test
  public void testGetReadChangeStreamDoFn() {
    SpannerToSourceDb.Options options = PipelineOptionsFactory.as(SpannerToSourceDb.Options.class);
    options.setChangeStreamName("test-stream");
    options.setMetadataInstance("test-instance");
    options.setMetadataDatabase("test-db");
    options.setStartTimestamp("");
    options.setEndTimestamp("");
    options.setSpannerPriority(com.google.cloud.spanner.Options.RpcPriority.HIGH);
    
    org.apache.beam.sdk.io.gcp.spanner.SpannerConfig mockSpannerConfig = 
        org.apache.beam.sdk.io.gcp.spanner.SpannerConfig.create();

    assertNotNull(SpannerToSourceDb.getReadChangeStreamDoFn(options, mockSpannerConfig));
  }
}
