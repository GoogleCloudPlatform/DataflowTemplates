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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class SpannerWriterTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCreateSpannerWriter() {
    SpannerConfig conf =
        SpannerConfig.create().withProjectId("p1").withInstanceId("instance").withDatabaseId("db1");
    SpannerWriter writer = new SpannerWriter(conf, null);
    assertNotNull(writer.getSpannerWrite());
  }

  @Test(expected = NullPointerException.class)
  public void testCreateSpannerWriter_NullPointerException() {
    SpannerWriter writer = new SpannerWriter(null, null);
    assertNotNull(writer.getSpannerWrite());
  }

  @Test
  public void testSetBatchSize() {
    SpannerConfig mockSpannerConfig = Mockito.mock(SpannerConfig.class);
    SpannerIO.Write mockWrite = Mockito.mock(SpannerIO.Write.class);
    Long testBatchSize = 42L;
    when(mockWrite.withBatchSizeBytes(testBatchSize)).thenReturn(mockWrite);

    assertThat(new SpannerWriter(mockSpannerConfig, -1L).setBatchSize(mockWrite))
        .isEqualTo(mockWrite);
    assertThat(new SpannerWriter(mockSpannerConfig, null).setBatchSize(mockWrite))
        .isEqualTo(mockWrite);
    assertThat(new SpannerWriter(mockSpannerConfig, testBatchSize).setBatchSize(mockWrite))
        .isEqualTo(mockWrite);
    verify(mockWrite, times(1)).withBatchSizeBytes(anyLong());
  }
}
