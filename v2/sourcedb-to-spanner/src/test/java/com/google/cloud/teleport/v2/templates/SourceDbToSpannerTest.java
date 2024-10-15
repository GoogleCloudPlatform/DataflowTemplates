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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Test;
import org.mockito.Mockito;

public class SourceDbToSpannerTest {

  @Test
  public void testCreateSpannerConfig() {
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getProjectId()).thenReturn("testProject");
    when(mockOptions.getSpannerHost()).thenReturn("testHost");
    when(mockOptions.getInstanceId()).thenReturn("testInstance");
    when(mockOptions.getDatabaseId()).thenReturn("testDatabaseId");

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(mockOptions);
    assertEquals(config.getProjectId().get(), "testProject");
    assertEquals(config.getInstanceId().get(), "testInstance");
    assertEquals(config.getDatabaseId().get(), "testDatabaseId");
  }
}
