/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

import com.google.cloud.teleport.v2.spanner.migrations.metadata.SpannerToGcsJobMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class JobMetadataUpdaterTest {
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private SpannerDao spannerDaoMock;

  @Test
  public void jobMetadataUpdaterSuccess() {
    doNothing()
        .when(spannerDaoMock)
        .upsertSpannerToGcsMetadata(anyString(), anyString(), anyString());
    SpannerToGcsJobMetadata jobMetadata =
        new SpannerToGcsJobMetadata("2023-12-19T10:00:00Z", "10s");
    JobMetadataUpdater.writeStartAndDuration(spannerDaoMock, "run1", jobMetadata);

    Mockito.verify(spannerDaoMock)
        .upsertSpannerToGcsMetadata("2023-12-19T10:00:00.000Z", "10s", "run1");
  }

  @Test
  public void jobMetadataUpdaterWindowAlignedSuccess() {
    doNothing()
        .when(spannerDaoMock)
        .upsertSpannerToGcsMetadata(anyString(), anyString(), anyString());
    SpannerToGcsJobMetadata jobMetadata =
        new SpannerToGcsJobMetadata("2023-12-26T10:26:02.707000000Z", "10s");
    JobMetadataUpdater.writeStartAndDuration(spannerDaoMock, "run1", jobMetadata);

    Mockito.verify(spannerDaoMock)
        .upsertSpannerToGcsMetadata("2023-12-26T10:26:00.000Z", "10s", "run1");
  }

  @Test
  public void jobMetadataUpdaterFailed() {

    doThrow(RuntimeException.class)
        .when(spannerDaoMock)
        .upsertSpannerToGcsMetadata(anyString(), anyString(), anyString());
    SpannerToGcsJobMetadata jobMetadata =
        new SpannerToGcsJobMetadata("2023-12-19T10:00:00Z", "10s");
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> JobMetadataUpdater.writeStartAndDuration(spannerDaoMock, "run1", jobMetadata));
  }
}
