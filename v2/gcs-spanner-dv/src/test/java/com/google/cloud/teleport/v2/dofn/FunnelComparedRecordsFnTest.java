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
package com.google.cloud.teleport.v2.dofn;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FunnelComparedRecordsFnTest {

  @Test
  public void testMatch() {
    // Mock key and CoGbkResult
    String key = "testKey";
    CoGbkResult coGbkResult = mock(CoGbkResult.class);
    DoFn<KV<String, CoGbkResult>, ComparisonRecord>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    // Mock ComparisonRecord
    ComparisonRecord sourceRecord =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash")
            .setPrimaryKeyColumns(ImmutableList.of())
            .build();
    ComparisonRecord spannerRecord =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash")
            .setPrimaryKeyColumns(ImmutableList.of())
            .build();

    // Setup CoGbkResult behavior for MATCH (both present)
    when(coGbkResult.getAll(SOURCE_TAG)).thenReturn(ImmutableList.of(sourceRecord));
    when(coGbkResult.getAll(SPANNER_TAG)).thenReturn(ImmutableList.of(spannerRecord));

    // Setup Context behavior
    when(context.element()).thenReturn(KV.of(key, coGbkResult));

    // Execute
    FunnelComparedRecordsFn fn = new FunnelComparedRecordsFn();
    fn.processElement(context);

    // Verify output: Should be MATCHED_TAG with sourceRecord
    verify(context).output(MATCHED_TAG, sourceRecord);
  }

  @Test
  public void testMissingInSpanner() {
    // Mock key and CoGbkResult
    String key = "testKey";
    CoGbkResult coGbkResult = mock(CoGbkResult.class);
    DoFn<KV<String, CoGbkResult>, ComparisonRecord>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    // Mock ComparisonRecord
    ComparisonRecord sourceRecord =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash")
            .setPrimaryKeyColumns(ImmutableList.of())
            .build();

    // Setup CoGbkResult behavior for MISSING_IN_SPANNER (Source only)
    when(coGbkResult.getAll(SOURCE_TAG)).thenReturn(ImmutableList.of(sourceRecord));
    when(coGbkResult.getAll(SPANNER_TAG)).thenReturn(ImmutableList.of());

    // Setup Context behavior
    when(context.element()).thenReturn(KV.of(key, coGbkResult));

    // Execute
    FunnelComparedRecordsFn fn = new FunnelComparedRecordsFn();
    fn.processElement(context);

    // Verify output
    verify(context).output(MISSING_IN_SPANNER_TAG, sourceRecord);
  }

  @Test
  public void testMissingInSource() {
    // Mock key and CoGbkResult
    String key = "testKey";
    CoGbkResult coGbkResult = mock(CoGbkResult.class);
    DoFn<KV<String, CoGbkResult>, ComparisonRecord>.ProcessContext context =
        mock(DoFn.ProcessContext.class);

    // Mock ComparisonRecord
    ComparisonRecord spannerRecord =
        ComparisonRecord.builder()
            .setTableName("Table1")
            .setHash("hash")
            .setPrimaryKeyColumns(ImmutableList.of())
            .build();

    // Setup CoGbkResult behavior for MISSING_IN_SOURCE (Spanner only)
    when(coGbkResult.getAll(SOURCE_TAG)).thenReturn(ImmutableList.of());
    when(coGbkResult.getAll(SPANNER_TAG)).thenReturn(ImmutableList.of(spannerRecord));

    // Setup Context behavior
    when(context.element()).thenReturn(KV.of(key, coGbkResult));

    // Execute
    FunnelComparedRecordsFn fn = new FunnelComparedRecordsFn();
    fn.processElement(context);

    // Verify output
    verify(context).output(MISSING_IN_SOURCE_TAG, spannerRecord);
  }
}
