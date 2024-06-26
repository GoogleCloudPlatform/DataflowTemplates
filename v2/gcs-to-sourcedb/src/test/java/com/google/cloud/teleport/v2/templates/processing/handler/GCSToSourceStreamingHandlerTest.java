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
package com.google.cloud.teleport.v2.templates.processing.handler;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.common.ProcessingContext;
import com.google.cloud.teleport.v2.templates.common.TrimmedShardedDataChangeRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

public class GCSToSourceStreamingHandlerTest {
  private static final String VALID_GCS_PATH = "gs://my-bucket/my-path/";

  @Captor private ArgumentCaptor<BlobInfo> blobInfoCaptor;

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testWriteFilteredEventsToGcs_Success() {
    ProcessingContext taskContext = mock(ProcessingContext.class);
    Storage mockStorage = mock(Storage.class);
    Shard shard = mock(Shard.class);
    when(taskContext.getGCSPath()).thenReturn(VALID_GCS_PATH);
    when(taskContext.getStartTimestamp()).thenReturn("2023-06-23T10:15:30Z");
    when(taskContext.getWindowDuration()).thenReturn(org.joda.time.Duration.standardMinutes(10));
    when(shard.getLogicalShardId()).thenReturn("shard-123");
    when(taskContext.getShard()).thenReturn(shard);
    List<Mod> mods = new ArrayList<>();

    List<TrimmedShardedDataChangeRecord> filteredEvents = new ArrayList<>();
    TrimmedShardedDataChangeRecord record =
        new TrimmedShardedDataChangeRecord(
            Timestamp.parseTimestamp("2023-06-23T10:15:30Z"),
            "serverTxnId-123",
            "recordSeq-123",
            "tableName",
            mods,
            ModType.INSERT,
            1L,
            "txnTag-123");

    filteredEvents.add(record);

    GCSToSourceStreamingHandler.writeFilteredEventsToGcs(taskContext, mockStorage, filteredEvents);

    verify(mockStorage)
        .create(
            blobInfoCaptor.capture(),
            eq(filteredEvents.toString().getBytes(StandardCharsets.UTF_8)));

    BlobInfo capturedBlobInfo = blobInfoCaptor.getValue();
    assertEquals(capturedBlobInfo.getBucket(), "my-bucket");
    assertEquals(
        capturedBlobInfo.getName(),
        "my-path/filteredEvents/shard-123/2023-06-23T10:15:30.000Z-2023-06-23T10:25:30.000Z-pane-0-last-0-of-1.txt");
  }

  @Test
  public void testWriteFilteredEventsToGcs_StorageException() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unable to ensure write access for the file path:");
    ProcessingContext taskContext = mock(ProcessingContext.class);
    Storage mockStorage = mock(Storage.class);
    when(taskContext.getGCSPath()).thenReturn(VALID_GCS_PATH);
    when(taskContext.getStartTimestamp()).thenReturn("2023-06-23T10:15:30Z");
    when(taskContext.getWindowDuration()).thenReturn(org.joda.time.Duration.standardMinutes(10));
    Shard shard = mock(Shard.class);
    when(shard.getLogicalShardId()).thenReturn("shard-123");
    when(taskContext.getShard()).thenReturn(shard);

    List<TrimmedShardedDataChangeRecord> filteredEvents = new ArrayList<>();

    doThrow(new RuntimeException("Storage exception"))
        .when(mockStorage)
        .create(any(BlobInfo.class), any(byte[].class));

    GCSToSourceStreamingHandler.writeFilteredEventsToGcs(taskContext, mockStorage, filteredEvents);
  }
}
