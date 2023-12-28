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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.templates.common.ShardProgress;
import com.google.cloud.teleport.v2.templates.dao.SpannerDao;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class ShardProgressTrackerTest {
  @Rule public final MockitoRule mocktio = MockitoJUnit.rule();
  @Mock private SpannerDao spannerDaoMock;

  @Test
  public void shardProgressTrackerInitSuccess() {
    doNothing().when(spannerDaoMock).checkAndCreateShardProgressTable();
    ShardProgressTracker shardProgressTracker =
        Mockito.spy(new ShardProgressTracker(spannerDaoMock, "run1"));
    shardProgressTracker.init();
    Mockito.verify(shardProgressTracker).init();
  }

  @Test
  public void shardProgressTrackerInitFailed() {

    doThrow(RuntimeException.class).when(spannerDaoMock).checkAndCreateShardProgressTable();
    ShardProgressTracker shardProgressTracker = new ShardProgressTracker(spannerDaoMock, "run1");
    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> shardProgressTracker.init());
  }

  @Test
  public void shardProgressTrackerGetShardProgressSuccess() {
    ShardProgress shardProgress = new ShardProgress("shardA", Timestamp.now(), "SUCCESS");
    Map<String, ShardProgress> expectedResponse = new HashMap<>();
    expectedResponse.put("shardA", shardProgress);
    doReturn(expectedResponse).when(spannerDaoMock).getShardProgress("run1");
    ShardProgressTracker shardProgressTracker = new ShardProgressTracker(spannerDaoMock, "run1");
    Map<String, ShardProgress> response = shardProgressTracker.getShardProgress();
    assertEquals(
        expectedResponse.get("shardA").getFileStartInterval(),
        response.get("shardA").getFileStartInterval());
  }

  @Test
  public void shardProgressTrackerGetShardProgressFailed() {

    doThrow(RuntimeException.class).when(spannerDaoMock).getShardProgress("run1");
    ShardProgressTracker shardProgressTracker = new ShardProgressTracker(spannerDaoMock, "run1");
    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> shardProgressTracker.getShardProgress());
  }

  @Test
  public void shardProgressTrackerWriteShardProgressSuccess() {
    ShardProgress shardProgress = new ShardProgress("shardA", Timestamp.now(), "SUCCESS");
    doNothing().when(spannerDaoMock).writeShardProgress(shardProgress, "run1");
    ShardProgressTracker shardProgressTracker =
        Mockito.spy(new ShardProgressTracker(spannerDaoMock, "run1"));
    shardProgressTracker.writeShardProgress(shardProgress);
    Mockito.verify(shardProgressTracker).writeShardProgress(shardProgress);
  }

  @Test
  public void shardProgressTrackerWriteShardProgressFailed() {
    ShardProgress shardProgress = new ShardProgress("shardA", Timestamp.now(), "SUCCESS");
    doThrow(RuntimeException.class).when(spannerDaoMock).writeShardProgress(shardProgress, "run1");
    ShardProgressTracker shardProgressTracker = new ShardProgressTracker(spannerDaoMock, "run1");
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> shardProgressTracker.writeShardProgress(shardProgress));
  }
}
