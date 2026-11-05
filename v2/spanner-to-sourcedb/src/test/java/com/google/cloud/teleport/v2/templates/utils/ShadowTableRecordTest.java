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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import org.junit.Test;

public class ShadowTableRecordTest {

  @Test
  public void testShadowTableRecord() {
    Timestamp now = Timestamp.now();
    ShadowTableRecord record = new ShadowTableRecord(now, 1L);
    assertEquals(now, record.getProcessedCommitTimestamp());
    assertEquals(1L, record.getRecordSequence());
  }

  @Test
  public void testEquals() {
    Timestamp now = Timestamp.now();
    ShadowTableRecord record1 = new ShadowTableRecord(now, 1L);
    ShadowTableRecord record2 = new ShadowTableRecord(now, 1L);
    ShadowTableRecord record3 = new ShadowTableRecord(now, 2L);

    assertTrue(record1.equals(record2));
    assertFalse(record1.equals(record3));
  }

  @Test
  public void testIsEquals() {
    Timestamp now = Timestamp.now();
    ShadowTableRecord record1 = new ShadowTableRecord(now, 1L);
    ShadowTableRecord record2 = new ShadowTableRecord(now, 1L);
    ShadowTableRecord record3 = new ShadowTableRecord(now, 2L);

    assertTrue(ShadowTableRecord.isEquals(null, null));
    assertFalse(ShadowTableRecord.isEquals(record1, null));
    assertFalse(ShadowTableRecord.isEquals(null, record1));
    assertTrue(ShadowTableRecord.isEquals(record1, record2));
    assertFalse(ShadowTableRecord.isEquals(record1, record3));
  }
}
