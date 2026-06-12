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
package com.google.cloud.teleport.v2.templates.models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.migrations.shard.SpannerShard;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Coverage boost for model boilerplate methods. */
@RunWith(JUnit4.class)
public class ModelBoilerplateTest {

  @Test
  public void testSpannerShardBoilerplate() {
    SpannerShard shard1 = new SpannerShard("p", "i", "d");
    SpannerShard shard2 = new SpannerShard("p", "i", "d");
    SpannerShard shard3 = new SpannerShard("p2", "i2", "d2");

    assertEquals(shard1, shard2);
    assertNotEquals(shard1, shard3);
    assertEquals(shard1.hashCode(), shard2.hashCode());
    assertNotNull(shard1.toString());
  }

  @Test
  public void testSpannerMutationResponseBoilerplate() {
    Mutation m = Mutation.delete("T", com.google.cloud.spanner.Key.of(1));
    SpannerMutationResponse response = new SpannerMutationResponse(m);

    assertEquals(m, response.getMutation());
    assertNotNull(response.toString());
  }

  @Test
  public void testDMLGeneratorResponseBoilerplate() {
    DMLGeneratorResponse response = new DMLGeneratorResponse("INSERT");
    assertEquals("INSERT", response.getDmlStatement());
    response.setDmlStatement("UPDATE");
    assertEquals("UPDATE", response.getDmlStatement());
  }

  @Test
  public void testShadowTableRecordBoilerplate() {
    com.google.cloud.Timestamp now = com.google.cloud.Timestamp.now();
    ShadowTableRecord record1 = new ShadowTableRecord(now, 100L);
    ShadowTableRecord record2 = new ShadowTableRecord(now, 100L);
    ShadowTableRecord record3 = new ShadowTableRecord(now, 200L);

    assertEquals(now, record1.getProcessedCommitTimestamp());
    assertEquals(100L, record1.getRecordSequence());
    assertTrue(record1.equals(record2));
    assertFalse(record1.equals(record3));
    assertTrue(ShadowTableRecord.isEquals(record1, record2));
    assertFalse(ShadowTableRecord.isEquals(record1, null));
    assertTrue(ShadowTableRecord.isEquals(null, null));
  }
}
