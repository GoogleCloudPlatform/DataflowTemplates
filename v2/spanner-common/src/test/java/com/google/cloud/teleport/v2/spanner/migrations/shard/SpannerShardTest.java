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
package com.google.cloud.teleport.v2.spanner.migrations.shard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SpannerShardTest {

  @Test
  public void gettersReturnConstructorValues() {
    SpannerShard shard = new SpannerShard("p1", "i1", "d1");
    assertEquals("", shard.getLogicalShardId());
    assertEquals("p1", shard.getProjectId());
    assertEquals("i1", shard.getInstanceId());
    assertEquals("d1", shard.getDatabaseId());
  }

  @Test
  public void databaseIdMapsToDbName() {
    SpannerShard shard = new SpannerShard("p", "i", "mydb");
    assertEquals("mydb", shard.getDbName());
  }

  @Test
  public void equalsReturnsTrueForSameInstance() {
    SpannerShard a = new SpannerShard("p", "i", "d");
    assertEquals(a, a);
  }

  @Test
  public void equalsReturnsFalseForDifferentProject() {
    SpannerShard a = new SpannerShard("p1", "i", "d");
    SpannerShard b = new SpannerShard("p2", "i", "d");
    assertNotEquals(a, b);
  }

  @Test
  public void equalsReturnsFalseForDifferentInstance() {
    SpannerShard a = new SpannerShard("p", "i1", "d");
    SpannerShard b = new SpannerShard("p", "i2", "d");
    assertNotEquals(a, b);
  }

  @Test
  public void equalsReturnsFalseForDifferentDatabase() {
    SpannerShard a = new SpannerShard("p", "i", "d1");
    SpannerShard b = new SpannerShard("p", "i", "d2");
    assertNotEquals(a, b);
  }

  @Test
  public void equalsReturnsFalseForNull() {
    SpannerShard a = new SpannerShard("p", "i", "d");
    assertNotEquals(a, null);
  }

  @Test
  public void equalsReturnsFalseForDifferentType() {
    SpannerShard a = new SpannerShard("p", "i", "d");
    assertNotEquals(a, "not-a-shard");
  }

  @Test
  public void toStringIncludesAllFields() {
    SpannerShard shard = new SpannerShard("proj", "inst", "db");
    String s = shard.toString();
    assertTrue(s.contains("proj"));
    assertTrue(s.contains("inst"));
    assertTrue(s.contains("db"));
  }
}
