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
package com.google.cloud.teleport.v2.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimestampSortKey}. */
@RunWith(JUnit4.class)
public class TimestampSortKeyTest {

  @Test
  public void testEpochSecondsComparison_primaryTier() {
    TimestampSortKey olderKey = TimestampSortKey.of(100L, 500L, true);
    TimestampSortKey newerKey = TimestampSortKey.of(200L, 100L, true);

    assertTrue(newerKey.compareTo(olderKey) > 0);
    assertTrue(olderKey.compareTo(newerKey) < 0);
  }

  @Test
  public void testStreamTypePrecedence_sameSecondCDCsupersedesBackfill() {
    TimestampSortKey backfillKey = TimestampSortKey.backfill(1000L, 999999L);
    TimestampSortKey cdcKey = TimestampSortKey.cdc(1000L, 1L);

    // CDC must strictly supersede Backfill at the same epoch second
    assertTrue(cdcKey.compareTo(backfillKey) > 0);
    assertTrue(backfillKey.compareTo(cdcKey) < 0);
  }

  @Test
  public void testSubSecondsComparison_tertiaryTier() {
    TimestampSortKey cdc1 = TimestampSortKey.cdc(1000L, 1L);
    TimestampSortKey cdc2 = TimestampSortKey.cdc(1000L, 2L);

    assertTrue(cdc2.compareTo(cdc1) > 0);
    assertTrue(cdc1.compareTo(cdc2) < 0);

    TimestampSortKey bf1 = TimestampSortKey.backfill(1000L, 100L);
    TimestampSortKey bf2 = TimestampSortKey.backfill(1000L, 200L);

    assertTrue(bf2.compareTo(bf1) > 0);
    assertTrue(bf1.compareTo(bf2) < 0);
  }

  @Test
  public void testEqualityAndHashCode() {
    TimestampSortKey key1 = TimestampSortKey.cdc(1000L, 5L);
    TimestampSortKey key2 = TimestampSortKey.cdc(1000L, 5L);
    TimestampSortKey key3 = TimestampSortKey.backfill(1000L, 5L);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
    assertEquals(0, key1.compareTo(key2));

    assertNotEquals(key1, key3);
  }

  @Test(expected = NullPointerException.class)
  public void testCompareTo_nullThrows() {
    TimestampSortKey key = TimestampSortKey.cdc(100L, 1L);
    key.compareTo(null);
  }
}
