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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimestampSortKey}. */
@RunWith(JUnit4.class)
public class TimestampSortKeyTest {

  @Test
  public void testNullEventReturnsNullKey() {
    assertNull(TimestampSortKey.of(null));
  }

  @Test(expected = NullPointerException.class)
  public void testCompareTo_nullThrowsNullPointerException() {
    TimestampSortKey t1 = new TimestampSortKey(1000L, 500L, true);
    t1.compareTo(null);
  }

  @Test
  public void testCompareTo_higherSecondsWins() {
    TimestampSortKey t1 = new TimestampSortKey(1000L, 500L, true);
    TimestampSortKey t2 = new TimestampSortKey(1001L, 100L, true);

    assertTrue(t2.compareTo(t1) > 0);
    assertTrue(t1.compareTo(t2) < 0);
  }

  @Test
  public void testCompareTo_sameSecondCdcBeatsBackfill() {
    TimestampSortKey cdcKey = new TimestampSortKey(1000L, 10L, true);
    TimestampSortKey backfillKey = new TimestampSortKey(1000L, 999999999L, false);

    assertTrue(cdcKey.compareTo(backfillKey) > 0);
    assertTrue(backfillKey.compareTo(cdcKey) < 0);
  }

  @Test
  public void testCompareTo_sameSecondSameStreamTypeComparesSubSeconds() {
    TimestampSortKey cdc1 = new TimestampSortKey(1000L, 10L, true);
    TimestampSortKey cdc2 = new TimestampSortKey(1000L, 20L, true);

    assertTrue(cdc2.compareTo(cdc1) > 0);
    assertTrue(cdc1.compareTo(cdc2) < 0);

    TimestampSortKey bf1 = new TimestampSortKey(1000L, 100L, false);
    TimestampSortKey bf2 = new TimestampSortKey(1000L, 200L, false);

    assertTrue(bf2.compareTo(bf1) > 0);
    assertTrue(bf1.compareTo(bf2) < 0);
  }

  @Test
  public void testCompareTo_equalKeysReturnZero() {
    TimestampSortKey k1 = new TimestampSortKey(1000L, 100L, true);
    TimestampSortKey k2 = new TimestampSortKey(1000L, 100L, true);

    assertEquals(0, k1.compareTo(k2));
    assertEquals(0, k2.compareTo(k1));
  }

  @Test
  public void testEqualsAndHashCode() {
    TimestampSortKey k1 = new TimestampSortKey(1000L, 100L, true);
    TimestampSortKey k2 = new TimestampSortKey(1000L, 100L, true);
    TimestampSortKey k3 = new TimestampSortKey(1000L, 100L, false);
    TimestampSortKey k4 = new TimestampSortKey(1001L, 100L, true);

    assertEquals(k1, k2);
    assertEquals(k1.hashCode(), k2.hashCode());
    assertNotEquals(k1, k3);
    assertNotEquals(k1, k4);
    assertNotEquals(k1, null);
    assertNotEquals(k1, "otherType");
  }

  @Test
  public void testToString() {
    TimestampSortKey cdcKey = new TimestampSortKey(1000L, 100L, true);
    TimestampSortKey backfillKey = new TimestampSortKey(1000L, 500L, false);

    assertEquals("1000:100:cdc", cdcKey.toString());
    assertEquals("1000:500:backfill", backfillKey.toString());
  }
}
