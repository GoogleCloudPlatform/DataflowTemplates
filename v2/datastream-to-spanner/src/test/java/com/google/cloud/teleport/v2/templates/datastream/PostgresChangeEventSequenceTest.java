/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Unit tests for testing change event comparison logic in Postgres database. */
public final class PostgresChangeEventSequenceTest {

  private final long previousEventTimestamp = 1615159727L;

  private final long eventTimestamp = 1615159728L;

  @Test
  public void canOrderBasedOnTimestamp() {
    PostgresChangeEventSequence oldEvent =
        new PostgresChangeEventSequence(previousEventTimestamp, "16/123");
    PostgresChangeEventSequence newEvent =
        new PostgresChangeEventSequence(eventTimestamp, "16/123");

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnRightLsn() {
    PostgresChangeEventSequence oldEvent =
        new PostgresChangeEventSequence(eventTimestamp, "16/FFFFFFFE");
    PostgresChangeEventSequence newEvent =
        new PostgresChangeEventSequence(eventTimestamp, "16/FFFFFFFF");

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnLeftLsn() {
    PostgresChangeEventSequence oldEvent =
        new PostgresChangeEventSequence(eventTimestamp, "5E30A78/3B9ACA00");
    PostgresChangeEventSequence newEvent =
        new PostgresChangeEventSequence(eventTimestamp, "3B9ACA00/5E30A78");

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void equalLeftAndRightLsn() {
    PostgresChangeEventSequence oldEvent =
        new PostgresChangeEventSequence(eventTimestamp, "3B9ACA00/16");
    PostgresChangeEventSequence newEvent =
        new PostgresChangeEventSequence(eventTimestamp, "3B9ACA00/16");

    assertTrue(oldEvent.compareTo(newEvent) == 0);
  }

  @Test
  public void testGetParsedLSN() {
    PostgresChangeEventSequence sequence = new PostgresChangeEventSequence(123456L, "16/2A50F3");
    Long expectedParsedLSN = 2773235L;
    Long actualParsedLSN = sequence.getParsedLSN(1);
    assertEquals(expectedParsedLSN, actualParsedLSN);
  }

  @Test
  public void testGetParsedLSNInvalid() {
    PostgresChangeEventSequence sequence = new PostgresChangeEventSequence(123456L, "16/2A50H3");
    assertThrows(NumberFormatException.class, () -> sequence.getParsedLSN(1));
  }

  @Test
  public void testGetParsedLSNForOutOfBoundIndex() {
    PostgresChangeEventSequence sequence = new PostgresChangeEventSequence(123456L, "16");
    Long expectedParsedLSN = 0L;
    Long actualParsedLSN = sequence.getParsedLSN(1);
    assertEquals(expectedParsedLSN, actualParsedLSN);
  }

  @Test
  public void canOrderDumpEventAndCDCEventAtSameTimestamp() {
    PostgresChangeEventSequence dumpEvent = new PostgresChangeEventSequence(eventTimestamp, "");
    PostgresChangeEventSequence cdcEvent =
        new PostgresChangeEventSequence(eventTimestamp, "16/123");

    assertTrue(dumpEvent.compareTo(cdcEvent) < 0);
    assertTrue(cdcEvent.compareTo(dumpEvent) > 0);
  }
}
