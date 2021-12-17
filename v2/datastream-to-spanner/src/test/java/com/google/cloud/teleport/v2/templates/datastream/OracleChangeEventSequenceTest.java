/*
 * Copyright (C) 2021 Google LLC
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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Unit tests for testing change event comparison logic in Oracle database. */
public final class OracleChangeEventSequenceTest {

  private final long previousEventTimestamp = 1615159727L;

  private final long eventTimestamp = 1615159728L;

  @Test
  public void canOrderBasedOnTimestamp() {
    OracleChangeEventSequence oldEvent = new OracleChangeEventSequence(previousEventTimestamp, 2L);
    OracleChangeEventSequence newEvent = new OracleChangeEventSequence(eventTimestamp, 2L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderBasedOnScn() {
    OracleChangeEventSequence oldEvent = new OracleChangeEventSequence(eventTimestamp, 2L);
    OracleChangeEventSequence newEvent = new OracleChangeEventSequence(eventTimestamp, 3L);

    assertTrue(oldEvent.compareTo(newEvent) < 0);
    assertTrue(newEvent.compareTo(oldEvent) > 0);
  }

  @Test
  public void canOrderDumpEventAndCDCEventAtSameTimestamp() {
    OracleChangeEventSequence dumpEvent = new OracleChangeEventSequence(eventTimestamp, -1L);
    OracleChangeEventSequence cdcEvent = new OracleChangeEventSequence(eventTimestamp, 3L);

    assertTrue(dumpEvent.compareTo(cdcEvent) < 0);
    assertTrue(cdcEvent.compareTo(dumpEvent) > 0);
  }
}
