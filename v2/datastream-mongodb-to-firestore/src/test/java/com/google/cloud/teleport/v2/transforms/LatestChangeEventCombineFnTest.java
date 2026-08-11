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
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.templates.datastream.MongoDbChangeEventContext;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LatestChangeEventCombineFn}. */
@RunWith(JUnit4.class)
public class LatestChangeEventCombineFnTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private LatestChangeEventCombineFn combineFn;

  @Before
  public void setUp() {
    combineFn = new LatestChangeEventCombineFn();
  }

  private MongoDbChangeEventContext createEventContext(
      String docId, long seconds, int nanos, String changeType, boolean isDlqReconsumed)
      throws Exception {
    return createEventContext(docId, seconds, nanos, changeType, isDlqReconsumed, "cdc");
  }

  private MongoDbChangeEventContext createEventContext(
      String docId,
      long seconds,
      int nanos,
      String changeType,
      boolean isDlqReconsumed,
      String readMethod)
      throws Exception {
    String payload =
        String.format(
            "{"
                + "\"_metadata_source\": {\"collection\": \"users\"},"
                + "\"_id\": \"\\\"%s\\\"\","
                + "\"_metadata_timestamp_seconds\": %d,"
                + "\"_metadata_timestamp_nanos\": %d,"
                + "\"_metadata_change_type\": \"%s\","
                + "\"_metadata_read_method\": \"%s\","
                + "\"isDlqReconsumed\": \"%s\","
                + "\"data\": \"{\\\"field1\\\": \\\"val1\\\"}\""
                + "}",
            docId, seconds, nanos, changeType, readMethod, isDlqReconsumed ? "true" : "false");
    return new MongoDbChangeEventContext(OBJECT_MAPPER.readTree(payload), "shadow_");
  }

  @Test
  public void testNullAccumulator() {
    assertNull(combineFn.createAccumulator());
  }

  @Test
  public void testAddInput_preservesNewer() throws Exception {
    MongoDbChangeEventContext older = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext newer = createEventContext("doc1", 1000L, 200, "UPDATE", false);

    MongoDbChangeEventContext acc = combineFn.addInput(null, older);
    assertEquals(older, acc);

    acc = combineFn.addInput(acc, newer);
    assertEquals(newer, acc);

    // Stale input should not overwrite newer accumulator
    MongoDbChangeEventContext stale = createEventContext("doc1", 1000L, 150, "UPDATE", false);
    acc = combineFn.addInput(acc, stale);
    assertEquals(newer, acc);
  }

  @Test
  public void testMergeAccumulators_findsGlobalWinner() throws Exception {
    MongoDbChangeEventContext e1 = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext e2 = createEventContext("doc1", 1000L, 500, "UPDATE", false);
    MongoDbChangeEventContext e3 = createEventContext("doc1", 1000L, 300, "UPDATE", false);

    MongoDbChangeEventContext winner = combineFn.mergeAccumulators(Arrays.asList(e1, e2, e3));
    assertEquals(e2, winner);
  }

  @Test
  public void testDlqReconsumed_prefersDlqOnEqualTimestamp() throws Exception {
    MongoDbChangeEventContext standard = createEventContext("doc1", 1000L, 100, "INSERT", false);
    MongoDbChangeEventContext dlq = createEventContext("doc1", 1000L, 100, "INSERT", true);

    MongoDbChangeEventContext acc = combineFn.addInput(standard, dlq);
    assertEquals(dlq, acc);
  }

  @Test
  public void testBackfillVsCdc_cdcWinsOnSameSecond() throws Exception {
    MongoDbChangeEventContext backfill =
        createEventContext("doc1", 1000L, 999000000, "READ", false, "backfill");
    MongoDbChangeEventContext cdc = createEventContext("doc1", 1000L, 100, "UPDATE", false, "cdc");

    // Even though backfill has 999M nanos and cdc has 100 oplog inc, CDC must win
    MongoDbChangeEventContext acc = combineFn.addInput(backfill, cdc);
    assertEquals(cdc, acc);

    MongoDbChangeEventContext acc2 = combineFn.addInput(cdc, backfill);
    assertEquals(cdc, acc2);
  }
}
