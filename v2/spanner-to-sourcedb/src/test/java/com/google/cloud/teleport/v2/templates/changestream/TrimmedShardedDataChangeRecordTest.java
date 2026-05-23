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
package com.google.cloud.teleport.v2.templates.changestream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.junit.Test;

public class TrimmedShardedDataChangeRecordTest {

  @Test
  public void testEquals() {
    Timestamp ts1 = Timestamp.ofTimeSecondsAndNanos(1000, 0);
    Timestamp ts2 = Timestamp.ofTimeSecondsAndNanos(2000, 0);
    Mod mod1 = new Mod("{\"id\": 1}", "{}", "{}");
    Mod mod2 = new Mod("{\"id\": 2}", "{}", "{}");

    TrimmedShardedDataChangeRecord record1 =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table1", mod1, ModType.INSERT, 1, "tag1");
    record1.setShard("shard1");

    TrimmedShardedDataChangeRecord record2 =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table1", mod1, ModType.INSERT, 1, "tag1");
    record2.setShard("shard1");

    assertTrue(record1.equals(record1)); // Same instance
    assertTrue(record1.equals(record2)); // Equal

    // Test differences in each field
    TrimmedShardedDataChangeRecord diffTs =
        new TrimmedShardedDataChangeRecord(
            ts2, "txn1", "seq1", "table1", mod1, ModType.INSERT, 1, "tag1");
    diffTs.setShard("shard1");
    assertFalse(record1.equals(diffTs));

    TrimmedShardedDataChangeRecord diffTxn =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn2", "seq1", "table1", mod1, ModType.INSERT, 1, "tag1");
    diffTxn.setShard("shard1");
    assertFalse(record1.equals(diffTxn));

    TrimmedShardedDataChangeRecord diffSeq =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq2", "table1", mod1, ModType.INSERT, 1, "tag1");
    diffSeq.setShard("shard1");
    assertFalse(record1.equals(diffSeq));

    TrimmedShardedDataChangeRecord diffTable =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table2", mod1, ModType.INSERT, 1, "tag1");
    diffTable.setShard("shard1");
    assertFalse(record1.equals(diffTable));

    TrimmedShardedDataChangeRecord diffMod =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table1", mod2, ModType.INSERT, 1, "tag1");
    diffMod.setShard("shard1");
    assertFalse(record1.equals(diffMod));

    TrimmedShardedDataChangeRecord diffModType =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table1", mod1, ModType.DELETE, 1, "tag1");
    diffModType.setShard("shard1");
    assertFalse(record1.equals(diffModType));

    TrimmedShardedDataChangeRecord diffCount =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table1", mod1, ModType.INSERT, 2, "tag1");
    diffCount.setShard("shard1");
    assertFalse(record1.equals(diffCount));

    TrimmedShardedDataChangeRecord diffTag =
        new TrimmedShardedDataChangeRecord(
            ts1, "txn1", "seq1", "table1", mod1, ModType.INSERT, 1, "tag2");
    diffTag.setShard("shard1");
    assertFalse(record1.equals(diffTag));

    TrimmedShardedDataChangeRecord diffShard = new TrimmedShardedDataChangeRecord(record1);
    diffShard.setShard("shard2");
    assertFalse(record1.equals(diffShard));

    assertFalse(record1.equals(null)); // Null
    assertFalse(record1.equals("string")); // Different class
  }
}
