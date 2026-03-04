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
package com.google.cloud.teleport.v2.fn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
import com.google.cloud.teleport.v2.dto.ValidationSummaryAccumulator;
import java.util.Arrays;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ValidationSummaryCombineFnTest {

  @Test
  public void testCreateAccumulator() {
    ValidationSummaryCombineFn fn =
        new ValidationSummaryCombineFn("run1", Instant.now(), "src", "dst");
    ValidationSummaryAccumulator acc = fn.createAccumulator();
    assertEquals(0L, acc.totalTables);
    assertEquals(0L, acc.totalMatched);
    assertEquals(0L, acc.totalMismatched);
    assertTrue(acc.tablesWithMismatches.isEmpty());
  }

  @Test
  public void testAddInput() {
    ValidationSummaryCombineFn fn =
        new ValidationSummaryCombineFn("run1", Instant.now(), "src", "dst");
    ValidationSummaryAccumulator acc = fn.createAccumulator();

    TableValidationStats stats =
        TableValidationStats.builder()
            .setTableName("table1")
            .setMatchedRowCount(100L)
            .setMismatchRowCount(5L)
            .setRunId("run1")
            .setStatus("status")
            .setSourceRowCount(105L)
            .setDestinationRowCount(105L)
            .setStartTimestamp(Instant.now())
            .setEndTimestamp(Instant.now())
            .build();

    fn.addInput(acc, stats);

    assertEquals(1L, acc.totalTables);
    assertEquals(100L, acc.totalMatched);
    assertEquals(5L, acc.totalMismatched);
    assertTrue(acc.tablesWithMismatches.contains("table1"));
  }

  @Test
  public void testMergeAccumulators() {
    ValidationSummaryCombineFn fn =
        new ValidationSummaryCombineFn("run1", Instant.now(), "src", "dst");

    ValidationSummaryAccumulator acc1 = new ValidationSummaryAccumulator();
    acc1.totalTables = 1;
    acc1.totalMatched = 10;
    acc1.totalMismatched = 2;
    acc1.tablesWithMismatches.add("t1");

    ValidationSummaryAccumulator acc2 = new ValidationSummaryAccumulator();
    acc2.totalTables = 1;
    acc2.totalMatched = 20;
    acc2.totalMismatched = 0;

    ValidationSummaryAccumulator merged = fn.mergeAccumulators(Arrays.asList(acc1, acc2));

    assertEquals(2L, merged.totalTables);
    assertEquals(30L, merged.totalMatched);
    assertEquals(2L, merged.totalMismatched);
    assertTrue(merged.tablesWithMismatches.contains("t1"));
    assertEquals(1, merged.tablesWithMismatches.size());
  }

  @Test
  public void testExtractOutput_Match() {
    Instant start = Instant.now();
    ValidationSummaryCombineFn fn = new ValidationSummaryCombineFn("run1", start, "src", "dst");

    ValidationSummaryAccumulator acc = new ValidationSummaryAccumulator();
    acc.totalTables = 2;
    acc.totalMatched = 100;
    acc.totalMismatched = 0;

    ValidationSummary summary = fn.extractOutput(acc);

    assertEquals("MATCH", summary.getStatus());
    assertEquals("run1", summary.getRunId());
    assertEquals("src", summary.getSourceDatabase());
    assertEquals("dst", summary.getDestinationDatabase());
    assertEquals(2L, (long) summary.getTotalTablesValidated());
    assertEquals(100L, (long) summary.getTotalRowsMatched());
    assertEquals(0L, (long) summary.getTotalRowsMismatched());
    assertEquals("", summary.getTablesWithMismatches());
    assertEquals(start, summary.getStartTimestamp());
  }

  @Test
  public void testExtractOutput_Mismatch() {
    ValidationSummaryCombineFn fn =
        new ValidationSummaryCombineFn("run1", Instant.now(), "src", "dst");

    ValidationSummaryAccumulator acc = new ValidationSummaryAccumulator();
    acc.totalTables = 1;
    acc.totalMatched = 50;
    acc.totalMismatched = 5;
    acc.tablesWithMismatches.add("t1");

    ValidationSummary summary = fn.extractOutput(acc);

    assertEquals("MISMATCH", summary.getStatus());
    assertEquals(5L, (long) summary.getTotalRowsMismatched());
    assertEquals("t1", summary.getTablesWithMismatches());
  }
}
