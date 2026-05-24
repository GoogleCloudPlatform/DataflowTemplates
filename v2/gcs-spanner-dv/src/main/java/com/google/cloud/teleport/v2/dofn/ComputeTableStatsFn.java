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
package com.google.cloud.teleport.v2.dofn;

import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** A {@link DoFn} that computes {@link TableValidationStats} from a {@link CoGbkResult}. */
public class ComputeTableStatsFn extends DoFn<KV<String, CoGbkResult>, TableValidationStats> {

  private final String runId;
  private final String startTimestamp;
  private final TupleTag<Long> matchedTag;
  private final TupleTag<Long> missInSpannerTag;
  private final TupleTag<Long> missInSourceTag;

  @StartBundle
  public void startBundle() {
    try {
      System.out.println("Worker: manual class inspection started.");

      System.out.println("Worker: TableValidationStats methods:");
      for (java.lang.reflect.Method m : TableValidationStats.class.getDeclaredMethods()) {
        System.out.println(
            "TableValidationStats Method: " + m.getReturnType().getName() + " " + m.getName());
      }

      System.out.println("Worker: ValidationSummary methods:");
      for (java.lang.reflect.Method m : ValidationSummary.class.getDeclaredMethods()) {
        System.out.println(
            "Worker: ValidationSummary Method: " + m.getReturnType().getName() + " " + m.getName());
      }

      System.out.println("=== WORKER PRE-FLIGHT DIAGNOSTIC CHECK START ===");
      org.apache.beam.sdk.schemas.SchemaRegistry registry =
          org.apache.beam.sdk.schemas.SchemaRegistry.createDefault();

      // 1. Test TableValidationStats Coder
      try {
        System.out.println("Worker Pre-flight: Testing TableValidationStats Coder...");
        org.apache.beam.sdk.coders.Coder<TableValidationStats> statsCoder =
            org.apache.beam.sdk.schemas.SchemaCoder.of(
                registry.getSchema(TableValidationStats.class),
                org.apache.beam.sdk.values.TypeDescriptor.of(TableValidationStats.class),
                registry.getToRowFunction(TableValidationStats.class),
                registry.getFromRowFunction(TableValidationStats.class));

        TableValidationStats stats =
            TableValidationStats.builder()
                .setRunId("diagnostic-run-worker")
                .setTableName("test-table")
                .setStatus("MATCH")
                .setSourceRowCount(10L)
                .setDestinationRowCount(10L)
                .setMatchedRowCount(10L)
                .setMismatchRowCount(0L)
                .setStartTimestamp("2026-05-24T11:45:39.000Z")
                .setEndTimestamp("2026-05-24T11:45:39.000Z")
                .build();

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        statsCoder.encode(stats, out);
        byte[] bytes = out.toByteArray();
        System.out.println(
            "Worker Pre-flight: Successfully encoded TableValidationStats ("
                + bytes.length
                + " bytes).");

        TableValidationStats decoded = statsCoder.decode(new java.io.ByteArrayInputStream(bytes));
        System.out.println(
            "Worker Pre-flight: Successfully decoded TableValidationStats: " + decoded);
      } catch (Exception e) {
        System.err.println("Worker Pre-flight ERROR: TableValidationStats coder test failed!");
        e.printStackTrace(System.err);
      }

      // 2. Test ValidationSummary Coder
      try {
        System.out.println("Worker Pre-flight: Testing ValidationSummary Coder...");
        org.apache.beam.sdk.coders.Coder<ValidationSummary> summaryCoder =
            org.apache.beam.sdk.coders.SerializableCoder.of(ValidationSummary.class);

        ValidationSummary summary =
            ValidationSummary.builder()
                .setRunId("diagnostic-run-worker")
                .setSourceDatabase("GCS")
                .setDestinationDatabase("Spanner")
                .setStatus("MATCH")
                .setTotalTablesValidated(1L)
                .setTablesWithMismatches("")
                .setTotalRowsMatched(10L)
                .setTotalRowsMismatched(0L)
                .setStartTimestamp("2026-05-24T11:45:39.000Z")
                .setEndTimestamp("2026-05-24T11:45:39.000Z")
                .build();

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        summaryCoder.encode(summary, out);
        byte[] bytes = out.toByteArray();
        System.out.println(
            "Worker Pre-flight: Successfully encoded ValidationSummary ("
                + bytes.length
                + " bytes).");

        ValidationSummary decoded = summaryCoder.decode(new java.io.ByteArrayInputStream(bytes));
        System.out.println("Worker Pre-flight: Successfully decoded ValidationSummary: " + decoded);
      } catch (Exception e) {
        System.err.println("Worker Pre-flight ERROR: ValidationSummary coder test failed!");
        e.printStackTrace(System.err);
      }

      System.out.println("=== WORKER PRE-FLIGHT DIAGNOSTIC CHECK END ===");
    } catch (Exception e) {
      System.out.println("Worker: Error during manual diagnostic execution: " + e);
      e.printStackTrace();
    }
  }

  public ComputeTableStatsFn(
      String runId,
      String startTimestamp,
      TupleTag<Long> matchedTag,
      TupleTag<Long> missInSpannerTag,
      TupleTag<Long> missInSourceTag) {
    this.runId = runId;
    this.startTimestamp = startTimestamp;
    this.matchedTag = matchedTag;
    this.missInSpannerTag = missInSpannerTag;
    this.missInSourceTag = missInSourceTag;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String tableName = c.element().getKey();
    CoGbkResult result = c.element().getValue();

    long matched = result.getOnly(matchedTag, 0L);
    long onlyInGcs = result.getOnly(missInSpannerTag, 0L);
    long onlyInSpanner = result.getOnly(missInSourceTag, 0L);
    long mismatch = onlyInGcs + onlyInSpanner;

    String status = mismatch == 0 ? "MATCH" : "MISMATCH";
    Instant now = Instant.now();

    TableValidationStats stats =
        TableValidationStats.builder()
            .setRunId(runId)
            .setTableName(tableName)
            .setStatus(status)
            .setSourceRowCount(matched + onlyInGcs)
            .setDestinationRowCount(matched + onlyInSpanner)
            .setMatchedRowCount(matched)
            .setMismatchRowCount(mismatch)
            .setStartTimestamp(startTimestamp)
            .setEndTimestamp(now.toString())
            .build();
    System.out.println("ComputeTableStatsFn: outputting stats: " + stats);
    c.output(stats);
  }
}
