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

import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
import com.google.cloud.teleport.v2.dto.ValidationSummaryAccumulator;
import org.apache.beam.sdk.transforms.Combine;
import org.joda.time.Instant;

/**
 * A {@link Combine.CombineFn} that aggregates individual {@link TableValidationStats} into a single
 * global {@link ValidationSummary}.
 *
 * <p>In Apache Beam, a {@link Combine.CombineFn} is used to perform associative and commutative
 * aggregations on a {@link org.apache.beam.sdk.values.PCollection}. It works by distributing the
 * aggregation across multiple workers:
 *
 * <ol>
 *   <li><b>Create Accumulator:</b> Initializes a new mutable accumulator ({@link
 *       ValidationSummaryAccumulator}).
 *   <li><b>Add Input:</b> Adds a single {@link TableValidationStats} input to the accumulator.
 *   <li><b>Merge Accumulators:</b> Merges multiple accumulators (potentially from different
 *       workers) into one.
 *   <li><b>Extract Output:</b> Produces the final {@link ValidationSummary} from the merged
 *       accumulator.
 * </ol>
 *
 * <p>This specific implementation aggregates validation results by:
 *
 * <ul>
 *   <li>Summing up total matched and mismatched row counts across all tables.
 *   <li>Collecting a list of tables that contain mismatches (joined as a string).
 *   <li>Determining the overall validation status ("MATCH" if zero mismatches, otherwise
 *       "MISMATCH").
 *   <li>Populating metadata such as Run ID, source/destination database names, and timestamps.
 * </ul>
 */
public class ValidationSummaryCombineFn
    extends Combine.CombineFn<
        TableValidationStats, ValidationSummaryAccumulator, ValidationSummary> {

  private final String runId;
  private final Instant startTimestamp;
  private final String sourceDatabase;
  private final String destinationDatabase;

  public ValidationSummaryCombineFn(
      String runId, Instant startTimestamp, String sourceDatabase, String destinationDatabase) {
    this.runId = runId;
    this.startTimestamp = startTimestamp;
    this.sourceDatabase = sourceDatabase;
    this.destinationDatabase = destinationDatabase;
  }

  @Override
  public ValidationSummaryAccumulator createAccumulator() {
    return new ValidationSummaryAccumulator();
  }

  @Override
  public ValidationSummaryAccumulator addInput(
      ValidationSummaryAccumulator accumulator, TableValidationStats input) {
    accumulator.totalTables++;
    accumulator.totalMatched += input.getMatchedRowCount();
    accumulator.totalMismatched += input.getMismatchRowCount();
    if (input.getMismatchRowCount() > 0) {
      accumulator.tablesWithMismatches.add(input.getTableName());
    }
    return accumulator;
  }

  @Override
  public ValidationSummaryAccumulator mergeAccumulators(
      Iterable<ValidationSummaryAccumulator> accumulators) {
    ValidationSummaryAccumulator merged = new ValidationSummaryAccumulator();
    for (ValidationSummaryAccumulator acc : accumulators) {
      merged.totalTables += acc.totalTables;
      merged.totalMatched += acc.totalMatched;
      merged.totalMismatched += acc.totalMismatched;
      merged.tablesWithMismatches.addAll(acc.tablesWithMismatches);
    }
    return merged;
  }

  @Override
  public ValidationSummary extractOutput(ValidationSummaryAccumulator accumulator) {
    String status = accumulator.totalMismatched == 0 ? "MATCH" : "MISMATCH";
    return ValidationSummary.builder()
        .setRunId(runId)
        .setSourceDatabase(sourceDatabase)
        .setDestinationDatabase(destinationDatabase)
        .setStatus(status)
        .setTotalTablesValidated(accumulator.totalTables)
        .setTablesWithMismatches(String.join(",", accumulator.tablesWithMismatches))
        .setTotalRowsMatched(accumulator.totalMatched)
        .setTotalRowsMismatched(accumulator.totalMismatched)
        .setStartTimestamp(startTimestamp)
        .setEndTimestamp(Instant.now())
        .build();
  }
}
