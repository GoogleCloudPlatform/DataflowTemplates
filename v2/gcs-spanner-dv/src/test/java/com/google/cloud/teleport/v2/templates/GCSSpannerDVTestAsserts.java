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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.google.cloud.bigquery.TableResult;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test helper class for verifying BigQuery output from the gcs-spanner-dv pipeline.
 *
 * <p>This class provides strongly-typed Data Transfer Objects (DTOs) and assertion mappers to
 * safely compare expected validation results against the actual rows written to BigQuery.
 */
public final class GCSSpannerDVTestAsserts {

  private static final Logger LOG = LoggerFactory.getLogger(GCSSpannerDVTestAsserts.class);

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private GCSSpannerDVTestAsserts() {}

  private static <T> void assertTableRecords(
      BigQueryResourceManager bigQueryResourceManager,
      String tableName,
      Class<T> dtoClass,
      List<T> expected) {
    TableResult result = bigQueryResourceManager.readTable(tableName);
    List<Map<String, Object>> rows = BigQueryAsserts.tableResultToRecords(result);

    List<T> dtos =
        rows.stream().map(row -> MAPPER.convertValue(row, dtoClass)).collect(Collectors.toList());

    assertThat(dtos).containsExactlyElementsIn(expected);
  }

  public static void assertValidationSummary(
      BigQueryResourceManager bigQueryResourceManager, List<ValidationSummaryDto> expected) {
    assertTableRecords(
        bigQueryResourceManager, "ValidationSummary", ValidationSummaryDto.class, expected);
  }

  public static void assertTableValidationStats(
      BigQueryResourceManager bigQueryResourceManager, List<TableValidationStatsDto> expected) {
    assertTableRecords(
        bigQueryResourceManager, "TableValidationStats", TableValidationStatsDto.class, expected);
  }

  public static void assertMismatchedRecords(
      BigQueryResourceManager bigQueryResourceManager, List<MismatchedRecordDto> expected) {
    assertTableRecords(
        bigQueryResourceManager, "MismatchedRecords", MismatchedRecordDto.class, expected);
  }

  /**
   * Reads the MismatchedRecords table aggregated by (table, mismatch type, shard) and logs every
   * group. Use this instead of {@link #assertMismatchedRecords} when the table is too large to read
   * in full (for example in load tests). A NULL {@code shard_id} maps to a {@code null} {@link
   * MismatchedRecordCountDto#shardId()}.
   */
  public static List<MismatchedRecordCountDto> readMismatchedRecordCounts(
      BigQueryResourceManager bigQueryResourceManager) {
    String query =
        String.format(
            "SELECT TO_JSON_STRING(t) FROM (SELECT table_name, mismatch_type, shard_id,"
                + " COUNT(*) AS record_count FROM `%s.%s.MismatchedRecords`"
                + " GROUP BY table_name, mismatch_type, shard_id) AS t",
            bigQueryResourceManager.getProjectId(), bigQueryResourceManager.getDatasetId());
    LOG.info("[DV-LT] Running MismatchedRecords aggregate query: {}", query);
    TableResult result = bigQueryResourceManager.runQuery(query);
    List<MismatchedRecordCountDto> counts =
        BigQueryAsserts.tableResultToRecords(result).stream()
            .map(row -> MAPPER.convertValue(row, MismatchedRecordCountDto.class))
            .sorted(
                Comparator.comparing(
                        MismatchedRecordCountDto::tableName,
                        Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(
                        MismatchedRecordCountDto::mismatchType,
                        Comparator.nullsFirst(Comparator.naturalOrder()))
                    .thenComparing(
                        MismatchedRecordCountDto::shardId,
                        Comparator.nullsFirst(Comparator.naturalOrder())))
            .collect(Collectors.toList());
    LOG.info("[DV-LT] MismatchedRecords aggregate returned {} group(s)", counts.size());
    counts.forEach(c -> LOG.info("[DV-LT] MismatchedRecords group: {}", c));
    return counts;
  }

  /** Logs every row of a (small) BigQuery output table. Used for debugging load test runs. */
  public static void logTableRows(BigQueryResourceManager bigQueryResourceManager, String table) {
    try {
      List<Map<String, Object>> rows =
          BigQueryAsserts.tableResultToRecords(bigQueryResourceManager.readTable(table));
      LOG.info("[DV-LT] BigQuery table {} has {} row(s)", table, rows.size());
      rows.forEach(row -> LOG.info("[DV-LT] {} row: {}", table, row));
    } catch (Exception e) {
      LOG.warn("[DV-LT] Failed to read BigQuery table {} for logging", table, e);
    }
  }

  /**
   * These DTOs contain only the core columns necessary to assert the functional correctness of the
   * validation pipeline. Transient or dynamic fields (such as `run_id`) that are not strictly
   * required to verify the core logic should be intentionally excluded. This principle should serve
   * as the decision criteria before adding any new fields to these DTOs.
   */
  public record ValidationSummaryDto(
      String status,
      Long totalTablesValidated,
      Long totalRowsMatched,
      Long totalRowsMismatched,
      String tablesWithMismatches) {}

  public record TableValidationStatsDto(
      String schemaName,
      String tableName,
      String status,
      Long sourceRowCount,
      Long destinationRowCount,
      Long matchedRowCount,
      Long mismatchRowCount) {}

  public record MismatchedRecordDto(
      String shardId, String schemaName, String tableName, String recordKey, String mismatchType) {}

  /** One group of {@link #readMismatchedRecordCounts}: row count per table, type and shard. */
  public record MismatchedRecordCountDto(
      String tableName, String mismatchType, String shardId, Long recordCount) {}
}
