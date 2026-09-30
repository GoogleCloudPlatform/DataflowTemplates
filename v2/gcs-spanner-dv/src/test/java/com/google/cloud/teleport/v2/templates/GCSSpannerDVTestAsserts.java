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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Test helper class for verifying BigQuery output from the gcs-spanner-dv pipeline.
 *
 * <p>This class provides strongly-typed Data Transfer Objects (DTOs) and assertion mappers to
 * safely compare expected validation results against the actual rows written to BigQuery.
 */
public final class GCSSpannerDVTestAsserts {

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
   * Returns the number of rows in {@code MismatchedRecords} matching all non-null filter arguments
   * ({@code null} arguments are not filtered on). Use this instead of {@link
   * #assertMismatchedRecords} at load-test scale where reading the table row by row is infeasible.
   */
  public static long countMismatchedRecords(
      BigQueryResourceManager bigQueryResourceManager,
      @Nullable String schemaName,
      @Nullable String tableName,
      @Nullable String mismatchType,
      @Nullable String shardId) {
    List<String> conditions = new ArrayList<>();
    if (schemaName != null) {
      conditions.add(String.format("schema_name = '%s'", schemaName));
    }
    if (tableName != null) {
      conditions.add(String.format("table_name = '%s'", tableName));
    }
    if (mismatchType != null) {
      conditions.add(String.format("mismatch_type = '%s'", mismatchType));
    }
    if (shardId != null) {
      conditions.add(String.format("shard_id = '%s'", shardId));
    }
    String whereClause =
        conditions.isEmpty() ? "" : " WHERE " + String.join(" AND ", conditions);
    String query =
        String.format(
            "SELECT COUNT(*) FROM `%s.%s.MismatchedRecords`%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            whereClause);
    TableResult result = bigQueryResourceManager.runQuery(query);
    return result.getValues().iterator().next().get(0).getLongValue();
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
}
