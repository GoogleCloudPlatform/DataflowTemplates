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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts;

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
