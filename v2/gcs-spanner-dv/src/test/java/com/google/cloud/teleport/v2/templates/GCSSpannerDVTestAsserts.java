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

import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.v2.dto.MismatchedRecord;
import com.google.cloud.teleport.v2.dto.TableValidationStats;
import com.google.cloud.teleport.v2.dto.ValidationSummary;
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

  private GCSSpannerDVTestAsserts() {}

  public static void assertValidationSummary(
      BigQueryResourceManager bigQueryResourceManager, List<ValidationSummaryDto> expected) {
    assertThat(bigQueryResourceManager.getRowCount("ValidationSummary"))
        .isEqualTo((long) expected.size());
    TableResult summaryResult = bigQueryResourceManager.readTable("ValidationSummary");
    List<Map<String, Object>> summaryRows = BigQueryAsserts.tableResultToRecords(summaryResult);
    List<ValidationSummaryDto> summaryDtos =
        summaryRows.stream()
            .map(
                row ->
                    new ValidationSummaryDto(
                        (String) row.get(ValidationSummary.STATUS_COLUMN_NAME),
                        ((Number) row.get(ValidationSummary.TOTAL_TABLES_VALIDATED_COLUMN_NAME))
                            .longValue(),
                        ((Number) row.get(ValidationSummary.TOTAL_ROWS_MATCHED_COLUMN_NAME))
                            .longValue(),
                        ((Number) row.get(ValidationSummary.TOTAL_ROWS_MISMATCHED_COLUMN_NAME))
                            .longValue(),
                        (String) row.get(ValidationSummary.TABLES_WITH_MISMATCHES_COLUMN_NAME)))
            .collect(Collectors.toList());

    assertThat(summaryDtos).containsExactlyElementsIn(expected);
  }

  public static void assertTableValidationStats(
      BigQueryResourceManager bigQueryResourceManager, List<TableValidationStatsDto> expected) {
    assertThat(bigQueryResourceManager.getRowCount("TableValidationStats"))
        .isEqualTo((long) expected.size());
    TableResult statsResult = bigQueryResourceManager.readTable("TableValidationStats");
    List<Map<String, Object>> statsRows = BigQueryAsserts.tableResultToRecords(statsResult);

    List<TableValidationStatsDto> statsDtos =
        statsRows.stream()
            .map(
                row ->
                    new TableValidationStatsDto(
                        (String) row.get(TableValidationStats.SCHEMA_NAME),
                        row.get(TableValidationStats.TABLE_NAME_COLUMN_NAME).toString(),
                        (String) row.get(TableValidationStats.STATUS_COLUMN_NAME),
                        ((Number) row.get(TableValidationStats.SOURCE_ROW_COUNT_COLUMN_NAME))
                            .longValue(),
                        ((Number) row.get(TableValidationStats.DESTINATION_ROW_COUNT_COLUMN_NAME))
                            .longValue(),
                        ((Number) row.get(TableValidationStats.MATCHED_ROW_COUNT_COLUMN_NAME))
                            .longValue(),
                        ((Number) row.get(TableValidationStats.MISMATCH_ROW_COUNT_COLUMN_NAME))
                            .longValue()))
            .collect(Collectors.toList());

    assertThat(statsDtos).containsExactlyElementsIn(expected);
  }

  public static void assertMismatchedRecords(
      BigQueryResourceManager bigQueryResourceManager, List<MismatchedRecordDto> expected) {
    assertThat(bigQueryResourceManager.getRowCount("MismatchedRecords"))
        .isEqualTo((long) expected.size());
    TableResult mismatchesResult = bigQueryResourceManager.readTable("MismatchedRecords");
    List<Map<String, Object>> mismatchRows = BigQueryAsserts.tableResultToRecords(mismatchesResult);

    List<MismatchedRecordDto> mismatchDtos =
        mismatchRows.stream()
            .map(
                row ->
                    new MismatchedRecordDto(
                        (String) row.get(MismatchedRecord.SHARD_ID_COLUMN_NAME),
                        (String) row.get(MismatchedRecord.SCHEMA_NAME),
                        (String) row.get(MismatchedRecord.TABLE_NAME_COLUMN_NAME),
                        row.get(MismatchedRecord.RECORD_KEY_COLUMN_NAME)
                            .toString()
                            .replaceAll("\\s+", ""),
                        row.get(MismatchedRecord.MISMATCH_TYPE_COLUMN_NAME).toString()))
            .collect(Collectors.toList());

    assertThat(mismatchDtos).containsExactlyElementsIn(expected);
  }

  public static class ValidationSummaryDto {
    public final String status;
    public final Long totalTablesValidated;
    public final Long totalRowsMatched;
    public final Long totalRowsMismatched;
    public final String tablesWithMismatches;

    public ValidationSummaryDto(
        String status,
        Long totalTablesValidated,
        Long totalRowsMatched,
        Long totalRowsMismatched,
        String tablesWithMismatches) {
      this.status = status;
      this.totalTablesValidated = totalTablesValidated;
      this.totalRowsMatched = totalRowsMatched;
      this.totalRowsMismatched = totalRowsMismatched;
      this.tablesWithMismatches = tablesWithMismatches;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ValidationSummaryDto that = (ValidationSummaryDto) o;
      return java.util.Objects.equals(status, that.status)
          && java.util.Objects.equals(totalTablesValidated, that.totalTablesValidated)
          && java.util.Objects.equals(totalRowsMatched, that.totalRowsMatched)
          && java.util.Objects.equals(totalRowsMismatched, that.totalRowsMismatched)
          && java.util.Objects.equals(tablesWithMismatches, that.tablesWithMismatches);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(
          status,
          totalTablesValidated,
          totalRowsMatched,
          totalRowsMismatched,
          tablesWithMismatches);
    }

    @Override
    public String toString() {
      return "ValidationSummaryDto{"
          + "status='"
          + status
          + '\''
          + ", totalTablesValidated="
          + totalTablesValidated
          + ", totalRowsMatched="
          + totalRowsMatched
          + ", totalRowsMismatched="
          + totalRowsMismatched
          + ", tablesWithMismatches='"
          + tablesWithMismatches
          + '\''
          + '}';
    }
  }

  public static class TableValidationStatsDto {
    public final String schemaName;
    public final String tableName;
    public final String status;
    public final Long sourceRowCount;
    public final Long destinationRowCount;
    public final Long matchedRowCount;
    public final Long mismatchRowCount;

    public TableValidationStatsDto(
        String schemaName,
        String tableName,
        String status,
        Long sourceRowCount,
        Long destinationRowCount,
        Long matchedRowCount,
        Long mismatchRowCount) {
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.status = status;
      this.sourceRowCount = sourceRowCount;
      this.destinationRowCount = destinationRowCount;
      this.matchedRowCount = matchedRowCount;
      this.mismatchRowCount = mismatchRowCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TableValidationStatsDto that = (TableValidationStatsDto) o;
      return java.util.Objects.equals(schemaName, that.schemaName)
          && java.util.Objects.equals(tableName, that.tableName)
          && java.util.Objects.equals(status, that.status)
          && java.util.Objects.equals(sourceRowCount, that.sourceRowCount)
          && java.util.Objects.equals(destinationRowCount, that.destinationRowCount)
          && java.util.Objects.equals(matchedRowCount, that.matchedRowCount)
          && java.util.Objects.equals(mismatchRowCount, that.mismatchRowCount);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(
          schemaName,
          tableName,
          status,
          sourceRowCount,
          destinationRowCount,
          matchedRowCount,
          mismatchRowCount);
    }

    @Override
    public String toString() {
      return "TableValidationStatsDto{"
          + "schemaName='"
          + schemaName
          + '\''
          + ", tableName='"
          + tableName
          + '\''
          + ", status='"
          + status
          + '\''
          + ", sourceRowCount="
          + sourceRowCount
          + ", destinationRowCount="
          + destinationRowCount
          + ", matchedRowCount="
          + matchedRowCount
          + ", mismatchRowCount="
          + mismatchRowCount
          + '}';
    }
  }

  public static class MismatchedRecordDto {
    public final String shardId;
    public final String schemaName;
    public final String tableName;
    public final String recordKey;
    public final String mismatchType;

    public MismatchedRecordDto(
        String shardId,
        String schemaName,
        String tableName,
        String recordKey,
        String mismatchType) {
      this.shardId = shardId;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.recordKey = recordKey;
      this.mismatchType = mismatchType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MismatchedRecordDto that = (MismatchedRecordDto) o;
      return java.util.Objects.equals(shardId, that.shardId)
          && java.util.Objects.equals(schemaName, that.schemaName)
          && java.util.Objects.equals(tableName, that.tableName)
          && java.util.Objects.equals(recordKey, that.recordKey)
          && java.util.Objects.equals(mismatchType, that.mismatchType);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(shardId, schemaName, tableName, recordKey, mismatchType);
    }

    @Override
    public String toString() {
      return "MismatchedRecordDto{"
          + "shardId='"
          + shardId
          + '\''
          + ", schemaName='"
          + schemaName
          + '\''
          + ", tableName='"
          + tableName
          + '\''
          + ", recordKey='"
          + recordKey
          + '\''
          + ", mismatchType='"
          + mismatchType
          + '\''
          + '}';
    }
  }
}
