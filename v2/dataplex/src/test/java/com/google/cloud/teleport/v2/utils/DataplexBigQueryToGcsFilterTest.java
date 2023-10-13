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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.options.DataplexBigQueryToGcsOptions;
import com.google.cloud.teleport.v2.utils.BigQueryMetadataLoader.Filter;
import com.google.cloud.teleport.v2.utils.DataplexWriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.utils.DataplexWriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.TestPipeline;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DataplexBigQueryToGcsFilter}. */
@RunWith(JUnit4.class)
public class DataplexBigQueryToGcsFilterTest {
  public static final Long TS_MICROS_2021_01_01_15_00_00_UTC = 1609513200000000L;

  private DataplexBigQueryToGcsOptions options;

  @Before
  public void setUp() throws InterruptedException, IOException {
    options = TestPipeline.testingPipelineOptions().as(DataplexBigQueryToGcsOptions.class);
  }

  @Test
  public void test_whenNoFilterOptions_filterAcceptsAllTablesAndPartitions() {
    BigQueryTable.Builder t = table();
    BigQueryTablePartition p = partition().build();

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime(null);

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());

    assertThat(f.shouldSkipUnpartitionedTable(t)).isFalse();
    assertThat(f.shouldSkipPartitionedTable(t, Collections.singletonList(p))).isFalse();
    assertThat(f.shouldSkipPartition(t, p)).isFalse();
  }

  @Test
  public void test_whenTablesSet_filterExcludesTablesByName() {
    BigQueryTable.Builder includedTable1 = table().setTableName("includedTable1");
    BigQueryTable.Builder includedTable2 = table().setTableName("includedTable2");
    BigQueryTable.Builder excludedTable = table().setTableName("excludedTable");
    BigQueryTablePartition p = partition().build();

    options.setTables("includedTable1,includedTable2");
    options.setExportDataModifiedBeforeDateTime(null);

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());

    assertThat(f.shouldSkipUnpartitionedTable(includedTable1)).isFalse();
    assertThat(f.shouldSkipUnpartitionedTable(includedTable2)).isFalse();
    assertThat(f.shouldSkipUnpartitionedTable(excludedTable)).isTrue();
    assertThat(f.shouldSkipPartitionedTable(includedTable1, Collections.singletonList(p)))
        .isFalse();
    assertThat(f.shouldSkipPartitionedTable(includedTable2, Collections.singletonList(p)))
        .isFalse();
    assertThat(f.shouldSkipPartitionedTable(excludedTable, Collections.singletonList(p))).isTrue();
    assertThat(f.shouldSkipPartition(includedTable1, p)).isFalse();
    assertThat(f.shouldSkipPartition(includedTable2, p)).isFalse();
    // Should NOT skip PARTITIONS, only tables as a whole because of their name:
    assertThat(f.shouldSkipPartition(excludedTable, p)).isFalse();
  }

  @Test(expected = IllegalArgumentException.class)
  public void test_whenTablesIsInvalid_throwsException() {
    options.setTables(",");
    new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
  }

  @Test
  public void test_whenBeforeDateSet_filterExcludesTablesAndPartitions() {
    BigQueryTable.Builder olderTable =
        table().setLastModificationTime(TS_MICROS_2021_01_01_15_00_00_UTC - 1000L);
    BigQueryTable.Builder newerTable =
        table().setLastModificationTime(TS_MICROS_2021_01_01_15_00_00_UTC + 1000L);
    BigQueryTablePartition olderPartition =
        partition()
            .setPartitionName("p1")
            .setLastModificationTime(TS_MICROS_2021_01_01_15_00_00_UTC - 1000L)
            .build();
    BigQueryTablePartition newerPartition =
        partition()
            .setPartitionName("p2")
            .setLastModificationTime(TS_MICROS_2021_01_01_15_00_00_UTC + 1000L)
            .build();
    List<BigQueryTablePartition> partitions = Arrays.asList(olderPartition, newerPartition);

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime("2021-01-01T15:00:00Z");

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());

    assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isTrue();
    assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isFalse();
    // If a table is partitioned, we should filter individual partitions by modification time,
    // so the table itself should NOT be skipped no matter what the table modification time is.
    // Expecting shouldSkip = false for both newer and older tables:
    assertThat(f.shouldSkipPartitionedTable(newerTable, partitions)).isFalse();
    assertThat(f.shouldSkipPartitionedTable(olderTable, partitions)).isFalse();
    assertThat(f.shouldSkipPartition(olderTable, newerPartition)).isTrue();
    assertThat(f.shouldSkipPartition(olderTable, olderPartition)).isFalse();
  }

  @Test
  public void test_whenBeforeDateHasTimeZone_timeParsedCorrectly() {
    BigQueryTable.Builder newerTable =
        table().setLastModificationTime(TS_MICROS_2021_01_01_15_00_00_UTC - 1000L);
    BigQueryTable.Builder olderTable =
        table().setLastModificationTime(TS_MICROS_2021_01_01_15_00_00_UTC + 1000L);

    options.setTables(null);

    {
      options.setExportDataModifiedBeforeDateTime("2021-01-01T15:00:00Z");
      Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
      assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isTrue();
      assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isFalse();
    }

    {
      // Should be the same as 15:00 UTC:
      options.setExportDataModifiedBeforeDateTime("2021-01-01T14:00:00-01:00");
      Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
      assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isTrue();
      assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isFalse();
    }

    {
      // Should be the same as 15:00 UTC:
      options.setExportDataModifiedBeforeDateTime("2021-01-01T17:00:00+02:00");
      Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
      assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isTrue();
      assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isFalse();
    }

    {
      // 14:00 UTC is 1 hour is earlier that both table's last modified time
      // (14:59:59.999 and 15:00:00.001 UTC). Expecting both to be skipped.
      options.setExportDataModifiedBeforeDateTime("2021-01-01T14:00:00Z");
      Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
      assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isTrue();
      assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isTrue();
    }
  }

  @Test
  public void test_whenBeforeDateHasNoTime_dateParsedCorrectly() {
    // 2021-02-15 in the DEFAULT time zone:
    long micros = Instant.parse("2021-02-15T00:00:00").getMillis() * 1000L;

    BigQueryTable.Builder newerTable = table().setLastModificationTime(micros - 1000L);
    BigQueryTable.Builder olderTable = table().setLastModificationTime(micros + 1000L);

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime("2021-02-15");

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
    assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isTrue();
    assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isFalse();
  }

  @Test
  public void test_whenBeforeDateIs1DayDuration_dateParsedCorrectly() {
    // current time in the DEFAULT time zone minus one day:
    long micros = Instant.now().minus(Duration.standardDays(1)).getMillis() * 1000L;

    BigQueryTable.Builder olderTable = table().setLastModificationTime(micros - 100000L);
    BigQueryTable.Builder newerTable = table().setLastModificationTime(micros + 100000L);

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime("-P1D");

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
    assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isTrue();
    assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isFalse();
  }

  @Test
  public void test_whenBeforeDateIs1Day3HoursDuration_dateParsedCorrectly() {
    // current time in the DEFAULT time zone minus 1 day 3 hours:
    long micros = Instant.now().minus(Duration.millis(27 * 60 * 60 * 1000)).getMillis() * 1000L;

    BigQueryTable.Builder olderTable = table().setLastModificationTime(micros - 100000L);
    BigQueryTable.Builder newerTable = table().setLastModificationTime(micros + 100000L);

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime("-p1dt3h");

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());
    assertThat(f.shouldSkipUnpartitionedTable(newerTable)).isTrue();
    assertThat(f.shouldSkipUnpartitionedTable(olderTable)).isFalse();
  }

  @Test
  public void test_whenPartitionedTableHasNoPartitions_filterExcludesTable() {
    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime(null);

    Filter f = new DataplexBigQueryToGcsFilter(options, new ArrayList<String>());

    assertThat(f.shouldSkipPartitionedTable(table(), Collections.emptyList())).isTrue();
  }

  @Test
  public void test_whenTargetFileExistsWithWriteDispositionSKIP_filterExcludesTables() {
    BigQueryTable.Builder t = table().setTableName("table1").setPartitioningColumn("p2");
    BigQueryTablePartition p = partition().setPartitionName("partition1").build();

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime(null);
    options.setFileFormat(FileFormatOptions.AVRO);
    options.setWriteDisposition(WriteDispositionOptions.SKIP);

    Filter f =
        new DataplexBigQueryToGcsFilter(
            options,
            Arrays.asList(
                "table1/output-table1.avro", "table1/p2=partition1/output-table1-partition1.avro"));

    assertThat(f.shouldSkipUnpartitionedTable(t)).isTrue();
    assertThat(f.shouldSkipPartition(t, p)).isTrue();
  }

  @Test
  public void test_whenTargetFileExistsWithWriteDispositionOverwrite_filterAcceptsTables() {
    BigQueryTable.Builder t = table().setTableName("table1").setPartitioningColumn("p2");
    BigQueryTablePartition p = partition().setPartitionName("partition1").build();

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime(null);
    options.setFileFormat(FileFormatOptions.AVRO);
    options.setWriteDisposition(WriteDispositionOptions.OVERWRITE);

    Filter f =
        new DataplexBigQueryToGcsFilter(
            options,
            Arrays.asList(
                "table1/output-table1.avro", "table1/p2=partition1/output-table1-partition1.avro"));

    assertThat(f.shouldSkipUnpartitionedTable(t)).isFalse();
    assertThat(f.shouldSkipPartition(t, p)).isFalse();
  }

  @Test(expected = WriteDispositionException.class)
  public void test_whenTargetFileExistsWithWriteDispositionFail_filterAcceptsTables() {
    BigQueryTable.Builder t = table().setTableName("table1").setPartitioningColumn("p2");
    BigQueryTablePartition p = partition().setPartitionName("partition1").build();

    options.setTables(null);
    options.setExportDataModifiedBeforeDateTime(null);
    options.setFileFormat(FileFormatOptions.AVRO);
    options.setWriteDisposition(WriteDispositionOptions.FAIL);
    Filter f =
        new com.google.cloud.teleport.v2.utils.DataplexBigQueryToGcsFilter(
            options,
            Arrays.asList(
                "table1/output-table1.avro", "table1/p2=partition1/output-table1-partition1.avro"));

    f.shouldSkipUnpartitionedTable(t);
    f.shouldSkipPartition(t, p);
  }

  private static BigQueryTable.Builder table() {
    return BigQueryTable.builder().setTableName("defaultTableName").setLastModificationTime(0L);
  }

  private static BigQueryTablePartition.Builder partition() {
    return BigQueryTablePartition.builder()
        .setPartitionName("defaultPartitionName")
        .setLastModificationTime(0L);
  }
}
