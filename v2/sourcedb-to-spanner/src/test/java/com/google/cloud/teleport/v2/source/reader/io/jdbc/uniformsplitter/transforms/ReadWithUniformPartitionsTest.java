/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ReadWithUniformPartitions}. */
@RunWith(MockitoJUnitRunner.class)
public class ReadWithUniformPartitionsTest implements Serializable {
  private static final String tableName = "test_table_read_with_uniform_partitions";

  SerializableFunction<Void, DataSource> dataSourceProviderFn =
      ignored -> TransformTestUtils.DATA_SOURCE;

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws SQLException {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
    TransformTestUtils.createDerbyTable(tableName);
  }

  @Test
  public void testReadWithUniformPartitionsPartitionTillSingleRow() throws Exception {
    // In this test we expect all the entries to split till single row. And hence each range to have
    // at most 2 count (mean being 1, there will be at most 2 rows in the partitions).
    TestRangesPeekVerification testRangesPeekVerification =
        (capturedRanges) -> {
          long totalCount = 0;
          for (Range range : capturedRanges) {
            totalCount = range.accumulateCount(totalCount);
            assertThat(range.count()).isAtMost(2L);
          }
          assertThat(totalCount).isEqualTo(6L);
        };
    TestRangesPeek testRangesPeek = new TestRangesPeek(testRangesPeekVerification);
    ReadWithUniformPartitions readWithUniformPartitions =
        getReadWithUniformPartitionsForTest(100L, 10L, testRangesPeek, null, null, null);

    PCollection<String> output =
        (PCollection<String>) testPipeline.apply(readWithUniformPartitions);
    PAssert.that(output)
        .containsInAnyOrder("Data A", "Data B", "Data C", "Data D", "Data E", "Data F");
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithUniformPartitionsPartitionSinglePartition() throws Exception {
    // In this test we expect all the entries to split to a single partition. And hence the range to
    // have a count of 6 count.
    TestRangesPeekVerification testRangesPeekVerification =
        (capturedRanges) -> {
          long totalCount = 0;
          assertThat(capturedRanges.size()).isEqualTo(1);
          for (Range range : capturedRanges) {
            totalCount = range.accumulateCount(totalCount);
            assertThat(range.count()).isEqualTo(6L);
          }
          assertThat(totalCount).isEqualTo(6L);
        };
    TestRangesPeek testRangesPeek = new TestRangesPeek(testRangesPeekVerification);

    ReadWithUniformPartitions readWithUniformPartitions =
        getReadWithUniformPartitionsForTest(6L, 1L, testRangesPeek, null, null, null);
    PCollection<String> output =
        (PCollection<String>) testPipeline.apply(readWithUniformPartitions);

    PAssert.that(output)
        .containsInAnyOrder("Data A", "Data B", "Data C", "Data D", "Data E", "Data F");
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithUniformPartitionsPartitionFewPartitions() throws Exception {
    // Here we expect the table to split int max of 3 partitions and hence each row to have a max of
    // 4 rows.
    TestRangesPeekVerification testRangesPeekVerification =
        (capturedRanges) -> {
          long totalCount = 0;
          assertThat(capturedRanges.size()).isEqualTo(3);
          for (Range range : capturedRanges) {
            totalCount = range.accumulateCount(totalCount);
            assertThat(range.count()).isAtMost(4L);
          }
          assertThat(totalCount).isEqualTo(6L);
        };
    TestRangesPeek testRangesPeek = new TestRangesPeek(testRangesPeekVerification);

    ReadWithUniformPartitions readWithUniformPartitions =
        getReadWithUniformPartitionsForTest(6L, 3L, testRangesPeek, null, null, null);
    PCollection<String> output =
        (PCollection<String>) testPipeline.apply(readWithUniformPartitions);

    PAssert.that(output)
        .containsInAnyOrder("Data A", "Data B", "Data C", "Data D", "Data E", "Data F");
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithUniformPartitionsWithWait() throws Exception {
    // We do two reads with the second-one waiting for first.

    Range firstRead =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName(tableName).build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(25)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Range secondRead = firstRead.toBuilder().setStart(10).setEnd(15).build();

    ReadWithUniformPartitions readWithUniformPartitionsFirst =
        getReadWithUniformPartitionsForTest(6L, 2L, null, firstRead, null, null);
    PCollection<String> firstReadRows =
        (PCollection<String>) testPipeline.apply("firstRead", readWithUniformPartitionsFirst);

    ReadWithUniformPartitions readWithUniformPartitionsSecond =
        getReadWithUniformPartitionsForTest(
            6L, 2L, null, secondRead, Wait.on(ImmutableList.of(firstReadRows)), null);
    PCollection<String> secondReadRows =
        (PCollection<String>) testPipeline.apply("secondRead", readWithUniformPartitionsSecond);

    var output =
        PCollectionList.of(secondReadRows).and(firstReadRows).apply(Flatten.pCollections());

    // We check that the rows read in first read appear before the rows in second read.
    PAssert.that(output)
        .satisfies(
            new SerializableFunction<Iterable<String>, Void>() {
              @Override
              public Void apply(Iterable<String> input) {
                ImmutableList.Builder<String> firstFour = ImmutableList.builder();
                ImmutableList.Builder<String> lastTwo = ImmutableList.builder();
                Iterator<String> iterator = input.iterator();
                for (int i = 0; i < 4; i++) {
                  firstFour.add(iterator.next());
                }
                while (iterator.hasNext()) {
                  lastTwo.add(iterator.next());
                }
                assertThat(firstFour.build())
                    .containsExactlyElementsIn(
                        ImmutableList.of("Data C", "Data D", "Data E", "Data F"));
                assertThat(lastTwo.build())
                    .containsExactlyElementsIn(ImmutableList.of("Data A", "Data B"));
                return null;
              }
            });
    testPipeline.run().waitUntilFinish();
  }

  /**
   * Test Auto-Inference for maxPartition. The AutoInference sets the default to MAx(1,
   * Floor(sqrt({@link TableSplitSpecification#approxRowCount()})) / 20).
   */
  @Test
  public void testMaxPartitionAutoInference() {
    // Small Row Count
    ReadWithUniformPartitions readWithUniformPartitionsSmallRowCount =
        getReadWithUniformPartitionsForTest(64L, null, null, null, null, null);
    // Large RoCount
    ReadWithUniformPartitions readWithUniformPartitionsLargeRowCount =
        getReadWithUniformPartitionsForTest(
            8_000_000_000L /* 8 billion */, null, null, null, null, null);
    assertThat(
            ((TableSplitSpecification)
                    (readWithUniformPartitionsSmallRowCount.tableSplitSpecifications().get(0)))
                .maxPartitionsHint())
        .isEqualTo(1L);
    assertThat(
            ((TableSplitSpecification)
                    (readWithUniformPartitionsLargeRowCount.tableSplitSpecifications().get(0)))
                .maxPartitionsHint())
        .isEqualTo(4472L);
  }

  @Test
  public void testBuildJdbc() {
    JdbcIO.ReadAll mockReadAll = mock(JdbcIO.ReadAll.class);
    String testQuery = "Select *";
    JdbcIO.PreparedStatementSetter<Range> mockRangePrepareator =
        mock(JdbcIO.PreparedStatementSetter.class);
    SerializableFunction<Void, DataSource> mockDataSourceProviderFn =
        mock(SerializableFunction.class);
    JdbcIO.RowMapper mockRowMapper = mock(RowMapper.class);
    Integer testFetchSize = 42;

    when(mockReadAll.withQuery(testQuery)).thenReturn(mockReadAll);
    when(mockReadAll.withParameterSetter(mockRangePrepareator)).thenReturn(mockReadAll);
    when(mockReadAll.withDataSourceProviderFn(mockDataSourceProviderFn)).thenReturn(mockReadAll);
    when(mockReadAll.withOutputParallelization(false)).thenReturn(mockReadAll);
    when(mockReadAll.withRowMapper(mockRowMapper)).thenReturn(mockReadAll);
    when(mockReadAll.withFetchSize(testFetchSize)).thenReturn(mockReadAll);

    ReadWithUniformPartitions.buildJdbcIO(
        mockReadAll,
        testQuery,
        mockRangePrepareator,
        mockDataSourceProviderFn,
        mockRowMapper,
        null);
    // No fetch size set.
    verify(mockReadAll, never()).withFetchSize(anyInt());
    ReadWithUniformPartitions.buildJdbcIO(
        mockReadAll,
        testQuery,
        mockRangePrepareator,
        mockDataSourceProviderFn,
        mockRowMapper,
        testFetchSize);
    verify(mockReadAll, times(1)).withFetchSize(testFetchSize);
    verify(mockReadAll, times(2)).withQuery(testQuery);
    verify(mockReadAll, times(2)).withParameterSetter(mockRangePrepareator);
    verify(mockReadAll, times(2)).withDataSourceProviderFn(mockDataSourceProviderFn);
    verify(mockReadAll, times(2)).withOutputParallelization(false);
    verify(mockReadAll, times(2)).withRowMapper(mockRowMapper);
  }

  @Test
  public void testMaxPartitionAutoInferencePreConditions() {
    Range initialRangeWithWrongColumChild =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName(tableName).build())
            .setColName("wrongCol")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Range initialRange =
        Range.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName(tableName).build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(42)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build()
            .withChildRange(initialRangeWithWrongColumChild, null);

    assertThrows(
        IllegalStateException.class,
        () -> getReadWithUniformPartitionsForTest(100L, null, null, initialRange, null, null));
  }

  @Test
  public void testLogTOBaseTwo() {
    assertThat(TableSplitSpecification.logToBaseTwo(0L)).isEqualTo(0L);
    assertThat(TableSplitSpecification.logToBaseTwo(1L)).isEqualTo(0L);
    assertThat(TableSplitSpecification.logToBaseTwo(2_305_843_009_213_693_851L)).isEqualTo(61L);
    assertThat(TableSplitSpecification.logToBaseTwo(100L)).isEqualTo(7L);
    assertThat(TableSplitSpecification.logToBaseTwo(Long.MAX_VALUE)).isEqualTo(63L);
  }

  private ReadWithUniformPartitions getReadWithUniformPartitionsForTest(
      long approximateTotalCount,
      @Nullable Long maxPartitionHint,
      @Nullable TestRangesPeek testRangesPeek,
      @Nullable Range initialRange,
      Wait.OnSignal<?> waitOnSignal,
      @Nullable String tableName) {

    if (tableName == null) {
      tableName = ReadWithUniformPartitionsTest.tableName;
    }

    TableIdentifier tableIdentifier = TableIdentifier.builder().setTableName(tableName).build();

    TableSplitSpecification.Builder specBuilder =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableIdentifier)
            .setApproxRowCount(approximateTotalCount)
            .setInitialRange(initialRange)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("col2")
                        .setColumnClass(Integer.class)
                        .build()));

    if (maxPartitionHint != null) {
      specBuilder.setMaxPartitionsHint(maxPartitionHint);
    }

    TableReadSpecification<String> readSpec =
        TableReadSpecification.<String>builder()
            .setTableIdentifier(tableIdentifier)
            .setRowMapper(
                new RowMapper<String>() {
                  @Override
                  public String mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
                      throws @UnknownKeyFor @NonNull @Initialized Exception {
                    return resultSet.getString(3);
                  }
                })
            .build();

    ReadWithUniformPartitions.Builder<String> readWithPartitionBuilder =
        ReadWithUniformPartitions.<String>builder()
            .setTableSplitSpecifications(ImmutableList.of(specBuilder.build()))
            .setTableReadSpecifications(ImmutableMap.of(tableIdentifier, readSpec))
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .setDataSourceProviderFn(dataSourceProviderFn)
            .setAdditionalOperationsOnRanges(testRangesPeek);
    if (maxPartitionHint != null) {
      // For the purpose of this UT we disable auto adjustment as we try to verify the partitioning
      // logic.
      readWithPartitionBuilder = readWithPartitionBuilder.setAutoAdjustMaxPartitions(false);
    }
    if (waitOnSignal != null) {
      readWithPartitionBuilder = readWithPartitionBuilder.setWaitOn(waitOnSignal);
    }
    return readWithPartitionBuilder.build();
  }

  private static class UnKeyRangesDoFn
      extends DoFn<KV<Integer, ImmutableList<Range>>, ImmutableList<Range>> {
    @ProcessElement
    public void processElement(
        @Element KV<Integer, ImmutableList<Range>> element,
        OutputReceiver<ImmutableList<Range>> out) {
      out.output(element.getValue());
    }
  }

  /*
   * Beam uses reflections to get PTransform Signature forcing us to make this public.
   */
  private class TestRangesPeek
      extends PTransform<PCollection<KV<Integer, ImmutableList<Range>>>, PCollection<Void>> {

    private TestRangesPeekDoFn peekFn;

    @Override
    public PCollection<Void> expand(PCollection<KV<Integer, ImmutableList<Range>>> input) {
      return input.apply(ParDo.of(new UnKeyRangesDoFn())).apply(ParDo.of(peekFn));
    }

    public TestRangesPeek(TestRangesPeekVerification verification) {
      this.peekFn = new TestRangesPeekDoFn(verification);
    }
  }

  /*
   * Beam uses reflections to get DoFn signature forcing us to make this public.
   */
  public class TestRangesPeekDoFn extends DoFn<ImmutableList<Range>, Void> {
    TestRangesPeekVerification verification;
    boolean calledOnce = false;

    @ProcessElement
    public void processElement(@Element ImmutableList<Range> element, OutputReceiver<Void> out) {
      assertThat(calledOnce).isFalse();
      calledOnce = true;
      verification.verifyRanges(element);
      out.output(null);
    }

    public TestRangesPeekDoFn(TestRangesPeekVerification verification) {
      this.verification = verification;
    }
  }

  public interface TestRangesPeekVerification extends Serializable {
    void verifyRanges(ImmutableList<Range> capturedRanges);
  }

  @AfterClass
  public static void exitDerby() throws SQLException {
    TransformTestUtils.dropDerbyTable(tableName);
  }

  @Test
  public void testEndToEndWithTwoTables() throws SQLException {
    String tableName1 = "test_table_1";
    String tableName2 = "test_table_2";
    TransformTestUtils.createDerbyTable(tableName1);
    TransformTestUtils.createDerbyTable(tableName2);

    try {
      // Table 1: 6 rows
      TestRangesPeekVerification verification1 =
          (capturedRanges) -> {
            long totalCount = 0;
            for (Range range : capturedRanges) {
              totalCount = range.accumulateCount(totalCount);
            }
          };
      TestRangesPeek testRangesPeek1 = new TestRangesPeek(verification1);

      ReadWithUniformPartitions<String> readWithUniformPartitions1 =
          getReadWithUniformPartitionsForTest(100L, 10L, testRangesPeek1, null, null, tableName1);

      PCollection<String> output1 =
          (PCollection<String>) testPipeline.apply("ReadTable1", readWithUniformPartitions1);

      // Table 2: 3 rows
      // Clear table2 before inserting new values.
      try (java.sql.Connection conn = dataSourceProviderFn.apply(null).getConnection();
          java.sql.Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DELETE FROM " + tableName2);
      }
      TransformTestUtils.insertValuesIntoTable(
          tableName2, ImmutableList.of(1, 2, 3), ImmutableList.of("X", "Y", "Z"));

      TestRangesPeekVerification verification2 =
          (capturedRanges) -> {
            long totalCount = 0;
            for (Range range : capturedRanges) {
              totalCount = range.accumulateCount(totalCount);
            }
          };
      TestRangesPeek testRangesPeek2 = new TestRangesPeek(verification2);

      ReadWithUniformPartitions<String> readWithUniformPartitions2 =
          getReadWithUniformPartitionsForTest(100L, 10L, testRangesPeek2, null, null, tableName2);

      PCollection<String> output2 =
          (PCollection<String>) testPipeline.apply("ReadTable2", readWithUniformPartitions2);

      PAssert.that(output1)
          .containsInAnyOrder("Data A", "Data B", "Data C", "Data D", "Data E", "Data F");
      PAssert.that(output2).containsInAnyOrder("X", "Y", "Z");

      testPipeline.run().waitUntilFinish();
    } finally {
      TransformTestUtils.dropDerbyTable(tableName1);
      TransformTestUtils.dropDerbyTable(tableName2);
    }
  }

  @Test
  public void testGetCollationReferencesDeduplicates() {
    // Arrange
    CollationReference collation1 =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_unicode_ci")
            .setPadSpace(true)
            .build();

    CollationReference collation2 =
        CollationReference.builder()
            .setDbCharacterSet("latin1")
            .setDbCollation("latin1_swedish_ci")
            .setPadSpace(false)
            .build();

    CollationReference collation1Dup =
        CollationReference.builder()
            .setDbCharacterSet("utf8mb4")
            .setDbCollation("utf8mb4_unicode_ci")
            .setPadSpace(true)
            .build();

    TableSplitSpecification spec1 =
        TableSplitSpecification.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("table1").build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("id")
                        .setColumnClass(Long.class)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("name")
                        .setColumnClass(String.class)
                        .setStringMaxLength(256)
                        .setStringCollation(collation1)
                        .build()))
            .setApproxRowCount(100L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();

    TableSplitSpecification spec2 =
        TableSplitSpecification.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("table2").build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("address")
                        .setColumnClass(String.class)
                        .setStringMaxLength(256)
                        .setStringCollation(collation2)
                        .build(),
                    PartitionColumn.builder()
                        .setColumnName("comment")
                        .setColumnClass(String.class)
                        .setStringMaxLength(256)
                        .setStringCollation(collation1Dup) // Duplicate of collation1
                        .build()))
            .setApproxRowCount(100L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(1L)
            .build();

    TableSplitSpecification spec3 =
        TableSplitSpecification.builder()
            .setTableIdentifier(TableIdentifier.builder().setTableName("table3").build())
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("id")
                        .setColumnClass(Long.class)
                        .build())) // No string collation
            .setApproxRowCount(100L)
            .setInitialSplitHeight(1L)
            .setSplitStagesCount(1L)
            .build();

    ImmutableList<TableSplitSpecification> specs = ImmutableList.of(spec1, spec2, spec3);

    // Act
    ImmutableList<CollationReference> result =
        ReadWithUniformPartitions.getCollationReferences(specs);

    // Assert
    assertThat(result).containsExactlyElementsIn(ImmutableList.of(collation1, collation2));
    assertThat(result).hasSize(2);
  }

  @Test
  public void testReadWithUniformPartitionsTwoTables() throws SQLException {
    String tableName1 = "test_table_1_combined";
    String tableName2 = "test_table_2_combined";
    TransformTestUtils.createDerbyTable(tableName1);
    TransformTestUtils.createDerbyTable(tableName2);

    try {
      TableSplitSpecification spec1 =
          TableSplitSpecification.builder()
              .setTableIdentifier(TableIdentifier.builder().setTableName(tableName1).build())
              .setApproxRowCount(100L)
              .setPartitionColumns(
                  ImmutableList.of(
                      PartitionColumn.builder()
                          .setColumnName("col1")
                          .setColumnClass(Integer.class)
                          .build(),
                      PartitionColumn.builder()
                          .setColumnName("col2")
                          .setColumnClass(Integer.class)
                          .build()))
              .build();

      // Clear table2 before inserting new values.
      try (java.sql.Connection conn = dataSourceProviderFn.apply(null).getConnection();
          java.sql.Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DELETE FROM " + tableName2);
      }
      TransformTestUtils.insertValuesIntoTable(
          tableName2, ImmutableList.of(1, 2, 3), ImmutableList.of("X", "Y", "Z"));

      TableSplitSpecification spec2 =
          TableSplitSpecification.builder()
              .setTableIdentifier(TableIdentifier.builder().setTableName(tableName2).build())
              .setApproxRowCount(100L)
              .setPartitionColumns(
                  ImmutableList.of(
                      PartitionColumn.builder()
                          .setColumnName("col1")
                          .setColumnClass(Integer.class)
                          .build(),
                      PartitionColumn.builder()
                          .setColumnName("col2")
                          .setColumnClass(Integer.class)
                          .build()))
              .build();

      TableReadSpecification<String> readSpec1 =
          TableReadSpecification.<String>builder()
              .setTableIdentifier(spec1.tableIdentifier())
              .setRowMapper(
                  new RowMapper<String>() {
                    @Override
                    public String mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
                        throws @UnknownKeyFor @NonNull @Initialized Exception {
                      return resultSet.getString(3);
                    }
                  })
              .build();

      TableReadSpecification<String> readSpec2 =
          TableReadSpecification.<String>builder()
              .setTableIdentifier(spec2.tableIdentifier())
              .setRowMapper(
                  new RowMapper<String>() {
                    @Override
                    public String mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
                        throws @UnknownKeyFor @NonNull @Initialized Exception {
                      return resultSet.getString(3);
                    }
                  })
              .build();

      ReadWithUniformPartitions<String> readWithUniformPartitions =
          ReadWithUniformPartitions.<String>builder()
              .setTableSplitSpecifications(ImmutableList.of(spec1, spec2))
              .setTableReadSpecifications(
                  ImmutableMap.of(
                      spec1.tableIdentifier(), readSpec1, spec2.tableIdentifier(), readSpec2))
              .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
              .setDataSourceProviderFn(dataSourceProviderFn)
              .build();

      PCollection<String> output =
          (PCollection<String>) testPipeline.apply(readWithUniformPartitions);

      PAssert.that(output)
          .containsInAnyOrder(
              "Data A", "Data B", "Data C", "Data D", "Data E", "Data F", "X", "Y", "Z");

      testPipeline.run().waitUntilFinish();
    } finally {
      TransformTestUtils.dropDerbyTable(tableName1);
      TransformTestUtils.dropDerbyTable(tableName2);
    }
  }
}
