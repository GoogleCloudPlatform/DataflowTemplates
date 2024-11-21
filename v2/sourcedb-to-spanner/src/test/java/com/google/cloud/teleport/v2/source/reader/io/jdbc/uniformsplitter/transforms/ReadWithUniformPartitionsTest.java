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
import com.google.common.collect.ImmutableList;
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
        getReadWithUniformPartitionsForTest(100L, 10L, testRangesPeek, null, null);

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
        getReadWithUniformPartitionsForTest(6L, 1L, testRangesPeek, null, null);
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
        getReadWithUniformPartitionsForTest(6L, 3L, testRangesPeek, null, null);
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
        getReadWithUniformPartitionsForTest(6L, 2L, null, firstRead, null);
    PCollection<String> firstReadRows =
        (PCollection<String>) testPipeline.apply("firstRead", readWithUniformPartitionsFirst);

    ReadWithUniformPartitions readWithUniformPartitionsSecond =
        getReadWithUniformPartitionsForTest(
            6L, 2L, null, secondRead, Wait.on(ImmutableList.of(firstReadRows)));
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
   * Floor(sqrt({@link ReadWithUniformPartitions#approxTotalRowCount()})) / 10).
   */
  @Test
  public void testMaxPartitionAutoInference() {
    // Small Row Count
    ReadWithUniformPartitions readWithUniformPartitionsSmallRowCount =
        getReadWithUniformPartitionsForTest(64L, null, null, null, null);
    // Large RoCount
    ReadWithUniformPartitions readWithUniformPartitionsLargeRowCount =
        getReadWithUniformPartitionsForTest(8_000_000_000L /* 1 billion */, null, null, null, null);
    assertThat(readWithUniformPartitionsSmallRowCount.maxPartitionsHint()).isEqualTo(1L);
    assertThat(readWithUniformPartitionsLargeRowCount.maxPartitionsHint()).isEqualTo(4472L);
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
            .setColName("wrongCol")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Range initialRange =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(42)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build()
            .withChildRange(initialRangeWithWrongColumChild, null);

    assertThrows(
        IllegalStateException.class,
        () -> getReadWithUniformPartitionsForTest(100L, null, null, initialRange, null));
  }

  @Test
  public void testLogTOBaseTwo() {
    assertThat(ReadWithUniformPartitions.logToBaseTwo(0L)).isEqualTo(0L);
    assertThat(ReadWithUniformPartitions.logToBaseTwo(1L)).isEqualTo(0L);
    assertThat(ReadWithUniformPartitions.logToBaseTwo(2_305_843_009_213_693_851L)).isEqualTo(61L);
    assertThat(ReadWithUniformPartitions.logToBaseTwo(100L)).isEqualTo(7L);
    assertThat(ReadWithUniformPartitions.logToBaseTwo(Long.MAX_VALUE)).isEqualTo(63L);
  }

  private ReadWithUniformPartitions getReadWithUniformPartitionsForTest(
      long approximateTotalCount,
      @Nullable Long maxPartitionHint,
      @Nullable TestRangesPeek testRangesPeek,
      @Nullable Range initialRange,
      Wait.OnSignal<?> waitOnSignal) {

    ReadWithUniformPartitions.Builder<String> readWithPartitionBuilder =
        ReadWithUniformPartitions.<String>builder()
            .setApproxTotalRowCount(approximateTotalCount)
            .setTableName(tableName)
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
                        .build()))
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .setDataSourceProviderFn(dataSourceProviderFn)
            .setAdditionalOperationsOnRanges(testRangesPeek)
            .setRowMapper(
                new RowMapper<String>() {
                  @Override
                  public String mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
                      throws @UnknownKeyFor @NonNull @Initialized Exception {
                    return resultSet.getString(3);
                  }
                });
    if (maxPartitionHint != null) {
      // For the purpose of this UT we disable auto adjustment as we try to verify the partitioning
      // logic.
      readWithPartitionBuilder =
          readWithPartitionBuilder
              .setMaxPartitionsHint(maxPartitionHint)
              .setAutoAdjustMaxPartitions(false);
    }
    if (waitOnSignal != null) {
      readWithPartitionBuilder = readWithPartitionBuilder.setWaitOn(waitOnSignal);
    }
    return readWithPartitionBuilder.build();
  }

  /*
   * Beam uses reflections to get PTransform Signature forcing us to make this public.
   */
  private class TestRangesPeek
      extends PTransform<PCollection<ImmutableList<Range>>, PCollection<Void>> {

    private TestRangesPeekDoFn peekFn;

    @Override
    public PCollection<Void> expand(PCollection<ImmutableList<Range>> input) {
      return input.apply(ParDo.of(peekFn));
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
    public void processElemnt(@Element ImmutableList<Range> element, OutputReceiver<Void> out) {
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
}
