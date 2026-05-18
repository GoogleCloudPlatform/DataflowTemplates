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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.DataSourceProviderImpl;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeCountTransform}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeCountTransformTest {
  private static final String tableName = "test_table_range_counter";

  SerializableFunction<Void, DataSource> dataSourceProviderFn =
      ignored -> TransformTestUtils.DATA_SOURCE;

  SerializableFunction<Void, DataSource> dataSourceProviderFnShard2 =
      ignored -> TransformTestUtils.DATA_SOURCE_SHARD_2;

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws SQLException {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
    TransformTestUtils.createDerbyTable(tableName);
    TransformTestUtils.createDerbyTable("RCT_multi_shard1");
    TransformTestUtils.createDerbyTableShard2("RCT_multi_shard2");
  }

  @AfterClass
  public static void exitDerby() throws SQLException {
    TransformTestUtils.dropDerbyTable(tableName);
    TransformTestUtils.dropDerbyTable("RCT_multi_shard1");
    try (java.sql.Connection connection = TransformTestUtils.getConnectionShard2()) {
      java.sql.Statement statement = connection.createStatement();
      statement.executeUpdate("drop table RCT_multi_shard2");
    }
  }

  @Test
  public void testRangeCountTransform_multiShard() throws Exception {
    String shard1Id = "shard1";
    String shard2Id = "shard2";
    String table1Name = "RCT_multi_shard1";
    String table2Name = "RCT_multi_shard2";

    Range range1 =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId(shard1Id)
                    .setTableName(table1Name)
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(10)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    Range range2 =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId(shard2Id)
                    .setTableName(table2Name)
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(10)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    RangeCountTransform transform =
        RangeCountTransform.builder()
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .setTableSplitSpecifications(
                ImmutableList.of(
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder()
                                .setDataSourceId(shard1Id)
                                .setTableName(table1Name)
                                .build())
                        .setPartitionColumns(
                            ImmutableList.of(
                                PartitionColumn.builder()
                                    .setColumnName("col1")
                                    .setColumnClass(Integer.class)
                                    .build()))
                        .setApproxRowCount(100L)
                        .setMaxPartitionsHint(10L)
                        .setInitialSplitHeight(5L)
                        .setSplitStagesCount(1L)
                        .build(),
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder()
                                .setDataSourceId(shard2Id)
                                .setTableName(table2Name)
                                .build())
                        .setPartitionColumns(
                            ImmutableList.of(
                                PartitionColumn.builder()
                                    .setColumnName("col1")
                                    .setColumnClass(Integer.class)
                                    .build()))
                        .setApproxRowCount(100L)
                        .setMaxPartitionsHint(10L)
                        .setInitialSplitHeight(5L)
                        .setSplitStagesCount(1L)
                        .build()))
            .setDataSourceProvider(
                DataSourceProviderImpl.builder()
                    .addDataSource(shard1Id, dataSourceProviderFn)
                    .addDataSource(shard2Id, dataSourceProviderFnShard2)
                    .build())
            .setTimeoutMillis(42L)
            .build();

    PCollection<Range> output = testPipeline.apply(Create.of(range1, range2)).apply(transform);

    // Both tables have 6 rows in TransformTestUtils
    PAssert.that(output).containsInAnyOrder(range1.withCount(6L, null), range2.withCount(6L, null));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testRangeCountTransform() throws Exception {
    ImmutableList<String> partitionCols = ImmutableList.of("col1", "col2");
    Range singleColNonLastRange =
        Range.<Integer>builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName(tableName)
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(25)
            .setIsLast(false)
            .build();
    Range bothColRange =
        Range.<Integer>builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName(tableName)
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(11)
            .setIsLast(false)
            .build()
            .withChildRange(
                Range.<Integer>builder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName(tableName)
                            .build())
                    .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                    .setColName("col2")
                    .setColClass(Integer.class)
                    .setStart(30)
                    .setEnd(40)
                    .build(),
                null);
    // Create a test pipeline.
    PCollection<Range> input = testPipeline.apply(Create.of(singleColNonLastRange, bothColRange));
    RangeCountTransform rangeCountTransform =
        RangeCountTransform.builder()
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .setTableSplitSpecifications(
                ImmutableList.of(
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder()
                                .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                                .setTableName(tableName)
                                .build())
                        .setPartitionColumns(
                            partitionCols.stream()
                                .map(
                                    c ->
                                        PartitionColumn.builder()
                                            .setColumnName(c)
                                            .setColumnClass(Long.class)
                                            .build())
                                .collect(ImmutableList.toImmutableList()))
                        .setApproxRowCount(100L)
                        .setMaxPartitionsHint(10L)
                        .setInitialSplitHeight(5L)
                        .setSplitStagesCount(1L)
                        .build()))
            .setDataSourceProvider(
                DataSourceProviderImpl.builder()
                    .addDataSource("b1a1ec3b-195d-4755-b04b-02bc64dc4458", dataSourceProviderFn)
                    .build())
            .setTimeoutMillis(42L)
            .build();
    PCollection<Range> output = input.apply(rangeCountTransform);

    PAssert.that(output)
        .containsInAnyOrder(
            singleColNonLastRange.withCount(2L, null), bothColRange.withCount(1L, null));

    testPipeline.run().waitUntilFinish();
  }
}
