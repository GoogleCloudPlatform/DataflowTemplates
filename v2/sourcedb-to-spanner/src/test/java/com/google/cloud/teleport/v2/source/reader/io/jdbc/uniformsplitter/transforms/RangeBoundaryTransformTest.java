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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
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

/** Test class for {@link RangeBoundaryTransform}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeBoundaryTransformTest {

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
    TransformTestUtils.createDerbyTable("RBT_table1");
    TransformTestUtils.createDerbyTable("RBT_table2");
    TransformTestUtils.createDerbyTable("RBT_multi_shard1");
    TransformTestUtils.createDerbyTableShard2("RBT_multi_shard2");
  }

  @Test
  public void testRangeBoundaryTransform_multiShard() throws Exception {
    String shard1Id = "shard1";
    String shard2Id = "shard2";
    String table1Name = "RBT_multi_shard1";
    String table2Name = "RBT_multi_shard2";

    ColumnForBoundaryQuery query1 =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId(shard1Id)
                    .setTableName(table1Name)
                    .build())
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .build();

    ColumnForBoundaryQuery query2 =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId(shard2Id)
                    .setTableName(table2Name)
                    .build())
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .build();

    Range expectedRange1 =
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

    Range expectedRange2 =
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

    RangeBoundaryTransform transform =
        RangeBoundaryTransform.builder()
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
            .build();

    PCollection<Range> output = testPipeline.apply(Create.of(query1, query2)).apply(transform);

    PAssert.that(output).containsInAnyOrder(expectedRange1, expectedRange2);

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testRangeBoundaryTransform() throws Exception {
    ImmutableList<String> partitionCols = ImmutableList.of("col1", "col2");
    TableSplitSpecification tableSplitSpecification =
        TableSplitSpecification.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
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
            .setApproxRowCount(2L)
            .setSplitStagesCount(1L)
            .setInitialSplitHeight(2L)
            .build();
    RangePreparedStatementSetter rangePreparedStatementSetter =
        new RangePreparedStatementSetter(ImmutableList.of(tableSplitSpecification));

    ColumnForBoundaryQuery firstColumnForBoundaryQuery =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .build();
    Range fullCol1Range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(10)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    Range unSplitableCol1Range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(40)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .build();
    ColumnForBoundaryQuery secondColumnForBoundaryQuery =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
            .setColumnName("col2")
            .setColumnClass(Integer.class)
            .setParentRange(unSplitableCol1Range)
            .build();
    Range col2Range =
        unSplitableCol1Range.withChildRange(
            Range.builder()
                .setTableIdentifier(
                    TableIdentifier.builder()
                        .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                        .setTableName("RBT_table1")
                        .build())
                .setColName("col2")
                .setColClass(Integer.class)
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .setStart(70)
                .setEnd(80)
                .setIsFirst(true)
                .setIsLast(true)
                .build(),
            null);

    // Create a test pipeline.
    PCollection<ColumnForBoundaryQuery> input =
        testPipeline.apply(Create.of(firstColumnForBoundaryQuery, secondColumnForBoundaryQuery));
    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .setTableSplitSpecifications(
                ImmutableList.of(
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder()
                                .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                                .setTableName("RBT_table1")
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
            .build();
    PCollection<Range> output = input.apply(rangeBoundaryTransform);

    PAssert.that(output).containsInAnyOrder(fullCol1Range, col2Range);

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testRangeBoundaryTransformMultipleTables() throws Exception {
    ImmutableList<String> partitionCols1 = ImmutableList.of("col1", "col2");
    ImmutableList<String> partitionCols2 = ImmutableList.of("col1", "col2");

    ColumnForBoundaryQuery firstColumnForBoundaryQuery =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .build();
    Range fullCol1Range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table1")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(10)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    ColumnForBoundaryQuery secondColumnForBoundaryQuery =
        ColumnForBoundaryQuery.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table2")
                    .build())
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .setParentRange(null)
            .build();
    Range fullCol3Range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("RBT_table2")
                    .build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setStart(10)
            .setEnd(40)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    // Create a test pipeline.
    PCollection<ColumnForBoundaryQuery> input =
        testPipeline.apply(Create.of(firstColumnForBoundaryQuery, secondColumnForBoundaryQuery));
    RangeBoundaryTransform rangeBoundaryTransform =
        RangeBoundaryTransform.builder()
            .setDbAdapter(new MysqlDialectAdapter(MySqlVersion.DEFAULT))
            .setTableSplitSpecifications(
                ImmutableList.of(
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder()
                                .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                                .setTableName("RBT_table1")
                                .build())
                        .setPartitionColumns(
                            partitionCols1.stream()
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
                        .build(),
                    TableSplitSpecification.builder()
                        .setTableIdentifier(
                            TableIdentifier.builder()
                                .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                                .setTableName("RBT_table2")
                                .build())
                        .setPartitionColumns(
                            partitionCols2.stream()
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
            .build();
    PCollection<Range> output = input.apply(rangeBoundaryTransform);

    PAssert.that(output).containsInAnyOrder(fullCol1Range, fullCol3Range);

    testPipeline.run().waitUntilFinish();
  }

  @AfterClass
  public static void exitDerby() throws SQLException {
    TransformTestUtils.dropDerbyTable("RBT_table1");
    TransformTestUtils.dropDerbyTable("RBT_table2");
    TransformTestUtils.dropDerbyTable("RBT_multi_shard1");
    // Shard 2 uses a different connection, but dropDerbyTable by default uses shard 1 connection.
    // I should probably add a way to drop from shard 2 or just let it go as it's in-memory.
    // However, let's be consistent if possible.
    try (java.sql.Connection connection = TransformTestUtils.getConnectionShard2()) {
      java.sql.Statement statement = connection.createStatement();
      statement.executeUpdate("drop table RBT_multi_shard2");
    }
  }
}
