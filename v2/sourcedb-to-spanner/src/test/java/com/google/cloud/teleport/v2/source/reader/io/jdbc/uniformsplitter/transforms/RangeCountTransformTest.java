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
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.RangePreparedStatementSetter;
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
  public void testRangeCountTransform() throws Exception {
    ImmutableList<String> partitionCols = ImmutableList.of("col1", "col2");
    RangePreparedStatementSetter rangePreparedStatementSetter =
        new RangePreparedStatementSetter(partitionCols.size());

    Range singleColNonLastRange =
        Range.<Integer>builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(25)
            .setIsLast(false)
            .build();
    Range bothColRange =
        Range.<Integer>builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(11)
            .setIsLast(false)
            .build()
            .withChildRange(
                Range.<Integer>builder()
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
            .setPartitionColumns(partitionCols)
            .setDataSourceProviderFn(dataSourceProviderFn)
            .setTimeoutMillis(42L)
            .setTableName(tableName)
            .build();
    PCollection<Range> output = input.apply(rangeCountTransform);

    PAssert.that(output)
        .containsInAnyOrder(
            singleColNonLastRange.withCount(2L, null), bothColRange.withCount(1L, null));

    testPipeline.run().waitUntilFinish();
  }

  @AfterClass
  public static void exitDerby() throws SQLException {
    TransformTestUtils.dropDerbyTable(tableName);
  }
}
