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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.dialectadapter.mysql.MysqlDialectAdapter.MySqlVersion;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary.ColumnForBoundaryQuery;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link RangeBoundaryDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeBoundaryDoFnTest {
  SerializableFunction<Void, DataSource> mockDataSourceProviderFn =
      Mockito.mock(SerializableFunction.class, withSettings().serializable());
  DataSource mockDataSource = Mockito.mock(DataSource.class, withSettings().serializable());

  Connection mockConnection = Mockito.mock(Connection.class, withSettings().serializable());

  @Mock PreparedStatement mockPreparedStatemet;

  @Mock ResultSet mockResultSet;

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<Range> rangeCaptor;
  @Mock DoFn.ProcessContext mockProcessContext;

  @Test
  public void testRangeCountDoFnBasic() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    doNothing().when(mockPreparedStatemet).setObject(anyInt(), any());
    when(mockPreparedStatemet.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getLong(1)).thenReturn(0L);
    when(mockResultSet.getLong(2)).thenReturn(42L);

    RangeBoundaryDoFn rangeBoundaryDoFn =
        new RangeBoundaryDoFn(
            mockDataSourceProviderFn,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            "testTable",
            ImmutableList.of("col1"),
            null);
    ColumnForBoundaryQuery input =
        ColumnForBoundaryQuery.builder()
            .setColumnClass(Long.class)
            .setColumnName("col1")
            .setParentRange(null)
            .build();
    rangeBoundaryDoFn.setup();
    rangeBoundaryDoFn.processElement(input, mockOut, mockProcessContext);

    verify(mockOut).output(rangeCaptor.capture());
    Range newRange = rangeCaptor.getValue();
    assertThat(newRange)
        .isEqualTo(
            Range.builder()
                .setStart(0L)
                .setEnd(42L)
                .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
                .setColName("col1")
                .setColClass(Long.class)
                .build());
  }

  @Test
  public void testRangeCountDoFnSqlException() throws Exception {

    when(mockDataSourceProviderFn.apply(any())).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt()))
        .thenReturn(mockPreparedStatemet);
    doNothing().when(mockPreparedStatemet).setObject(anyInt(), any());
    when(mockPreparedStatemet.executeQuery()).thenThrow(new SQLException("test"));

    RangeBoundaryDoFn rangeBoundaryDoFn =
        new RangeBoundaryDoFn(
            mockDataSourceProviderFn,
            new MysqlDialectAdapter(MySqlVersion.DEFAULT),
            "testTable",
            ImmutableList.of("col1"),
            null);
    ColumnForBoundaryQuery input =
        ColumnForBoundaryQuery.builder()
            .setColumnClass(Long.class)
            .setColumnName("col1")
            .setParentRange(null)
            .build();
    rangeBoundaryDoFn.setup();

    assertThrows(
        SQLException.class,
        () -> rangeBoundaryDoFn.processElement(input, mockOut, mockProcessContext));
  }
}
