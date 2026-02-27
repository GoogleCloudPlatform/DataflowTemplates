/*
 * Copyright (C) 2025 Google LLC
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.UniformSplitterDBAdapter;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableSplitSpecification;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryProviderImplTest {

  @Test
  public void testGetQuery_success() throws Exception {
    TableIdentifier knownTable = TableIdentifier.builder().setTableName("knownTable").build();
    String query = "SELECT * FROM knownTable";

    UniformSplitterDBAdapter mockAdapter = mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.getReadQuery(eq("knownTable"), any())).thenReturn(query);

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(knownTable)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(100L)
            .build();

    QueryProviderImpl queryProvider =
        QueryProviderImpl.builder()
            .setTableSplitSpecifications(ImmutableList.of(spec), mockAdapter)
            .build();

    Range knownTableRange =
        Range.<Integer>builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(knownTable)
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(25)
            .setIsLast(false)
            .build();

    assertThat(queryProvider.getQuery(knownTableRange)).isEqualTo(query);
  }

  @Test
  public void testGetQuery_throwsOnUnknownTable() throws Exception {
    TableIdentifier knownTable = TableIdentifier.builder().setTableName("knownTable").build();
    UniformSplitterDBAdapter mockAdapter = mock(UniformSplitterDBAdapter.class);
    when(mockAdapter.getReadQuery(eq("knownTable"), any())).thenReturn("SELECT * FROM knownTable");

    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(knownTable)
            .setPartitionColumns(
                ImmutableList.of(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Integer.class)
                        .build()))
            .setApproxRowCount(100L)
            .build();

    QueryProviderImpl queryProvider =
        QueryProviderImpl.builder()
            .setTableSplitSpecifications(ImmutableList.of(spec), mockAdapter)
            .build();

    Range unknownTableRange =
        Range.<Integer>builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(TableIdentifier.builder().setTableName("unknownTable").build())
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(10)
            .setEnd(25)
            .setIsLast(false)
            .build();

    assertThrows(RuntimeException.class, () -> queryProvider.getQuery(unknownTableRange));
  }

  @Test
  public void testBuilder_setTableSplitSpecifications() {
    TableIdentifier tableId = TableIdentifier.builder().setTableName("testTable").build();
    PartitionColumn col =
        PartitionColumn.builder().setColumnName("col1").setColumnClass(Integer.class).build();
    TableSplitSpecification spec =
        TableSplitSpecification.builder()
            .setTableIdentifier(tableId)
            .setPartitionColumns(ImmutableList.of(col))
            .setApproxRowCount(100L)
            .build();

    UniformSplitterDBAdapter mockAdapter = mock(UniformSplitterDBAdapter.class);
    String expectedQuery = "SELECT col1 FROM testTable";
    when(mockAdapter.getReadQuery(eq("testTable"), any())).thenReturn(expectedQuery);

    QueryProviderImpl queryProvider =
        QueryProviderImpl.builder()
            .setTableSplitSpecifications(ImmutableList.of(spec), mockAdapter)
            .build();

    assertThat(queryProvider.queryMap()).containsEntry(tableId, expectedQuery);
  }
}
