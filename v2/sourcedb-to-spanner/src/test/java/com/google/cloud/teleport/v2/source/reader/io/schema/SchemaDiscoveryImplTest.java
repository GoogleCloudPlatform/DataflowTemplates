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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.exception.RetriableSchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryException;
import com.google.cloud.teleport.v2.source.reader.io.exception.SchemaDiscoveryRetriesExhaustedException;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SchemaDiscoveryImpl}. */
@RunWith(MockitoJUnitRunner.class)
public class SchemaDiscoveryImplTest {
  @Mock RetriableSchemaDiscovery mockRetriableSchemaDiscovery;

  @Mock DataSource mockDataSource;
  @Mock javax.sql.DataSource mockJdbcDataSource;

  @Mock SourceSchemaReference mockSourceSchemaReference;

  @Test
  public void testConvertException_SchemaDiscoveryException() {
    SchemaDiscoveryException expected = new SchemaDiscoveryException(new RuntimeException("test"));
    SchemaDiscoveryException actual =
        assertThrows(
            SchemaDiscoveryException.class, () -> SchemaDiscoveryImpl.convertException(expected));
    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  public void testConvertException_SchemaDiscoveryRetriesExhaustedException() {
    SchemaDiscoveryRetriesExhaustedException expected =
        new SchemaDiscoveryRetriesExhaustedException(new RuntimeException("test"));
    SchemaDiscoveryRetriesExhaustedException actual =
        assertThrows(
            SchemaDiscoveryRetriesExhaustedException.class,
            () -> SchemaDiscoveryImpl.convertException(expected));
    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  public void testConvertException_NestedSchemaDiscoveryException() {
    SchemaDiscoveryException expected = new SchemaDiscoveryException(new RuntimeException("test"));
    RuntimeException wrapper = new RuntimeException(new CompletionException(expected));
    SchemaDiscoveryException actual =
        assertThrows(
            SchemaDiscoveryException.class, () -> SchemaDiscoveryImpl.convertException(wrapper));
    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  public void testConvertException_NestedRetriesExhaustedException() {
    SchemaDiscoveryRetriesExhaustedException expected =
        new SchemaDiscoveryRetriesExhaustedException(new RuntimeException("test"));
    ExecutionException wrapper = new ExecutionException(new RuntimeException(expected));
    SchemaDiscoveryRetriesExhaustedException actual =
        assertThrows(
            SchemaDiscoveryRetriesExhaustedException.class,
            () -> SchemaDiscoveryImpl.convertException(wrapper));
    assertThat(actual).isSameInstanceAs(expected);
  }

  @Test
  public void testConvertException_GenericException() {
    IOException generic = new IOException("generic");
    SchemaDiscoveryException actual =
        assertThrows(
            SchemaDiscoveryException.class, () -> SchemaDiscoveryImpl.convertException(generic));
    assertThat(actual.getCause()).isSameInstanceAs(generic);
  }

  @Test
  public void testConvertException_NestedGenericException() {
    IOException generic = new IOException("generic");
    RuntimeException wrapper = new RuntimeException(new ExecutionException(generic));
    SchemaDiscoveryException actual =
        assertThrows(
            SchemaDiscoveryException.class, () -> SchemaDiscoveryImpl.convertException(wrapper));
    assertThat(actual.getCause()).isSameInstanceAs(wrapper);
  }

  @Test
  public void testConvertException_NullExceptionCause() {
    RuntimeException wrapper = new RuntimeException((Throwable) null);
    SchemaDiscoveryException actual =
        assertThrows(
            SchemaDiscoveryException.class, () -> SchemaDiscoveryImpl.convertException(wrapper));
    assertThat(actual.getCause()).isSameInstanceAs(wrapper);
  }

  @Test
  public void testDiscoverTableSchema_EmptyTables() {
    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);
    assertThat(
            schemaDiscovery.discoverTableSchema(
                mockDataSource, mockSourceSchemaReference, ImmutableList.of()))
        .isEmpty();
  }

  @Test
  public void testDiscoverTableIndexes_EmptyTables() {
    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);
    assertThat(
            schemaDiscovery.discoverTableIndexes(
                mockDataSource, mockSourceSchemaReference, ImmutableList.of()))
        .isEmpty();
  }

  @Test
  public void testDiscoverTableSchema_ExceptionInFuture() throws RetriableSchemaDiscoveryException {
    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);
    when(mockRetriableSchemaDiscovery.discoverTableSchema(any(), any(), any()))
        .thenThrow(new SchemaDiscoveryException(new RuntimeException("test")));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            schemaDiscovery.discoverTableSchema(
                mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable")));
  }

  @Test
  public void testDiscoverTableSchema_RetriesExhaustedExceptionInFuture()
      throws RetriableSchemaDiscoveryException {
    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);
    when(mockRetriableSchemaDiscovery.discoverTableSchema(any(), any(), any()))
        .thenThrow(new SchemaDiscoveryRetriesExhaustedException(new RuntimeException("test")));

    assertThrows(
        SchemaDiscoveryRetriesExhaustedException.class,
        () ->
            schemaDiscovery.discoverTableSchema(
                mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable")));
  }

  @Test
  public void testDiscoverTableIndexes_ExceptionInFuture()
      throws RetriableSchemaDiscoveryException {
    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);
    when(mockRetriableSchemaDiscovery.discoverTableIndexes(any(), any(), any()))
        .thenThrow(new SchemaDiscoveryException(new RuntimeException("test")));

    assertThrows(
        SchemaDiscoveryException.class,
        () ->
            schemaDiscovery.discoverTableIndexes(
                mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable")));
  }

  @Test
  public void testDiscoverTableIndexes_RetriesExhaustedExceptionInFuture()
      throws RetriableSchemaDiscoveryException {
    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);
    when(mockRetriableSchemaDiscovery.discoverTableIndexes(any(), any(), any()))
        .thenThrow(new SchemaDiscoveryRetriesExhaustedException(new RuntimeException("test")));

    assertThrows(
        SchemaDiscoveryRetriesExhaustedException.class,
        () ->
            schemaDiscovery.discoverTableIndexes(
                mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable")));
  }

  @Test
  public void testSchemaDiscoveryImpl() throws RetriableSchemaDiscoveryException {
    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    final ImmutableList<String> tables = ImmutableList.of("testTable");
    when(mockRetriableSchemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, tables))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenReturn(ImmutableMap.of("testTable", ImmutableMap.of()));
    assertThat(
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTableSchema(mockDataSource, mockSourceSchemaReference, tables))
        .isEqualTo(ImmutableMap.of("testTable", ImmutableMap.of()));
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount))
        .discoverTableSchema(any(), any(), any());
  }

  @Test
  public void testSchemaDiscoveryImplThrowsRetriesExhausted()
      throws SQLException, IOException, RetriableSchemaDiscoveryException {

    final FluentBackoff mockFluentBackoff = mock(FluentBackoff.class);
    final BackOff mockBackoff = mock(BackOff.class);
    final ImmutableList<String> tables = ImmutableList.of("testTable");

    when(mockFluentBackoff.backoff()).thenReturn(mockBackoff);
    when(mockBackoff.nextBackOffMillis()).thenThrow(new RuntimeException("test"));
    when(mockRetriableSchemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, tables))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()));

    assertThrows(
        SchemaDiscoveryRetriesExhaustedException.class,
        () ->
            new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, mockFluentBackoff)
                .discoverTableSchema(mockDataSource, mockSourceSchemaReference, tables));
    verify(mockRetriableSchemaDiscovery, times(1 /* No Retries */))
        .discoverTableSchema(any(), any(), any());
  }

  @Test
  public void testSchemaDiscoveryImplHandlesIOException()
      throws SQLException, IOException, RetriableSchemaDiscoveryException {

    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    final ImmutableList<String> tables = ImmutableList.of("testTable");
    when(mockRetriableSchemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, tables))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()));

    assertThrows(
        SchemaDiscoveryRetriesExhaustedException.class,
        () ->
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTableSchema(mockDataSource, mockSourceSchemaReference, tables));
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount))
        .discoverTableSchema(any(), any(), any());
  }

  @Test
  public void testTableDiscoveryImpl() throws RetriableSchemaDiscoveryException {
    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    final ImmutableList<String> testTables = ImmutableList.of("testTable1", "testTable2");
    when(mockRetriableSchemaDiscovery.discoverTables(mockDataSource, mockSourceSchemaReference))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenReturn(testTables);
    assertThat(
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTables(mockDataSource, mockSourceSchemaReference))
        .isEqualTo(testTables);
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount)).discoverTables(any(), any());
  }

  @Test
  public void testIndexDiscoveryImpl() throws RetriableSchemaDiscoveryException {
    final int testRetryCount = 2;
    final int expectedCallsCount = testRetryCount + 1;
    final ImmutableMap<String, ImmutableList<SourceColumnIndexInfo>> discoveredIndexes =
        ImmutableMap.of(
            "testTable1",
            ImmutableList.of(
                SourceColumnIndexInfo.builder()
                    .setColumnName("testCol")
                    .setIndexName("PRIMARY")
                    .setIsPrimary(true)
                    .setIsUnique(true)
                    .setCardinality(42L)
                    .setOrdinalPosition(1)
                    .setIndexType(IndexType.NUMERIC)
                    .build()));

    when(mockRetriableSchemaDiscovery.discoverTableIndexes(
            mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable1")))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenThrow(new RetriableSchemaDiscoveryException(new SQLTransientConnectionException()))
        .thenReturn(discoveredIndexes);
    assertThat(
            new SchemaDiscoveryImpl(
                    mockRetriableSchemaDiscovery,
                    FluentBackoff.DEFAULT
                        .withInitialBackoff(Duration.millis(10L))
                        .withExponent(1)
                        .withMaxRetries(testRetryCount))
                .discoverTableIndexes(
                    mockDataSource, mockSourceSchemaReference, ImmutableList.of("testTable1")))
        .isEqualTo(discoveredIndexes);
    verify(mockRetriableSchemaDiscovery, times(expectedCallsCount))
        .discoverTableIndexes(any(), any(), any());
  }

  @Test
  public void testPartitionWork() {
    List<String> tables = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      tables.add("table" + i);
    }

    List<List<String>> partitioned = SchemaDiscoveryImpl.partitionWork(tables, 3);
    assertThat(partitioned).hasSize(4);
    assertThat(partitioned.get(0)).containsExactly("table0", "table1", "table2");
    assertThat(partitioned.get(3)).containsExactly("table9");

    assertThat(SchemaDiscoveryImpl.partitionWork(new ArrayList<>(), 3)).isEmpty();
  }

  @Test
  public void testParallelBatchDiscovery() throws RetriableSchemaDiscoveryException {
    // Generate 1001 tables to trigger 3 batches (BATCH_SIZE is 500)
    List<String> tables = new ArrayList<>();
    for (int i = 0; i < 1001; i++) {
      tables.add("table" + i);
    }

    SchemaDiscoveryImpl schemaDiscovery =
        new SchemaDiscoveryImpl(mockRetriableSchemaDiscovery, FluentBackoff.DEFAULT);

    // Mock returns for each batch
    // Batch 1: 0-499
    // Batch 2: 500-999
    // Batch 3: 1000
    when(mockRetriableSchemaDiscovery.discoverTableSchema(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              ImmutableList<String> batch = invocation.getArgument(2);
              ImmutableMap.Builder<String, ImmutableMap<String, SourceColumnType>> builder =
                  ImmutableMap.builder();
              for (String table : batch) {
                builder.put(table, ImmutableMap.of());
              }
              return builder.build();
            });

    ImmutableMap<String, ImmutableMap<String, SourceColumnType>> result =
        schemaDiscovery.discoverTableSchema(
            mockDataSource, mockSourceSchemaReference, ImmutableList.copyOf(tables));

    assertThat(result).hasSize(1001);
    for (int i = 0; i < 1001; i++) {
      assertThat(result).containsKey("table" + i);
    }
  }
}
